//! Validation for filter operators whose arguments have a required shape.

use serde_json::Value;

use crate::bson_types;
use crate::date;
use crate::geo;
use crate::numeric;
use crate::regex;
use crate::{Error, Result};

/// Top-level filter operators implemented by the matcher. `$text` is resolved by
/// the search layer, never by the matcher itself.
const FILTER_OPERATORS: &[&str] = &[
    "$and",
    "$or",
    "$nor",
    "$not",
    "$expr",
    "$jsonSchema",
    "$text",
];

/// Field-level operators implemented by the matcher.
const FIELD_OPERATORS: &[&str] = &[
    "$eq",
    "$ne",
    "$gt",
    "$gte",
    "$lt",
    "$lte",
    "$in",
    "$nin",
    "$all",
    "$size",
    "$elemMatch",
    "$exists",
    "$regex",
    "$options",
    "$type",
    "$mod",
    "$bitsAllClear",
    "$bitsAllSet",
    "$bitsAnyClear",
    "$bitsAnySet",
    "$near",
    "$nearSphere",
    "$geoWithin",
    "$geoIntersects",
    "$not",
];

/// Validates regex and array operator arguments before a query runs.
///
/// Operators outside the documented subset are rejected here, so a typo such as
/// `{"email": {"$regx": "..."}}` fails instead of matching nothing. That matters
/// for negation: `$nor`/`$not` would otherwise turn a silently-false predicate
/// into "matches every document".
pub fn validate_filter(filter: &Value) -> Result<()> {
    regex::validate_filter(filter)?;
    validate_array_filter(filter)
}

fn validate_array_filter(filter: &Value) -> Result<()> {
    let Some(obj) = filter.as_object() else {
        return Ok(());
    };
    for (key, cond) in obj {
        if key.starts_with('$') && !FILTER_OPERATORS.contains(&key.as_str()) {
            return Err(Error::Query(format!("unknown top level operator: {key}")));
        }
        match key.as_str() {
            "$and" | "$or" | "$nor" => {
                if let Some(arr) = cond.as_array() {
                    for f in arr {
                        validate_array_filter(f)?;
                    }
                }
            }
            "$not" => validate_array_filter(cond)?,
            "$expr" => validate_expression(cond)?,
            "$jsonSchema" => crate::query::schema::validate(cond)?,
            // `$text` is resolved by the search layer, not by the matcher.
            "$text" => {}
            _ => validate_condition(cond)?,
        }
    }
    Ok(())
}

fn validate_condition(cond: &Value) -> Result<()> {
    // Extended JSON dates, numbers, BSON values, and BSON regex values are
    // predicates/values, not operator documents.
    if date::is_date(cond)
        || numeric::is_numeric(cond)
        || bson_types::is_bson_value(cond)
        || regex::is_value(cond)
    {
        return Ok(());
    }
    let Some(obj) = cond.as_object() else {
        return Ok(());
    };
    if obj.is_empty() || !obj.keys().all(|k| k.starts_with('$')) {
        return Ok(());
    }
    for (op, arg) in obj {
        if !FIELD_OPERATORS.contains(&op.as_str()) {
            return Err(Error::Query(format!("unknown operator: {op}")));
        }
        match op.as_str() {
            "$size" => {
                if arg.as_u64().and_then(|n| usize::try_from(n).ok()).is_none() {
                    return Err(Error::Query("$size requires a non-negative integer".into()));
                }
            }
            "$all" => {
                if !arg.is_array() {
                    return Err(Error::Query("$all requires an array".into()));
                }
            }
            "$mod" => {
                let Some(values) = arg.as_array() else {
                    return Err(Error::Query("$mod requires [divisor, remainder]".into()));
                };
                if values.len() != 2 || values.iter().any(|v| numeric::parse_value(v).is_none()) {
                    return Err(Error::Query("$mod requires two numeric arguments".into()));
                }
                if numeric::as_bigdecimal(&values[0]) == Some(0.into()) {
                    return Err(Error::Query("$mod divisor cannot be zero".into()));
                }
            }
            "$bitsAllClear" | "$bitsAllSet" | "$bitsAnyClear" | "$bitsAnySet" => {
                validate_bit_mask(arg)?;
            }
            "$elemMatch" => {
                let Some(query) = arg.as_object() else {
                    return Err(Error::Query("$elemMatch requires an object".into()));
                };
                if elem_match_uses_filter(query) {
                    validate_filter(arg)?;
                } else {
                    regex::validate_condition(arg)?;
                    validate_condition(arg)?;
                }
            }
            "$not" => {
                regex::validate_condition(arg)?;
                validate_condition(arg)?;
            }
            "$near" | "$nearSphere" | "$geoWithin" | "$geoIntersects" => {
                geo::validate_condition(op, arg)?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Rejects expression operators that the aggregation evaluator does not implement.
///
/// `$expr` used to evaluate unknown operators as `false`, which silently returned
/// no documents (and, under `$nor`/`$not`, every document).
fn validate_expression(expr: &Value) -> Result<()> {
    match expr {
        Value::Array(items) => {
            for item in items {
                validate_expression(item)?;
            }
        }
        Value::Object(obj) => {
            if obj.len() == 1
                && !date::is_date(expr)
                && !numeric::is_numeric(expr)
                && !bson_types::is_bson_value(expr)
                && !regex::is_value(expr)
            {
                if let Some(op) = obj.keys().next() {
                    if op.starts_with('$') && !crate::aggregate::expression_operator_supported(op) {
                        return Err(Error::Query(format!(
                            "unsupported aggregation expression `{op}`"
                        )));
                    }
                    // `$literal` value is data, not an expression tree.
                    if op == "$literal" {
                        return Ok(());
                    }
                }
            }
            for value in obj.values() {
                validate_expression(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_bit_mask(arg: &Value) -> Result<()> {
    if let Some(positions) = arg.as_array() {
        if positions.iter().any(|p| !is_bit_position(p)) {
            return Err(Error::Query(
                "bit positions must be integers between 0 and 63".into(),
            ));
        }
        return Ok(());
    }
    if numeric::as_bigdecimal(arg).is_some() {
        return Ok(());
    }
    Err(Error::Query(
        "bit mask must be an integer or an array of bit positions".into(),
    ))
}

fn is_bit_position(value: &Value) -> bool {
    let Some(number) = numeric::as_bigdecimal(value) else {
        return false;
    };
    if number != number.with_scale(0) {
        return false;
    }
    bigdecimal::num_traits::ToPrimitive::to_u64(&number).is_some_and(|position| position < 64)
}

fn elem_match_uses_filter(query: &serde_json::Map<String, Value>) -> bool {
    if query.is_empty() {
        return false;
    }
    if query.keys().any(|key| !key.starts_with('$')) {
        return true;
    }
    if query.keys().any(|key| {
        matches!(
            key.as_str(),
            "$and" | "$or" | "$nor" | "$expr" | "$jsonSchema"
        )
    }) {
        return true;
    }
    if let Some(Value::Object(not)) = query.get("$not") {
        return not
            .keys()
            .any(|key| !key.starts_with('$') || matches!(key.as_str(), "$and" | "$or" | "$nor"));
    }
    false
}
