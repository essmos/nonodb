//! Validation for filter operators whose arguments have a required shape.

use serde_json::Value;

use crate::bson_types;
use crate::date;
use crate::geo;
use crate::numeric;
use crate::regex;
use crate::{Error, Result};

/// Validates regex and array operator arguments before a query runs.
pub fn validate_filter(filter: &Value) -> Result<()> {
    regex::validate_filter(filter)?;
    validate_array_filter(filter)
}

fn validate_array_filter(filter: &Value) -> Result<()> {
    let Some(obj) = filter.as_object() else {
        return Ok(());
    };
    for (key, cond) in obj {
        match key.as_str() {
            "$and" | "$or" | "$nor" => {
                if let Some(arr) = cond.as_array() {
                    for f in arr {
                        validate_array_filter(f)?;
                    }
                }
            }
            "$not" => validate_array_filter(cond)?,
            "$expr" => {}
            "$jsonSchema" => crate::query::schema::validate(cond)?,
            _ => validate_condition(cond)?,
        }
    }
    Ok(())
}

fn validate_condition(cond: &Value) -> Result<()> {
    if date::is_date(cond) || numeric::is_numeric(cond) || bson_types::is_bson_value(cond) {
        return Ok(());
    }
    let Some(obj) = cond.as_object() else {
        return Ok(());
    };
    if obj.is_empty() || !obj.keys().all(|k| k.starts_with('$')) {
        return Ok(());
    }
    for (op, arg) in obj {
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
