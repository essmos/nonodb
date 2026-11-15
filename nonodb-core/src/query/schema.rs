//! A bounded `$jsonSchema` matcher.
//!
//! The supported keywords cover the common validation use cases: `bsonType`,
//! `required`, `properties`, `patternProperties`, `additionalProperties`,
//! `items`, `enum`, `const`, numeric/string/array bounds, and the
//! `allOf`/`anyOf`/`oneOf`/`not` combinators. Unknown keywords are ignored so
//! schemas written for newer MongoDB versions remain usable.

use bigdecimal::num_traits::Zero;
use serde_json::Value;

use crate::bson_types;
use crate::error::{Error, Result};
use crate::numeric;

use super::matcher::num_eq;

pub fn matches(value: &Value, schema: &Value) -> bool {
    schema_matches(value, schema)
}

fn schema_matches(value: &Value, schema: &Value) -> bool {
    let Some(schema) = schema.as_object() else {
        return false;
    };

    if let Some(bson_type) = schema.get("bsonType") {
        if !bson_types::type_matches(value, bson_type) {
            return false;
        }
    }
    if let Some(constant) = schema.get("const") {
        if !num_eq(value, constant) {
            return false;
        }
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        if !values.iter().any(|candidate| num_eq(value, candidate)) {
            return false;
        }
    }

    if let Some(obj) = value.as_object() {
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            if required
                .iter()
                .filter_map(Value::as_str)
                .any(|name| !obj.contains_key(name))
            {
                return false;
            }
        }
        if let Some(min) = schema.get("minProperties").and_then(Value::as_u64) {
            if obj.len() < min as usize {
                return false;
            }
        }
        if let Some(max) = schema.get("maxProperties").and_then(Value::as_u64) {
            if obj.len() > max as usize {
                return false;
            }
        }
        let properties = schema.get("properties").and_then(Value::as_object);
        let patterns = schema.get("patternProperties").and_then(Value::as_object);
        for (key, field) in obj {
            let mut matched = false;
            if let Some(field_schema) = properties.and_then(|properties| properties.get(key)) {
                matched = true;
                if !schema_matches(field, field_schema) {
                    return false;
                }
            }
            if let Some(patterns) = patterns {
                for (pattern, field_schema) in patterns {
                    let pattern = Value::String(pattern.clone());
                    if crate::regex::is_match(Some(&Value::String(key.clone())), &pattern, None) {
                        matched = true;
                        if !schema_matches(field, field_schema) {
                            return false;
                        }
                    }
                }
            }
            if !matched {
                if let Some(additional) = schema.get("additionalProperties") {
                    match additional {
                        Value::Bool(false) => return false,
                        Value::Bool(true) => {}
                        schema => {
                            if !schema_matches(field, schema) {
                                return false;
                            }
                        }
                    }
                }
            }
        }
    }

    if let Some(items) = value.as_array() {
        if let Some(min) = schema.get("minItems").and_then(Value::as_u64) {
            if items.len() < min as usize {
                return false;
            }
        }
        if let Some(max) = schema.get("maxItems").and_then(Value::as_u64) {
            if items.len() > max as usize {
                return false;
            }
        }
        if schema
            .get("uniqueItems")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            for (index, item) in items.iter().enumerate() {
                if items[..index].iter().any(|other| num_eq(item, other)) {
                    return false;
                }
            }
        }
        if let Some(item_schema) = schema.get("items") {
            if let Some(item_schemas) = item_schema.as_array() {
                for (index, item) in items.iter().enumerate() {
                    if let Some(schema) = item_schemas.get(index) {
                        if !schema_matches(item, schema) {
                            return false;
                        }
                    }
                }
            } else if items.iter().any(|item| !schema_matches(item, item_schema)) {
                return false;
            }
        }
    }

    if let Some(text) = value.as_str() {
        let length = text.chars().count() as u64;
        if let Some(min) = schema.get("minLength").and_then(Value::as_u64) {
            if length < min {
                return false;
            }
        }
        if let Some(max) = schema.get("maxLength").and_then(Value::as_u64) {
            if length > max {
                return false;
            }
        }
        if let Some(pattern) = schema.get("pattern") {
            if !crate::regex::is_match(Some(&Value::String(text.to_string())), pattern, None) {
                return false;
            }
        }
    }

    if let Some(number) = numeric::as_bigdecimal(value) {
        for (key, check) in [
            ("minimum", false),
            ("exclusiveMinimum", true),
            ("maximum", false),
            ("exclusiveMaximum", true),
        ] {
            if let Some(limit) = schema.get(key).and_then(numeric::as_bigdecimal) {
                let ord = number.cmp(&limit);
                let valid = match (check, key.starts_with("min")) {
                    (true, true) => ord == std::cmp::Ordering::Greater,
                    (false, true) => ord != std::cmp::Ordering::Less,
                    (true, false) => ord == std::cmp::Ordering::Less,
                    (false, false) => ord != std::cmp::Ordering::Greater,
                };
                if !valid {
                    return false;
                }
            }
        }
        if let Some(divisor) = schema.get("multipleOf").and_then(numeric::as_bigdecimal) {
            if divisor.is_zero() || !(number % divisor).is_zero() {
                return false;
            }
        }
    }

    if let Some(all) = schema.get("allOf").and_then(Value::as_array) {
        if !all.iter().all(|schema| schema_matches(value, schema)) {
            return false;
        }
    }
    if let Some(any) = schema.get("anyOf").and_then(Value::as_array) {
        if !any.iter().any(|schema| schema_matches(value, schema)) {
            return false;
        }
    }
    if let Some(one) = schema.get("oneOf").and_then(Value::as_array) {
        if one
            .iter()
            .filter(|schema| schema_matches(value, schema))
            .count()
            != 1
        {
            return false;
        }
    }
    if let Some(not) = schema.get("not") {
        if schema_matches(value, not) {
            return false;
        }
    }
    true
}

/// Validates the parts of a schema that would otherwise fail silently.
pub fn validate(schema: &Value) -> Result<()> {
    validate_schema(schema, "$jsonSchema")
}

fn validate_schema(schema: &Value, path: &str) -> Result<()> {
    let Some(obj) = schema.as_object() else {
        return Err(Error::Query(format!("{path} must be an object")));
    };
    if let Some(bson_type) = obj.get("bsonType") {
        validate_type_spec(bson_type, path)?;
    }
    if let Some(required) = obj.get("required") {
        let Some(items) = required.as_array() else {
            return Err(Error::Query(format!("{path}.required must be an array")));
        };
        if items.iter().any(|item| item.as_str().is_none()) {
            return Err(Error::Query(format!(
                "{path}.required entries must be strings"
            )));
        }
    }
    if let Some(properties) = obj.get("properties") {
        let Some(properties) = properties.as_object() else {
            return Err(Error::Query(format!("{path}.properties must be an object")));
        };
        for (name, schema) in properties {
            validate_schema(schema, &format!("{path}.properties.{name}"))?;
        }
    }
    if let Some(patterns) = obj.get("patternProperties") {
        let Some(patterns) = patterns.as_object() else {
            return Err(Error::Query(format!(
                "{path}.patternProperties must be an object"
            )));
        };
        for (pattern, schema) in patterns {
            regex::Regex::new(pattern).map_err(|err| {
                Error::Query(format!("{path}.patternProperties has invalid regex: {err}"))
            })?;
            validate_schema(schema, &format!("{path}.patternProperties.{pattern}"))?;
        }
    }
    if let Some(additional) = obj.get("additionalProperties") {
        if !additional.is_boolean() {
            validate_schema(additional, &format!("{path}.additionalProperties"))?;
        }
    }
    if let Some(items) = obj.get("items") {
        if let Some(items) = items.as_array() {
            for (index, schema) in items.iter().enumerate() {
                validate_schema(schema, &format!("{path}.items.{index}"))?;
            }
        } else {
            validate_schema(items, &format!("{path}.items"))?;
        }
    }
    if let Some(pattern) = obj.get("pattern") {
        let Some(pattern) = pattern.as_str() else {
            return Err(Error::Query(format!("{path}.pattern must be a string")));
        };
        regex::Regex::new(pattern)
            .map_err(|err| Error::Query(format!("{path}.pattern is invalid: {err}")))?;
    }
    for key in [
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
        "minProperties",
        "maxProperties",
    ] {
        if let Some(value) = obj.get(key) {
            if value.as_u64().is_none() {
                return Err(Error::Query(format!(
                    "{path}.{key} must be a non-negative integer"
                )));
            }
        }
    }
    for key in [
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
    ] {
        if let Some(value) = obj.get(key) {
            if numeric::as_bigdecimal(value).is_none() {
                return Err(Error::Query(format!("{path}.{key} must be a number")));
            }
        }
    }
    if let Some(divisor) = obj.get("multipleOf").and_then(numeric::as_bigdecimal) {
        if divisor.is_zero() {
            return Err(Error::Query(format!("{path}.multipleOf cannot be zero")));
        }
    }
    if let Some(values) = obj.get("enum") {
        if !values.is_array() {
            return Err(Error::Query(format!("{path}.enum must be an array")));
        }
    }
    for key in ["allOf", "anyOf", "oneOf"] {
        if let Some(schemas) = obj.get(key) {
            let Some(schemas) = schemas.as_array() else {
                return Err(Error::Query(format!("{path}.{key} must be an array")));
            };
            if schemas.is_empty() {
                return Err(Error::Query(format!("{path}.{key} cannot be empty")));
            }
            for (index, schema) in schemas.iter().enumerate() {
                validate_schema(schema, &format!("{path}.{key}.{index}"))?;
            }
        }
    }
    if let Some(schema) = obj.get("not") {
        validate_schema(schema, &format!("{path}.not"))?;
    }
    Ok(())
}

fn validate_type_spec(spec: &Value, path: &str) -> Result<()> {
    let valid = match spec {
        Value::String(name) => bson_type_name(name),
        Value::Array(names) => names
            .iter()
            .all(|name| name.as_str().is_some_and(bson_type_name)),
        _ => false,
    };
    if !valid {
        return Err(Error::Query(format!(
            "{path}.bsonType must be a type name or an array of type names"
        )));
    }
    Ok(())
}

fn bson_type_name(name: &str) -> bool {
    matches!(
        name,
        "double"
            | "string"
            | "object"
            | "array"
            | "binData"
            | "undefined"
            | "objectId"
            | "bool"
            | "date"
            | "null"
            | "regex"
            | "dbPointer"
            | "javascript"
            | "symbol"
            | "javascriptWithScope"
            | "int"
            | "timestamp"
            | "long"
            | "decimal"
            | "minKey"
            | "maxKey"
            | "number"
    )
}
