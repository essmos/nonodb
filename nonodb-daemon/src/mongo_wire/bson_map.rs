//! BSON <-> NonoDB Extended JSON conversion for the MongoDB wire frontend.

use bson::{Bson, Document};
use nonodb_core::{bson_types, date, numeric};
use serde_json::{json, Map, Number, Value};

/// Converts a BSON document into NonoDB's Extended JSON representation.
pub fn document_to_value(document: &Document) -> Result<Value, String> {
    let mut object = Map::new();
    for (key, value) in document {
        object.insert(key.clone(), bson_to_value(value)?);
    }
    Ok(Value::Object(object))
}

/// Converts a NonoDB Extended JSON document into BSON.
pub fn document_to_bson(document: &Value) -> Result<Document, String> {
    let object = document
        .as_object()
        .ok_or_else(|| "expected a document".to_string())?;
    let mut out = Document::new();
    for (key, value) in object {
        out.insert(key.clone(), value_to_bson(value)?);
    }
    Ok(out)
}

/// Converts one BSON value into NonoDB's Extended JSON representation.
pub fn bson_to_value(value: &Bson) -> Result<Value, String> {
    Ok(match value {
        Bson::Double(value) => double_to_value(*value),
        Bson::String(value) => Value::String(value.clone()),
        Bson::Array(values) => Value::Array(
            values
                .iter()
                .map(bson_to_value)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Bson::Document(document) => document_to_value(document)?,
        Bson::Boolean(value) => Value::Bool(*value),
        Bson::Null => Value::Null,
        Bson::RegularExpression(regex) => json!({
            "$regularExpression": {
                "pattern": regex.pattern.as_str(),
                "options": regex.options.as_str(),
            }
        }),
        Bson::Int32(value) => json!({"$numberInt": value.to_string()}),
        Bson::Int64(value) => json!({"$numberLong": value.to_string()}),
        Bson::Timestamp(value) => json!({
            "$timestamp": {"t": value.time, "i": value.increment}
        }),
        Bson::Binary(binary) => bson_types::binary_value(u8::from(binary.subtype), &binary.bytes),
        Bson::ObjectId(value) => bson_types::object_id_value(bson_types::ObjectId(value.bytes())),
        Bson::DateTime(value) => date::canonical(value.timestamp_millis()),
        Bson::Decimal128(value) => json!({"$numberDecimal": value.to_string()}),
        Bson::MinKey => json!({"$minKey": 1}),
        Bson::MaxKey => json!({"$maxKey": 1}),
        other => {
            return Err(format!(
                "unsupported BSON type in MongoDB wire value: {other:?}"
            ))
        }
    })
}

/// Converts one NonoDB Extended JSON value into BSON.
pub fn value_to_bson(value: &Value) -> Result<Bson, String> {
    if let Some(millis) = date::parse_value(value) {
        return Ok(Bson::DateTime(bson::DateTime::from_millis(millis)));
    }
    if let Some(id) = bson_types::parse_object_id(value) {
        return Ok(Bson::ObjectId(bson::oid::ObjectId::from_bytes(id.0)));
    }
    if let Some(binary) = bson_types::parse_binary(value) {
        return Ok(Bson::Binary(bson::Binary {
            subtype: binary.subtype.into(),
            bytes: binary.bytes,
        }));
    }
    if let Some(number) = numeric::parse_value(value) {
        return numeric_to_bson(value, number.kind());
    }
    if let Some(object) = value.as_object() {
        if let Some(payload) = object.get("$regularExpression").and_then(Value::as_object) {
            let pattern = payload
                .get("pattern")
                .and_then(Value::as_str)
                .ok_or_else(|| "`$regularExpression.pattern` must be a string".to_string())?;
            let options = payload
                .get("options")
                .and_then(Value::as_str)
                .unwrap_or_default();
            return Ok(Bson::RegularExpression(bson::Regex {
                pattern: pattern
                    .try_into()
                    .map_err(|_| "regex pattern contains a NUL byte".to_string())?,
                options: options
                    .try_into()
                    .map_err(|_| "regex options contain a NUL byte".to_string())?,
            }));
        }
        if let Some(payload) = object.get("$timestamp").and_then(Value::as_object) {
            let time = payload
                .get("t")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| "`$timestamp.t` must be a u32".to_string())?;
            let increment = payload
                .get("i")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| "`$timestamp.i` must be a u32".to_string())?;
            return Ok(Bson::Timestamp(bson::Timestamp { time, increment }));
        }
        if object.contains_key("$minKey") {
            return Ok(Bson::MinKey);
        }
        if object.contains_key("$maxKey") {
            return Ok(Bson::MaxKey);
        }
        return Ok(Bson::Document(document_to_bson(value)?));
    }
    Ok(match value {
        Value::Null => Bson::Null,
        Value::Bool(value) => Bson::Boolean(*value),
        Value::String(value) => Bson::String(value.clone()),
        Value::Array(values) => Bson::Array(
            values
                .iter()
                .map(value_to_bson)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                Bson::Int64(value)
            } else if let Some(value) = number.as_u64() {
                i64::try_from(value)
                    .map(Bson::Int64)
                    .map_err(|_| "integer is too large for BSON int64".to_string())?
            } else {
                Bson::Double(
                    number
                        .as_f64()
                        .ok_or_else(|| "invalid JSON number".to_string())?,
                )
            }
        }
        Value::Object(_) => unreachable!("objects returned above"),
    })
}

fn numeric_to_bson(value: &Value, kind: numeric::NumericKind) -> Result<Bson, String> {
    use numeric::NumericKind;

    if let Some(object) = value.as_object() {
        let text = |key: &str| {
            object
                .get(key)
                .and_then(Value::as_str)
                .ok_or_else(|| format!("`{key}` must be a string"))
        };
        return match kind {
            NumericKind::Int => {
                Ok(Bson::Int32(text("$numberInt")?.parse().map_err(
                    |error| format!("invalid `$numberInt`: {error}"),
                )?))
            }
            NumericKind::Long => {
                Ok(Bson::Int64(text("$numberLong")?.parse().map_err(
                    |error| format!("invalid `$numberLong`: {error}"),
                )?))
            }
            NumericKind::Double => Ok(Bson::Double(parse_double(text("$numberDouble")?)?)),
            NumericKind::Decimal => {
                Ok(Bson::Decimal128(text("$numberDecimal")?.parse().map_err(
                    |error| format!("invalid `$numberDecimal`: {error}"),
                )?))
            }
        };
    }
    let number = value
        .as_number()
        .ok_or_else(|| "invalid numeric value".to_string())?;
    match kind {
        NumericKind::Int => Ok(Bson::Int32(
            number
                .as_i64()
                .and_then(|value| i32::try_from(value).ok())
                .ok_or_else(|| "integer does not fit BSON int32".to_string())?,
        )),
        NumericKind::Long => {
            Ok(Bson::Int64(number.as_i64().ok_or_else(|| {
                "integer does not fit BSON int64".to_string()
            })?))
        }
        NumericKind::Double => Ok(Bson::Double(
            number
                .as_f64()
                .ok_or_else(|| "invalid BSON double".to_string())?,
        )),
        NumericKind::Decimal => {
            Ok(Bson::Decimal128(number.to_string().parse().map_err(
                |error| format!("invalid BSON decimal: {error}"),
            )?))
        }
    }
}

fn parse_double(value: &str) -> Result<f64, String> {
    match value {
        "NaN" => Ok(f64::NAN),
        "Infinity" | "Inf" => Ok(f64::INFINITY),
        "-Infinity" | "-Inf" => Ok(f64::NEG_INFINITY),
        value => value
            .parse()
            .map_err(|error| format!("invalid `$numberDouble`: {error}")),
    }
}

fn double_to_value(value: f64) -> Value {
    if value.is_nan() {
        json!({"$numberDouble": "NaN"})
    } else if value == f64::INFINITY {
        json!({"$numberDouble": "Infinity"})
    } else if value == f64::NEG_INFINITY {
        json!({"$numberDouble": "-Infinity"})
    } else {
        Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bson_special_values_round_trip_through_extended_json() {
        let document = bson::doc! {
            "_id": bson::oid::ObjectId::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
            "at": bson::DateTime::from_millis(1_767_225_600_000),
            "price": "199.99".parse::<bson::Decimal128>().unwrap(),
            "blob": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![0, 1, 2, 255] },
            "small": 42i32,
            "large": 9_007_199_254_740_993i64,
            "half": 0.5f64,
            "nan": f64::NAN,
            "ts": bson::Timestamp { time: 1_700_000_000, increment: 7 },
            "low": bson::Bson::MinKey,
            "high": bson::Bson::MaxKey,
        };
        let value = document_to_value(&document).unwrap();
        assert_eq!(value["_id"], json!({"$oid": "000102030405060708090a0b"}));
        assert_eq!(
            value["at"],
            json!({"$date": {"$numberLong": "1767225600000"}})
        );
        assert_eq!(value["price"], json!({"$numberDecimal": "199.99"}));
        assert_eq!(
            value["blob"],
            json!({"$binary": {"base64": "AAEC/w==", "subType": "00"}})
        );
        assert_eq!(value["small"], json!({"$numberInt": "42"}));
        assert_eq!(value["large"], json!({"$numberLong": "9007199254740993"}));
        assert_eq!(value["half"], json!(0.5));
        assert_eq!(value["nan"], json!({"$numberDouble": "NaN"}));
        assert_eq!(
            value["ts"],
            json!({"$timestamp": {"t": 1700000000u32, "i": 7u32}})
        );
        assert_eq!(value["low"], json!({"$minKey": 1}));
        assert_eq!(value["high"], json!({"$maxKey": 1}));

        let back = document_to_bson(&value).unwrap();
        assert_eq!(
            back.get_object_id("_id").unwrap(),
            document.get_object_id("_id").unwrap()
        );
        assert_eq!(
            back.get_datetime("at").unwrap().timestamp_millis(),
            1_767_225_600_000
        );
        assert_eq!(back.get_i32("small").unwrap(), 42);
        assert_eq!(back.get_i64("large").unwrap(), 9_007_199_254_740_993);
        assert!(back.get_f64("nan").unwrap().is_nan());
        assert_eq!(
            back.get_timestamp("ts").unwrap(),
            document.get_timestamp("ts").unwrap()
        );
        assert_eq!(back.get("low"), Some(&Bson::MinKey));
        assert_eq!(back.get("high"), Some(&Bson::MaxKey));
    }

    #[test]
    fn unsupported_bson_types_fail_loudly() {
        let error = bson_to_value(&Bson::JavaScriptCode("return 1".into())).unwrap_err();
        assert!(error.contains("unsupported BSON type"));
    }
}
