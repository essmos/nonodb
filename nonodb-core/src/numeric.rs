//! MongoDB-compatible numeric values.
//!
//! JSON numbers are compared as exact decimals rather than through `f64`.
//! Decimal128 values use MongoDB Extended JSON:
//!
//! - `{"$numberDecimal": "199.99"}`
//!
//! Integer and floating point Extended JSON forms are also accepted:
//!
//! - `{"$numberInt": "42"}`
//! - `{"$numberLong": "9007199254740993"}`
//! - `{"$numberDouble": "1.5"}`
//! - `{"$numberDouble": "NaN"}`, `"Infinity"`, `"-Infinity"`
//!
//! Finite numbers are normalized to a bounded scientific representation with at
//! most 34 significant digits. JSON `f64` values use 17 significant decimal
//! digits, enough to round-trip and preserve ordering. All numeric types share
//! one index encoding, so `1`, `1.0`, and `NumberDecimal("1")` sort together
//! and compare equal, matching a single numeric type bracket.

use std::cmp::Ordering;
use std::str::FromStr;

use bigdecimal::num_bigint::BigInt;
use bigdecimal::num_traits::ToPrimitive;
use bigdecimal::{BigDecimal, RoundingMode};
use bson::Decimal128;
use serde_json::{json, Number, Value};

use crate::error::{Error, Result};

pub const NUMBER_DECIMAL_KEY: &str = "$numberDecimal";
pub const NUMBER_DOUBLE_KEY: &str = "$numberDouble";
pub const NUMBER_INT_KEY: &str = "$numberInt";
pub const NUMBER_LONG_KEY: &str = "$numberLong";

const MAX_DIGITS: usize = 34;
const ADJ_BIAS: i32 = 1_000_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NumericKind {
    Int,
    Long,
    Double,
    Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Numeric {
    NaN,
    NegInfinity,
    Finite {
        negative: bool,
        exponent: i32,
        digits: [u8; MAX_DIGITS],
        len: u8,
    },
    PositiveInfinity,
}

#[derive(Clone, Copy, Debug)]
pub struct NumericValue {
    kind: NumericKind,
    value: Numeric,
}

impl NumericValue {
    pub fn kind(self) -> NumericKind {
        self.kind
    }

    pub fn kind_is_decimal(self) -> bool {
        self.kind == NumericKind::Decimal
    }

    pub fn value(self) -> Numeric {
        self.value
    }

    pub fn is_finite(self) -> bool {
        matches!(self.value, Numeric::Finite { .. })
    }
}

impl Numeric {
    fn zero() -> Self {
        Self::Finite {
            negative: false,
            exponent: 0,
            digits: [0; MAX_DIGITS],
            len: 1,
        }
    }

    fn is_zero(self) -> bool {
        matches!(self, Self::Finite { len: 1, digits, .. } if digits[0] == 0)
    }

    fn cmp_value(self, other: Self) -> Ordering {
        match (self, other) {
            (Self::NaN, Self::NaN) => Ordering::Equal,
            (Self::NaN, _) => Ordering::Less,
            (_, Self::NaN) => Ordering::Greater,
            (Self::NegInfinity, Self::NegInfinity) => Ordering::Equal,
            (Self::NegInfinity, _) => Ordering::Less,
            (_, Self::NegInfinity) => Ordering::Greater,
            (Self::PositiveInfinity, Self::PositiveInfinity) => Ordering::Equal,
            (Self::PositiveInfinity, _) => Ordering::Greater,
            (_, Self::PositiveInfinity) => Ordering::Less,
            (a, b) => compare_finite(a, b),
        }
    }

    fn to_bigdecimal(self) -> Option<BigDecimal> {
        match self {
            Self::NaN | Self::NegInfinity | Self::PositiveInfinity => None,
            Self::Finite {
                negative,
                exponent,
                digits,
                len,
            } => {
                let mut coeff = 0u128;
                for &d in &digits[..len as usize] {
                    coeff = coeff.checked_mul(10)?.checked_add(d as u128)?;
                }
                let mut out = BigDecimal::new(BigInt::from(coeff), -(exponent as i64));
                if negative {
                    out = -out;
                }
                Some(out)
            }
        }
    }
}

fn compare_finite(a: Numeric, b: Numeric) -> Ordering {
    let (a_neg, a_exp, a_digits, a_len) = match a {
        Numeric::Finite {
            negative,
            exponent,
            digits,
            len,
        } => (negative, exponent, digits, len as usize),
        _ => unreachable!(),
    };
    let (b_neg, b_exp, b_digits, b_len) = match b {
        Numeric::Finite {
            negative,
            exponent,
            digits,
            len,
        } => (negative, exponent, digits, len as usize),
        _ => unreachable!(),
    };
    if a.is_zero() || b.is_zero() {
        return match (a.is_zero(), b.is_zero()) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if b_neg {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, true) => {
                if a_neg {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            _ => unreachable!(),
        };
    }
    if a_neg != b_neg {
        return if a_neg {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    let a_adj = a_exp + a_len as i32;
    let b_adj = b_exp + b_len as i32;
    let ord = a_adj.cmp(&b_adj).then_with(|| {
        for i in 0..MAX_DIGITS {
            let ad = if i < a_len { a_digits[i] } else { 0 };
            let bd = if i < b_len { b_digits[i] } else { 0 };
            match ad.cmp(&bd) {
                Ordering::Equal => {}
                ord => return ord,
            }
        }
        Ordering::Equal
    });
    if a_neg {
        ord.reverse()
    } else {
        ord
    }
}

/// Parses a JSON number, `$numberDecimal`, or `$numberDouble` value.
pub fn parse_value(value: &Value) -> Option<NumericValue> {
    if let Value::Number(n) = value {
        let (kind, value) = from_number(n)?;
        return Some(NumericValue { kind, value });
    }
    let obj = value.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    if let Some(Value::String(s)) = obj.get(NUMBER_DECIMAL_KEY) {
        let dec = Decimal128::from_str(s).ok()?;
        let s = dec.to_string();
        let value = parse_finite(&s)?;
        return Some(NumericValue {
            kind: NumericKind::Decimal,
            value,
        });
    }
    if let Some(Value::String(s)) = obj.get(NUMBER_DOUBLE_KEY) {
        let value = match s.as_str() {
            "NaN" => Numeric::NaN,
            "Infinity" | "Inf" => Numeric::PositiveInfinity,
            "-Infinity" | "-Inf" => Numeric::NegInfinity,
            _ => parse_finite(s)?,
        };
        return Some(NumericValue {
            kind: NumericKind::Double,
            value,
        });
    }
    if obj.contains_key(NUMBER_LONG_KEY) {
        let Value::String(s) = obj.get(NUMBER_LONG_KEY)? else {
            return None;
        };
        let n = s.parse::<i64>().ok()?;
        return Some(NumericValue {
            kind: NumericKind::Long,
            value: from_i128(n as i128),
        });
    }
    if obj.contains_key(NUMBER_INT_KEY) {
        let Value::String(s) = obj.get(NUMBER_INT_KEY)? else {
            return None;
        };
        let n = s.parse::<i32>().ok()?;
        return Some(NumericValue {
            kind: NumericKind::Int,
            value: from_i128(n as i128),
        });
    }
    None
}

pub fn is_numeric(value: &Value) -> bool {
    parse_value(value).is_some()
}

pub fn is_decimal(value: &Value) -> bool {
    parse_value(value).is_some_and(|v| v.kind_is_decimal())
}

pub fn contains_numeric(value: &Value) -> bool {
    if is_numeric(value) {
        return true;
    }
    match value {
        Value::Array(items) => items.iter().any(contains_numeric),
        Value::Object(obj) => obj.values().any(contains_numeric),
        _ => false,
    }
}

/// Canonicalizes decimal/double Extended JSON objects in a document value.
pub fn normalize_value(value: &mut Value) -> Result<()> {
    if let Some(obj) = value.as_object() {
        if obj.len() == 1 && obj.contains_key(NUMBER_DECIMAL_KEY) {
            let text = {
                let Some(Value::String(s)) = obj.get(NUMBER_DECIMAL_KEY) else {
                    return Err(Error::InvalidNumeric(
                        "`$numberDecimal` must be a string".into(),
                    ));
                };
                let dec = Decimal128::from_str(s)
                    .map_err(|e| Error::InvalidNumeric(format!("invalid `$numberDecimal`: {e}")))?;
                let text = dec.to_string();
                parse_finite(&text).ok_or_else(|| {
                    Error::InvalidNumeric("`$numberDecimal` must be a finite number".into())
                })?;
                text
            };
            *value = json!({ NUMBER_DECIMAL_KEY: text });
            return Ok(());
        }
        if obj.len() == 1 && obj.contains_key(NUMBER_DOUBLE_KEY) {
            let canonical = {
                let Some(Value::String(s)) = obj.get(NUMBER_DOUBLE_KEY) else {
                    return Err(Error::InvalidNumeric(
                        "`$numberDouble` must be a string".into(),
                    ));
                };
                match s.as_str() {
                    "NaN" => "NaN".to_string(),
                    "Infinity" | "Inf" => "Infinity".to_string(),
                    "-Infinity" | "-Inf" => "-Infinity".to_string(),
                    _ => {
                        let f = s.parse::<f64>().map_err(|e| {
                            Error::InvalidNumeric(format!("invalid `$numberDouble`: {e}"))
                        })?;
                        if !f.is_finite() {
                            return Err(Error::InvalidNumeric(
                                "`$numberDouble` finite form must be finite".into(),
                            ));
                        }
                        format!("{f:?}")
                    }
                }
            };
            *value = json!({ NUMBER_DOUBLE_KEY: canonical });
            return Ok(());
        }
        if obj.len() == 1 && obj.contains_key(NUMBER_LONG_KEY) {
            let canonical = {
                let Some(Value::String(s)) = obj.get(NUMBER_LONG_KEY) else {
                    return Err(Error::InvalidNumeric(
                        "`$numberLong` must be a string".into(),
                    ));
                };
                let n = s
                    .parse::<i64>()
                    .map_err(|e| Error::InvalidNumeric(format!("invalid `$numberLong`: {e}")))?;
                n.to_string()
            };
            *value = json!({ NUMBER_LONG_KEY: canonical });
            return Ok(());
        }
        if obj.len() == 1 && obj.contains_key(NUMBER_INT_KEY) {
            let canonical = {
                let Some(Value::String(s)) = obj.get(NUMBER_INT_KEY) else {
                    return Err(Error::InvalidNumeric(
                        "`$numberInt` must be a string".into(),
                    ));
                };
                let n = s
                    .parse::<i32>()
                    .map_err(|e| Error::InvalidNumeric(format!("invalid `$numberInt`: {e}")))?;
                n.to_string()
            };
            *value = json!({ NUMBER_INT_KEY: canonical });
            return Ok(());
        }
    }
    if let Some(obj) = value.as_object_mut() {
        for item in obj.values_mut() {
            normalize_value(item)?;
        }
    } else if let Some(items) = value.as_array_mut() {
        for item in items {
            normalize_value(item)?;
        }
    }
    Ok(())
}

/// Canonicalizes numeric Extended JSON values inside a top-level document.
pub fn normalize_document(value: &mut Value) -> Result<()> {
    if let Some(obj) = value.as_object_mut() {
        for item in obj.values_mut() {
            normalize_value(item)?;
        }
    }
    Ok(())
}

pub fn cmp_values(a: &Value, b: &Value) -> Option<Ordering> {
    let a = parse_value(a)?;
    let b = parse_value(b)?;
    Some(a.value.cmp_value(b.value))
}

pub fn eq_values(a: &Value, b: &Value) -> Option<bool> {
    Some(cmp_values(a, b)? == Ordering::Equal)
}

/// Order-preserving, fixed-size numeric index key payload.
pub fn encode_ordered(value: &Value) -> Option<[u8; 39]> {
    let numeric = parse_value(value)?.value;
    let mut out = [0u8; 39];
    match numeric {
        Numeric::NaN => out[0] = 0,
        Numeric::NegInfinity => out[0] = 1,
        Numeric::Finite {
            negative: true,
            exponent,
            digits,
            len,
        } => {
            out[0] = 2;
            let mag = magnitude(exponent, digits, len);
            for (dst, src) in out[1..].iter_mut().zip(mag) {
                *dst = !src;
            }
        }
        Numeric::Finite {
            negative: false,
            exponent: _,
            digits,
            len,
        } if len == 1 && digits[0] == 0 => out[0] = 3,
        Numeric::Finite {
            negative: false,
            exponent,
            digits,
            len,
        } => {
            out[0] = 4;
            out[1..].copy_from_slice(&magnitude(exponent, digits, len));
        }
        Numeric::PositiveInfinity => out[0] = 5,
    }
    Some(out)
}

fn magnitude(exponent: i32, digits: [u8; MAX_DIGITS], len: u8) -> [u8; 38] {
    let mut out = [0u8; 38];
    let adj = exponent + len as i32 + ADJ_BIAS;
    out[..4].copy_from_slice(&(adj as u32).to_be_bytes());
    out[4..4 + len as usize].copy_from_slice(&digits[..len as usize]);
    out
}

/// Returns the textual form used by full-text/fuzzy indexes.
pub fn text_value(value: &Value) -> Option<String> {
    if let Value::Number(n) = value {
        return Some(n.to_string());
    }
    let obj = value.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    for key in [
        NUMBER_INT_KEY,
        NUMBER_LONG_KEY,
        NUMBER_DOUBLE_KEY,
        NUMBER_DECIMAL_KEY,
    ] {
        if let Some(Value::String(s)) = obj.get(key) {
            return Some(s.clone());
        }
    }
    None
}

/// Converts a numeric value to `BigDecimal` for decimal arithmetic.
pub fn as_bigdecimal(value: &Value) -> Option<BigDecimal> {
    parse_value(value)?.value.to_bigdecimal()
}

/// Converts any numeric value (including Extended JSON numbers) to `f64`.
pub fn as_f64(value: &Value) -> Option<f64> {
    match parse_value(value)?.value {
        Numeric::NaN => Some(f64::NAN),
        Numeric::NegInfinity => Some(f64::NEG_INFINITY),
        Numeric::PositiveInfinity => Some(f64::INFINITY),
        finite => finite.to_bigdecimal()?.to_f64(),
    }
}

/// Returns the Extended JSON integer kind, if the value is `$numberInt` or `$numberLong`.
pub fn extended_integer_kind(value: &Value) -> Option<NumericKind> {
    let obj = value.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    if obj.contains_key(NUMBER_LONG_KEY) {
        Some(NumericKind::Long)
    } else if obj.contains_key(NUMBER_INT_KEY) {
        Some(NumericKind::Int)
    } else {
        None
    }
}

/// Returns the exact integer value for JSON integers and `$numberInt`/`$numberLong`.
pub fn integer_value(value: &Value) -> Option<i128> {
    let numeric = parse_value(value)?;
    if !matches!(numeric.kind, NumericKind::Int | NumericKind::Long) {
        return None;
    }
    let Numeric::Finite {
        negative,
        exponent,
        digits,
        len,
    } = numeric.value
    else {
        return None;
    };
    if exponent != 0 {
        return None;
    }
    let mut out = 0i128;
    for &d in &digits[..len as usize] {
        out = out.checked_mul(10)?.checked_add(d as i128)?;
    }
    if negative {
        out = -out;
    }
    Some(out)
}

/// Builds an Extended JSON integer result with the requested BSON type.
pub fn integer_result(n: i128, kind: NumericKind) -> Result<Value> {
    match kind {
        NumericKind::Long => {
            let v = i64::try_from(n)
                .map_err(|_| Error::InvalidNumeric("$inc integer overflow".into()))?;
            Ok(json!({ NUMBER_LONG_KEY: v.to_string() }))
        }
        NumericKind::Int => {
            let v = i32::try_from(n)
                .map_err(|_| Error::InvalidNumeric("$inc integer overflow".into()))?;
            Ok(json!({ NUMBER_INT_KEY: v.to_string() }))
        }
        _ => integer_json(n),
    }
}

/// Builds the narrowest plain JSON number for an integer result.
pub fn integer_json(n: i128) -> Result<Value> {
    if let Ok(v) = i64::try_from(n) {
        return Ok(Value::from(v));
    }
    if let Ok(v) = u64::try_from(n) {
        return Ok(Value::from(v));
    }
    Err(Error::InvalidNumeric(
        "integer arithmetic result is out of int64/uint64 range".into(),
    ))
}

/// Builds a canonical `$numberDecimal` value.
///
/// Arithmetic results are rounded to 34 significant digits (the Decimal128
/// precision) using banker's rounding, matching decimal floating point.
pub fn decimal_value(value: &BigDecimal) -> Result<Value> {
    let rounded = if value.digits() > MAX_DIGITS as u64 {
        let (_, scale) = value.as_bigint_and_exponent();
        let keep_scale = scale - (value.digits() as i64 - MAX_DIGITS as i64);
        value.with_scale_round(keep_scale, RoundingMode::HalfEven)
    } else {
        value.clone()
    };
    let text = rounded.to_string();
    let dec = Decimal128::from_str(&text)
        .map_err(|e| Error::InvalidNumeric(format!("decimal result is out of range: {e}")))?;
    Ok(json!({ NUMBER_DECIMAL_KEY: dec.to_string() }))
}

fn from_number(n: &Number) -> Option<(NumericKind, Numeric)> {
    if let Some(i) = n.as_i64() {
        let kind = if i32::try_from(i).is_ok() {
            NumericKind::Int
        } else {
            NumericKind::Long
        };
        return Some((kind, from_i128(i as i128)));
    }
    if let Some(u) = n.as_u64() {
        let kind = if u <= i32::MAX as u64 {
            NumericKind::Int
        } else {
            NumericKind::Long
        };
        return Some((kind, from_i128(u as i128)));
    }
    Some((NumericKind::Double, finite_from_f64(n.as_f64()?)?))
}

fn from_i128(n: i128) -> Numeric {
    if n == 0 {
        return Numeric::zero();
    }
    let negative = n < 0;
    let mut magnitude = n.unsigned_abs();
    let mut rev = [0u8; MAX_DIGITS];
    let mut len = 0usize;
    while magnitude > 0 {
        rev[len] = (magnitude % 10) as u8;
        magnitude /= 10;
        len += 1;
    }
    let mut digits = [0u8; MAX_DIGITS];
    for i in 0..len {
        digits[i] = rev[len - 1 - i];
    }
    Numeric::Finite {
        negative,
        exponent: 0,
        digits,
        len: len as u8,
    }
}

fn finite_from_f64(f: f64) -> Option<Numeric> {
    if f.is_nan() {
        return Some(Numeric::NaN);
    }
    if f == f64::INFINITY {
        return Some(Numeric::PositiveInfinity);
    }
    if f == f64::NEG_INFINITY {
        return Some(Numeric::NegInfinity);
    }
    if f == 0.0 {
        return Some(Numeric::zero());
    }
    let negative = f < 0.0;
    let text = format!("{:.16e}", f.abs());
    let mut out = parse_finite(&text)?;
    if let Numeric::Finite {
        negative: ref mut n,
        ..
    } = out
    {
        *n = negative;
    }
    Some(out)
}

fn parse_finite(raw: &str) -> Option<Numeric> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    let (negative, rest) = match s.as_bytes()[0] {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let (mantissa, exp_part) = match rest.find(['e', 'E']) {
        Some(idx) => (&rest[..idx], rest[idx + 1..].parse::<i32>().ok()?),
        None => (rest, 0),
    };
    if mantissa.is_empty() {
        return None;
    }
    let mut digits = [0u8; MAX_DIGITS];
    let mut len = 0usize;
    let mut frac = 0i32;
    let mut seen_dot = false;
    for ch in mantissa.chars() {
        match ch {
            '.' if !seen_dot => seen_dot = true,
            '0'..='9' => {
                if len == MAX_DIGITS {
                    return None;
                }
                digits[len] = ch as u8 - b'0';
                len += 1;
                if seen_dot {
                    frac += 1;
                }
            }
            _ => return None,
        }
    }
    while len > 0 && digits[0] == 0 {
        digits.copy_within(1..len, 0);
        len -= 1;
    }
    if len == 0 {
        return Some(Numeric::zero());
    }
    let mut exponent = exp_part.checked_sub(frac)?;
    while len > 0 && digits[len - 1] == 0 {
        len -= 1;
        exponent = exponent.checked_add(1)?;
    }
    digits[len..].fill(0);
    Some(Numeric::Finite {
        negative,
        exponent,
        digits,
        len: len as u8,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_and_floats_compare_exactly() {
        assert_eq!(cmp_values(&json!(1), &json!(1.0)), Some(Ordering::Equal));
        assert_eq!(
            cmp_values(&json!(9007199254740993i64), &json!(9007199254740992i64)),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn decimal_compares_by_value() {
        let a = json!({"$numberDecimal": "0.10"});
        let b = json!({"$numberDecimal": "0.1"});
        let c = json!({"$numberDecimal": "0.10000000000000001"});
        assert_eq!(eq_values(&a, &b), Some(true));
        assert_eq!(eq_values(&a, &c), Some(false));
        assert_eq!(cmp_values(&c, &a), Some(Ordering::Greater));
    }

    #[test]
    fn numeric_index_order_matches_comparison() {
        let values = [
            json!({"$numberDouble": "NaN"}),
            json!({"$numberDouble": "-Infinity"}),
            json!(-100),
            json!({"$numberDecimal": "-0.5"}),
            json!(0),
            json!({"$numberDecimal": "0.5"}),
            json!(1),
            json!(1.0),
            json!({"$numberDecimal": "1.00"}),
            json!(2),
            json!({"$numberDouble": "Infinity"}),
        ];
        for pair in values.windows(2) {
            assert_ne!(
                cmp_values(&pair[0], &pair[1]),
                Some(Ordering::Greater),
                "{} > {}",
                pair[0],
                pair[1]
            );
            assert!(
                encode_ordered(&pair[0]) <= encode_ordered(&pair[1]),
                "index order mismatch: {} {}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn canonical_decimal_roundtrip() {
        let mut doc = json!({"price": {"$numberDecimal": "199.9900"}});
        normalize_value(&mut doc).unwrap();
        assert_eq!(doc["price"]["$numberDecimal"], "199.9900");
        assert_eq!(as_bigdecimal(&doc["price"]).unwrap().to_string(), "199.99");
    }

    #[test]
    fn decimal_division_rounds_to_decimal128_precision() {
        let one: BigDecimal = "1".parse().unwrap();
        let three: BigDecimal = "3".parse().unwrap();
        let third = one / three;
        let value = decimal_value(&third).unwrap();
        assert_eq!(
            value[NUMBER_DECIMAL_KEY],
            "0.3333333333333333333333333333333333"
        );
    }

    #[test]
    fn arithmetic_result_must_fit_decimal128() {
        let value = "1.5".parse::<BigDecimal>().unwrap();
        assert_eq!(decimal_value(&value).unwrap()["$numberDecimal"], "1.5");
        let huge = "1e7000".parse::<BigDecimal>().unwrap();
        assert!(decimal_value(&huge).is_err());
    }
}
