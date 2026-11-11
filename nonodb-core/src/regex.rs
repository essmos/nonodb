//! Safe MongoDB-style `$regex` support.
//!
//! The Rust `regex` engine has no backtracking, so matching time is linear in
//! the input size. Patterns are still bounded and compiled with memory limits.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use ::regex::{Regex, RegexBuilder};
use serde_json::Value;

use crate::bson_types;
use crate::date;
use crate::numeric;
use crate::{Error, Result};

/// Maximum accepted pattern length in bytes.
pub const MAX_PATTERN_BYTES: usize = 4096;
const CACHE_LIMIT: usize = 256;
const REGEX_SIZE_LIMIT: usize = 2 * 1024 * 1024;
const REGEX_DFA_SIZE_LIMIT: usize = 2 * 1024 * 1024;

type Cache = HashMap<(String, String), Arc<Regex>>;

fn cache() -> &'static Mutex<Cache> {
    static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Parses a regex argument and optional `$options`.
///
/// The argument may be a plain pattern string or an Extended JSON
/// `{"$regularExpression": {"pattern": "...", "options": "..."}}` object.
pub fn parse_argument(arg: &Value, options: Option<&Value>) -> Result<(String, String)> {
    let (pattern, inline_options) = match arg {
        Value::String(s) => (s.clone(), String::new()),
        Value::Object(obj) => {
            let re = obj
                .get("$regularExpression")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    Error::Query("$regex must be a string or $regularExpression object".into())
                })?;
            let pattern = re.get("pattern").and_then(Value::as_str).ok_or_else(|| {
                Error::Query("$regularExpression.pattern must be a string".into())
            })?;
            let options = re
                .get("options")
                .and_then(Value::as_str)
                .unwrap_or_default();
            (pattern.to_string(), options.to_string())
        }
        _ => return Err(Error::Query("$regex must be a string".into())),
    };

    let options = match options {
        Some(Value::String(s)) => s.clone(),
        Some(_) => return Err(Error::Query("$options must be a string".into())),
        None => inline_options,
    };
    Ok((pattern, options))
}

fn validate_options(options: &str) -> Result<()> {
    for ch in options.chars() {
        if !matches!(ch, 'i' | 'm' | 's' | 'x' | 'u') {
            return Err(Error::Query(format!(
                "unsupported $regex option `{ch}` (allowed: i, m, s, x, u)"
            )));
        }
    }
    Ok(())
}

/// Compiles a pattern, using a bounded process-wide cache.
pub fn compile(pattern: &str, options: &str) -> Result<Arc<Regex>> {
    if pattern.len() > MAX_PATTERN_BYTES {
        return Err(Error::Query(format!(
            "$regex pattern exceeds {MAX_PATTERN_BYTES} bytes"
        )));
    }
    validate_options(options)?;
    let key = (pattern.to_string(), options.to_string());
    let mut cache = cache().lock().unwrap_or_else(|e| e.into_inner());
    if let Some(re) = cache.get(&key) {
        return Ok(Arc::clone(re));
    }

    let mut builder = RegexBuilder::new(pattern);
    builder
        .case_insensitive(options.contains('i'))
        .multi_line(options.contains('m'))
        .dot_matches_new_line(options.contains('s'))
        .ignore_whitespace(options.contains('x'))
        .unicode(true)
        .size_limit(REGEX_SIZE_LIMIT)
        .dfa_size_limit(REGEX_DFA_SIZE_LIMIT);
    let re = builder
        .build()
        .map_err(|e| Error::Query(format!("invalid $regex: {e}")))?;
    let re = Arc::new(re);

    if cache.len() >= CACHE_LIMIT {
        cache.clear();
    }
    cache.insert(key, Arc::clone(&re));
    Ok(re)
}

/// Matches a regex condition against a field value.
///
/// Arrays match when any element matches. Missing fields and non-string
/// scalars never match.
pub fn is_match(actual: Option<&Value>, arg: &Value, options: Option<&Value>) -> bool {
    let Ok((pattern, options)) = parse_argument(arg, options) else {
        return false;
    };
    let Ok(re) = compile(&pattern, &options) else {
        return false;
    };
    regex_value_matches(actual, &re)
}

fn regex_value_matches(actual: Option<&Value>, re: &Regex) -> bool {
    match actual {
        Some(Value::String(s)) => re.is_match(s),
        Some(Value::Array(items)) => items.iter().any(|item| regex_value_matches(Some(item), re)),
        _ => false,
    }
}

/// Validates every regex condition in a filter before executing it.
pub fn validate_filter(filter: &Value) -> Result<()> {
    let Some(obj) = filter.as_object() else {
        return Ok(());
    };
    for (key, cond) in obj {
        match key.as_str() {
            "$and" | "$or" | "$nor" => {
                if let Some(arr) = cond.as_array() {
                    for f in arr {
                        validate_filter(f)?;
                    }
                }
            }
            "$not" => validate_filter(cond)?,
            _ => validate_condition(cond)?,
        }
    }
    Ok(())
}

pub(crate) fn validate_condition(cond: &Value) -> Result<()> {
    if date::is_date(cond) || numeric::is_numeric(cond) || bson_types::is_bson_value(cond) {
        return Ok(());
    }
    let Some(obj) = cond.as_object() else {
        return Ok(());
    };
    if obj.is_empty() || !obj.keys().all(|k| k.starts_with('$')) {
        return Ok(());
    }
    if let Some(arg) = obj.get("$regex") {
        let (pattern, options) = parse_argument(arg, obj.get("$options"))?;
        compile(&pattern, &options)?;
    } else if obj.contains_key("$options") {
        return Err(Error::Query("$options requires $regex".into()));
    }
    if let Some(not) = obj.get("$not") {
        validate_condition(not)?;
    }
    Ok(())
}

/// Returns a literal prefix for an anchored, case-sensitive pattern.
///
/// Used only as an index range hint. The residual matcher still verifies every
/// document, so a conservative `None` is always safe.
pub fn condition_prefix(cond: &Value) -> Option<String> {
    let obj = cond.as_object()?;
    if obj.keys().any(|k| k != "$regex" && k != "$options") {
        return None;
    }
    let (pattern, options) = parse_argument(obj.get("$regex")?, obj.get("$options")).ok()?;
    if !options.chars().all(|c| c == 'u') {
        return None;
    }
    let mut chars = pattern.chars();
    if chars.next()? != '^' {
        return None;
    }
    let mut prefix = String::new();
    for ch in chars {
        if matches!(
            ch,
            '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\'
        ) {
            break;
        }
        prefix.push(ch);
    }
    (!prefix.is_empty()).then_some(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn options_case_and_multiline() {
        assert!(is_match(
            Some(&json!("Ala")),
            &json!("^ala$"),
            Some(&json!("i"))
        ));
        assert!(!is_match(Some(&json!("Ala")), &json!("^ala$"), None));
        assert!(is_match(
            Some(&json!("one\ntwo")),
            &json!("^two$"),
            Some(&json!("m"))
        ));
        assert!(is_match(
            Some(&json!("a b")),
            &json!("a\\s+b"),
            Some(&json!("x"))
        ));
    }

    #[test]
    fn arrays_and_non_strings() {
        assert!(is_match(
            Some(&json!(["x", "Ala"])),
            &json!("^ala$"),
            Some(&json!("i"))
        ));
        assert!(!is_match(Some(&json!(42)), &json!("42"), None));
        assert!(!is_match(None, &json!(".*"), None));
    }

    #[test]
    fn extended_regex_argument() {
        assert!(is_match(
            Some(&json!("ABC")),
            &json!({"$regularExpression": {"pattern": "^abc$", "options": "i"}}),
            None
        ));
    }

    #[test]
    fn invalid_patterns_and_options_are_rejected() {
        assert!(compile("(", "").is_err());
        assert!(compile("x", "q").is_err());
        assert!(validate_filter(&json!({"a": {"$options": "i"}})).is_err());
        assert!(validate_filter(&json!({"a": {"$regex": "(", "$options": "i"}})).is_err());
    }

    #[test]
    fn anchored_prefix_is_conservative() {
        assert_eq!(
            condition_prefix(&json!({"$regex": "^hello"})),
            Some("hello".into())
        );
        assert_eq!(
            condition_prefix(&json!({"$regex": "^hello.*world"})),
            Some("hello".into())
        );
        assert_eq!(condition_prefix(&json!({"$regex": "hello"})), None);
        assert_eq!(
            condition_prefix(&json!({"$regex": "^hello", "$options": "i"})),
            None
        );
        assert_eq!(condition_prefix(&json!({"$regex": "^$"})), None);
    }
}
