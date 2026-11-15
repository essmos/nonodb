//! Deployment profiles for small hosts.
//!
//! A profile only fills variables that the operator did not set explicitly.
//! This keeps `NONODB_PROFILE=small` useful as a safe starting point without
//! preventing per-deployment overrides.

const SMALL: &[(&str, &str)] = &[
    ("NONODB_BLOCKING_THREADS", "8"),
    ("NONODB_MAX_OPEN_DATABASES", "8"),
    ("NONODB_CACHE_ENTRIES", "128"),
    ("NONODB_CACHE_MAX_BYTES", "65536"),
    ("NONODB_SCAN_WORKERS", "2"),
    ("NONODB_BACKFILL_WORKERS", "1"),
    ("NONODB_AGGREGATE_WORKERS", "1"),
    ("NONODB_TTL_SWEEP_INTERVAL", "10"),
    ("NONODB_TTL_SWEEP_LIMIT", "128"),
];

const TINY: &[(&str, &str)] = &[
    ("NONODB_BLOCKING_THREADS", "4"),
    ("NONODB_MAX_OPEN_DATABASES", "4"),
    ("NONODB_CACHE_ENTRIES", "64"),
    ("NONODB_CACHE_MAX_BYTES", "32768"),
    ("NONODB_SCAN_WORKERS", "1"),
    ("NONODB_BACKFILL_WORKERS", "1"),
    ("NONODB_AGGREGATE_WORKERS", "1"),
    ("NONODB_TTL_SWEEP_INTERVAL", "15"),
    ("NONODB_TTL_SWEEP_LIMIT", "64"),
];

fn defaults_for(profile: &str) -> Result<Option<&'static [(&'static str, &'static str)]>, String> {
    match profile {
        "" | "default" => Ok(None),
        "small" => Ok(Some(SMALL)),
        "tiny" => Ok(Some(TINY)),
        other => Err(format!(
            "unknown NONODB_PROFILE `{other}`; expected `small`, `tiny`, or `default`"
        )),
    }
}

fn is_set(key: &str) -> bool {
    std::env::var_os(key).is_some_and(|value| !value.is_empty())
}

/// Applies `NONODB_PROFILE` before any runtime or storage configuration is read.
pub fn apply() -> Result<(), String> {
    let profile = std::env::var("NONODB_PROFILE").unwrap_or_default();
    let Some(defaults) = defaults_for(&profile)? else {
        return Ok(());
    };
    for (key, value) in defaults {
        if !is_set(key) {
            std::env::set_var(key, value);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_profile_is_a_noop() {
        assert!(defaults_for("").unwrap().is_none());
        assert!(defaults_for("default").unwrap().is_none());
    }

    #[test]
    fn small_profile_is_conservative() {
        let values = defaults_for("small").unwrap().unwrap();
        assert!(values.contains(&("NONODB_BLOCKING_THREADS", "8")));
        assert!(values.contains(&("NONODB_MAX_OPEN_DATABASES", "8")));
        assert!(values.contains(&("NONODB_SCAN_WORKERS", "2")));
        assert!(values.contains(&("NONODB_BACKFILL_WORKERS", "1")));
    }

    #[test]
    fn tiny_profile_is_smaller_than_small() {
        let small = defaults_for("small").unwrap().unwrap();
        let tiny = defaults_for("tiny").unwrap().unwrap();
        assert!(tiny.contains(&("NONODB_BLOCKING_THREADS", "4")));
        assert!(tiny.contains(&("NONODB_MAX_OPEN_DATABASES", "4")));
        assert!(tiny.len() <= small.len());
    }

    #[test]
    fn unknown_profile_is_rejected() {
        let error = defaults_for("medium").unwrap_err();
        assert!(error.contains("unknown NONODB_PROFILE"));
    }
}
