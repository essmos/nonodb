//! Property-based tests: index encoding must preserve value order,
//! and the matcher/path helpers must stay consistent.

use proptest::prelude::*;
use serde_json::json;

use nonodb_core::index::encode_value;
use nonodb_core::query::{matches, path};

fn enc(v: &serde_json::Value) -> Vec<u8> {
    encode_value(v).expect("wartość skalarna")
}

proptest! {
    #[test]
    fn number_encoding_preserves_order(a in -1.0e12f64..1.0e12, b in -1.0e12f64..1.0e12) {
        let ea = enc(&json!(a));
        let eb = enc(&json!(b));
        prop_assert_eq!(ea.cmp(&eb), a.partial_cmp(&b).unwrap());
    }

    #[test]
    fn string_encoding_preserves_order(a in any::<String>(), b in any::<String>()) {
        let ea = enc(&json!(a));
        let eb = enc(&json!(b));
        prop_assert_eq!(ea.cmp(&eb), a.cmp(&b));
    }

    #[test]
    fn eq_is_reflexive(v in any::<i64>()) {
        let doc = json!({ "x": v });
        let hit = matches(&doc, &json!({ "x": v }));
        let miss = matches(&doc, &json!({ "x": { "$ne": v } }));
        prop_assert!(hit);
        prop_assert!(!miss);
    }

    #[test]
    fn set_then_get_roundtrip(key in "[a-z]{1,6}", val in any::<i64>()) {
        let mut doc = json!({});
        path::set(&mut doc, &key, json!(val)).unwrap();
        prop_assert_eq!(path::get(&doc, &key), Some(&json!(val)));
    }

    #[test]
    fn unset_removes_key(key in "[a-z]{1,6}", val in any::<i64>()) {
        let mut doc = json!({});
        path::set(&mut doc, &key, json!(val)).unwrap();
        prop_assert!(path::unset(&mut doc, &key));
        prop_assert!(path::get(&doc, &key).is_none());
        prop_assert!(!path::unset(&mut doc, &key));
    }
}
