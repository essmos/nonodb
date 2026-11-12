//! Conditional collection subscriptions evaluated by the writer.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::error::{Error, Result};

pub(crate) fn meta_key(coll: &str) -> String {
    format!("subscriptions:{coll}")
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct CountCondition {
    #[serde(default)]
    pub filter: Value,
    #[serde(default)]
    pub gt: Option<u64>,
    #[serde(default)]
    pub gte: Option<u64>,
    #[serde(default)]
    pub lt: Option<u64>,
    #[serde(default)]
    pub lte: Option<u64>,
    #[serde(default)]
    pub eq: Option<u64>,
}

impl CountCondition {
    pub fn matches(&self, count: u64) -> bool {
        if let Some(value) = self.gt {
            return count > value;
        }
        if let Some(value) = self.gte {
            return count >= value;
        }
        if let Some(value) = self.lt {
            return count < value;
        }
        if let Some(value) = self.lte {
            return count <= value;
        }
        if let Some(value) = self.eq {
            return count == value;
        }
        count > 0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Subscription {
    pub name: String,
    #[serde(default)]
    pub ops: Vec<String>,
    #[serde(default)]
    pub filter: Value,
    #[serde(default)]
    pub count: Option<CountCondition>,
    /// Emit only when the condition changes from false to true.
    #[serde(default)]
    pub once: bool,
    /// Optional delivered event type. Defaults to `subscription.<name>`.
    #[serde(default)]
    pub event_type: Option<String>,
    #[serde(default)]
    pub last_matched: bool,
    /// Client lease in seconds. Defaults to 600 (10 minutes).
    #[serde(default)]
    pub lease_seconds: u64,
    /// Absolute expiry in Unix milliseconds. Persisted and checked by the writer.
    #[serde(default)]
    pub expires_at: u64,
}

impl Subscription {
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(Error::Query("subscription name must not be empty".into()));
        }
        if self.name.len() > 200 {
            return Err(Error::Query("subscription name is too long".into()));
        }
        for op in &self.ops {
            if !matches!(op.as_str(), "insert" | "update" | "delete") {
                return Err(Error::Query(format!(
                    "unsupported subscription op `{op}`; use insert/update/delete"
                )));
            }
        }
        if self
            .event_type
            .as_deref()
            .is_some_and(|value| value.is_empty())
        {
            return Err(Error::Query(
                "subscription event_type must not be empty".into(),
            ));
        }
        Ok(())
    }

    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.expires_at == 0 || now_ms >= self.expires_at
    }

    pub fn watches_op(&self, op: &str) -> bool {
        self.ops.is_empty() || self.ops.iter().any(|candidate| candidate == op)
    }

    pub fn event_type(&self) -> String {
        self.event_type
            .clone()
            .unwrap_or_else(|| format!("subscription.{}", self.name))
    }

    pub fn event(
        &self,
        coll: &str,
        op: &str,
        doc_id: &str,
        before: Option<&Value>,
        after: Option<&Value>,
        count: Option<u64>,
    ) -> Value {
        json!({
            "_id": crate::events::new_event_id(),
            "type": self.event_type(),
            "doc_id": doc_id,
            "payload": {
                "subscription": self.name,
                "coll": coll,
                "op": op,
                "before": before.cloned().unwrap_or(Value::Null),
                "after": after.cloned().unwrap_or(Value::Null),
                "count": count,
            },
            "created_at": now_ms(),
        })
    }
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_conditions_support_all_comparisons() {
        let cases = [
            (
                CountCondition {
                    gt: Some(10),
                    ..Default::default()
                },
                11,
                true,
            ),
            (
                CountCondition {
                    gt: Some(10),
                    ..Default::default()
                },
                10,
                false,
            ),
            (
                CountCondition {
                    gte: Some(10),
                    ..Default::default()
                },
                10,
                true,
            ),
            (
                CountCondition {
                    lt: Some(10),
                    ..Default::default()
                },
                9,
                true,
            ),
            (
                CountCondition {
                    lte: Some(10),
                    ..Default::default()
                },
                10,
                true,
            ),
            (
                CountCondition {
                    eq: Some(10),
                    ..Default::default()
                },
                10,
                true,
            ),
        ];
        for (condition, count, expected) in cases {
            assert_eq!(condition.matches(count), expected);
        }
    }
}
