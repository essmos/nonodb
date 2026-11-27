//! Streams: durable offsets and consumer groups over append-only collections.
//!
//! A stream is a named, ordered view over one append-only collection. The single
//! writer assigns a strictly increasing `_seq` to every inserted document of a
//! stream collection, and consumers commit offsets to the server instead of
//! keeping them client-side.
//!
//! This module defines the metadata records and their validation rules. Storage
//! lives in the shared metadata sub-database (see `Store::set_stream_in` and
//! friends), so stream state replicates like other metadata.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Error, Result};

/// Reserved field carrying the stream offset inside an event document.
pub const SEQ_FIELD: &str = "_seq";

/// Metadata key prefix for stream definitions.
pub(crate) const STREAM_PREFIX: &str = "stream:";

/// Metadata key prefix for the next offset of a stream.
pub(crate) const STREAM_SEQ_PREFIX: &str = "stream:next:";

/// Metadata key prefix for consumer groups.
pub(crate) const GROUP_PREFIX: &str = "streamgroup:";

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn default_seq_field() -> String {
    SEQ_FIELD.to_string()
}

/// Retention policy for a stream.
///
/// Enforcement lives in the daemon retention pass; the fields are validated here
/// so a definition cannot be created with a policy that can never be honoured.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Retention {
    /// Keep at most this many newest events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_events: Option<u64>,
    /// Keep events newer than this many milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age_ms: Option<u64>,
    /// Allow deleting below a group offset, leaving this many events behind.
    #[serde(default)]
    pub min_seq_keep: u64,
}

impl Retention {
    pub fn validate(&self) -> Result<()> {
        if self.max_events == Some(0) {
            return Err(Error::Query(
                "retention.max_events must be greater than zero".into(),
            ));
        }
        if self.max_age_ms == Some(0) {
            return Err(Error::Query(
                "retention.max_age_ms must be greater than zero".into(),
            ));
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.max_events.is_none() && self.max_age_ms.is_none() && self.min_seq_keep == 0
    }
}

/// One stream definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamDef {
    /// Stream name. Independent of the collection name so one collection can be
    /// exposed under a stable stream name.
    pub name: String,
    pub coll: String,
    #[serde(default = "default_seq_field")]
    pub seq_field: String,
    #[serde(default)]
    pub retention: Retention,
    pub created_at: u64,
}

impl StreamDef {
    pub fn new(name: impl Into<String>, coll: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            coll: coll.into(),
            seq_field: default_seq_field(),
            retention: Retention::default(),
            created_at: now_ms(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_name(&self.name, "stream")?;
        validate_name(&self.coll, "collection")?;
        if self.seq_field != SEQ_FIELD {
            return Err(Error::Query(format!(
                "stream seq_field must be `{SEQ_FIELD}`"
            )));
        }
        if self.coll.starts_with('_') && self.coll != crate::events::EVENTS_COLLECTION {
            return Err(Error::Query(
                "streams can only be defined over `_events` or a normal collection".into(),
            ));
        }
        self.retention.validate()
    }
}

/// Where a newly created group starts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartAt {
    /// Start at the oldest retained event.
    Earliest,
    /// Start at the current head (default): only new events are delivered.
    #[default]
    Latest,
    /// Start at an explicit offset.
    Seq(u64),
}

/// Where a reset moves a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResetTo {
    Earliest,
    Latest,
    Seq(u64),
}

/// One consumer group over a stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamGroup {
    pub stream: String,
    pub group: String,
    /// Highest offset the group has fully processed.
    #[serde(default)]
    pub committed_seq: u64,
    /// Fencing token of the batch currently being processed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    /// Unix milliseconds after which the current batch may be claimed again.
    #[serde(default)]
    pub lease_until: u64,
    /// Member holding the current batch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
    /// Oldest offset still available, set when retention crossed the group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncated_below: Option<u64>,
    pub created_at: u64,
    pub updated_at: u64,
}

impl StreamGroup {
    pub fn new(stream: impl Into<String>, group: impl Into<String>, committed_seq: u64) -> Self {
        let now = now_ms();
        Self {
            stream: stream.into(),
            group: group.into(),
            committed_seq,
            lease_id: None,
            lease_until: 0,
            member: None,
            truncated_below: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_name(&self.stream, "stream")?;
        validate_name(&self.group, "group")?;
        Ok(())
    }

    pub fn touch(&mut self) {
        self.updated_at = now_ms();
    }

    /// Clears the in-flight batch. Used by commit, nack, and reset.
    pub fn clear_lease(&mut self) {
        self.lease_id = None;
        self.lease_until = 0;
        self.member = None;
        self.touch();
    }
}

/// Validates a stream, group, or collection name.
pub fn validate_name(name: &str, kind: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::Query(format!("{kind} name must not be empty")));
    }
    if name.len() > 128 {
        return Err(Error::Query(format!(
            "{kind} name must be at most 128 bytes"
        )));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(Error::Query(format!(
            "{kind} name may only contain letters, digits, `-`, `_` and `.`"
        )));
    }
    Ok(())
}

/// Rejects a user-supplied reserved offset field.
pub fn reject_reserved_fields(doc: &Value, seq_field: &str) -> Result<()> {
    if doc
        .as_object()
        .is_some_and(|object| object.contains_key(seq_field))
    {
        return Err(Error::Query(format!(
            "`{seq_field}` is reserved and managed by the stream writer"
        )));
    }
    Ok(())
}

/// Computes the group lag for a stream head.
pub fn lag(head: u64, committed: u64) -> u64 {
    head.saturating_sub(committed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn definition_validation_rejects_bad_names_and_seq_field() {
        let mut def = StreamDef::new("events", "_events");
        assert!(def.validate().is_ok());

        def.name = String::new();
        assert!(def.validate().is_err());

        let mut def = StreamDef::new("events", "_events");
        def.seq_field = "seq".into();
        assert!(def.validate().is_err());

        let mut def = StreamDef::new("events", "_internal");
        def.coll = "_internal".into();
        assert!(def.validate().is_err());

        assert!(StreamDef::new("a b", "_events").validate().is_err());
    }

    #[test]
    fn retention_rejects_zero_bounds() {
        let mut retention = Retention {
            max_events: Some(0),
            ..Default::default()
        };
        assert!(retention.validate().is_err());
        retention.max_events = Some(10);
        assert!(retention.validate().is_ok());
        retention.max_age_ms = Some(0);
        assert!(retention.validate().is_err());
    }

    #[test]
    fn reserved_field_is_rejected() {
        assert!(reject_reserved_fields(&json!({"_seq": 1}), SEQ_FIELD).is_err());
        assert!(reject_reserved_fields(&json!({"a": 1}), SEQ_FIELD).is_ok());
    }

    #[test]
    fn group_commit_state_is_cleared() {
        let mut group = StreamGroup::new("events", "g", 10);
        group.lease_id = Some("lease".into());
        group.lease_until = 5;
        group.member = Some("m1".into());
        group.clear_lease();
        assert!(group.lease_id.is_none());
        assert_eq!(group.lease_until, 0);
        assert!(group.member.is_none());
        assert_eq!(group.committed_seq, 10);
    }

    #[test]
    fn lag_never_underflows() {
        assert_eq!(lag(10, 4), 6);
        assert_eq!(lag(4, 10), 0);
    }
}
