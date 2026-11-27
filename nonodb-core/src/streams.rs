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
    /// Highest offset handed out to a batch. Batches are contiguous ranges
    /// above this watermark, so claims never overlap.
    #[serde(default)]
    pub next_seq: u64,
    /// Redelivery attempts before a batch is dead-lettered.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Oldest offset still available, set when retention crossed the group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncated_below: Option<u64>,
    pub created_at: u64,
    pub updated_at: u64,
}

fn default_max_attempts() -> u32 {
    5
}

impl StreamGroup {
    pub fn new(stream: impl Into<String>, group: impl Into<String>, committed_seq: u64) -> Self {
        let now = now_ms();
        Self {
            stream: stream.into(),
            group: group.into(),
            committed_seq,
            next_seq: committed_seq,
            max_attempts: default_max_attempts(),
            truncated_below: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_name(&self.stream, "stream")?;
        validate_name(&self.group, "group")?;
        if self.max_attempts == 0 {
            return Err(Error::Query("max_attempts must be positive".into()));
        }
        Ok(())
    }

    pub fn touch(&mut self) {
        self.updated_at = now_ms();
    }
}

/// Metadata key prefix for in-flight batch leases.
pub(crate) const LEASE_PREFIX: &str = "streamlease:";

/// Metadata key prefix for dead-lettered batches.
pub(crate) const DEAD_PREFIX: &str = "streamdead:";

/// State of one batch lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseStatus {
    /// A member owns the range until `lease_until`.
    Leased,
    /// The range is waiting for redelivery after `lease_until`.
    Available,
    /// The range is fully processed and only waits for earlier ranges before
    /// the committed offset can move.
    Done,
}

/// One contiguous range of offsets handed to a member.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamLease {
    pub stream: String,
    pub group: String,
    pub from_seq: u64,
    pub to_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
    /// Unix milliseconds after which the range may be claimed again.
    #[serde(default)]
    pub lease_until: u64,
    pub attempts: u32,
    pub max_attempts: u32,
    pub status: LeaseStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
}

impl StreamLease {
    pub fn new(stream: &str, group: &str, from_seq: u64, to_seq: u64, max_attempts: u32) -> Self {
        let now = now_ms();
        Self {
            stream: stream.to_string(),
            group: group.to_string(),
            from_seq,
            to_seq,
            lease_id: None,
            member: None,
            lease_until: 0,
            attempts: 0,
            max_attempts,
            status: LeaseStatus::Available,
            error: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_name(&self.stream, "stream")?;
        validate_name(&self.group, "group")?;
        if self.from_seq == 0 || self.to_seq < self.from_seq {
            return Err(Error::Query("invalid lease range".into()));
        }
        Ok(())
    }

    pub fn events(&self) -> u64 {
        self.to_seq - self.from_seq + 1
    }

    pub fn is_expired(&self, now: u64) -> bool {
        self.status == LeaseStatus::Leased && self.lease_until <= now
    }

    pub fn touch(&mut self) {
        self.updated_at = now_ms();
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
    fn new_group_starts_claiming_at_its_offset() {
        let group = StreamGroup::new("events", "g", 10);
        assert_eq!(group.committed_seq, 10);
        assert_eq!(group.next_seq, 10);
        assert_eq!(group.max_attempts, default_max_attempts());
    }

    #[test]
    fn lease_ranges_and_expiry() {
        let mut lease = StreamLease::new("events", "g", 5, 9, 3);
        assert_eq!(lease.events(), 5);
        assert!(!lease.is_expired(now_ms()));
        lease.status = LeaseStatus::Leased;
        lease.lease_until = 1;
        assert!(lease.is_expired(now_ms()));
        lease.status = LeaseStatus::Available;
        assert!(!lease.is_expired(1));
    }

    #[test]
    fn lag_never_underflows() {
        assert_eq!(lag(10, 4), 6);
        assert_eq!(lag(4, 10), 0);
    }
}
