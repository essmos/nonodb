//! Append-only document history.
//!
//! History entries are stored in a separate LMDB sub-database and never affect
//! the current collection or its indexes. Version numbers are global, monotonic
//! revisions assigned by the single writer; no per-document counter is read or
//! updated on the write path.

use std::collections::HashMap;
use std::ops::Bound;
use std::time::{SystemTime, UNIX_EPOCH};

use heed::types::Bytes;
use heed::{Database, RoTxn, RwTxn};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};

/// History sub-database name.
pub const HISTORY_DB: &str = "history";
/// Metadata key holding the highest allocated global revision.
const HISTORY_SEQ_KEY: &[u8] = b"history_seq";
/// Metadata key holding the last history key visited by pruning.
const HISTORY_PRUNE_CURSOR_KEY: &[u8] = b"history_prune_cursor";
/// Maximum history keys inspected by one pruning pass.
const PRUNE_SCAN_LIMIT: usize = 50_000;
/// Default number of revisions kept per document. `0` disables history writes.
pub const DEFAULT_HISTORY_MAX_VERSIONS: usize = 0;
/// Maximum number of revisions returned by one `history` request.
const MAX_HISTORY_PAGE: usize = 1000;

/// One immutable document revision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HistoryEntry {
    /// Global monotonic revision assigned by the writer.
    pub rev: u64,
    /// `insert`, `update`, or `delete`.
    pub op: String,
    /// Unix timestamp in milliseconds.
    pub at: u64,
    /// Document snapshot for inserts/updates. Absent for tombstones and when
    /// `include_docs=false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<Value>,
    /// True for a delete tombstone.
    #[serde(default)]
    pub deleted: bool,
}

/// Builds the key prefix shared by all revisions of one document.
fn prefix(coll: &str, id: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + coll.len() + 4 + id.len());
    out.extend_from_slice(&(coll.len() as u16).to_be_bytes());
    out.extend_from_slice(coll.as_bytes());
    out.extend_from_slice(&(id.len() as u32).to_be_bytes());
    out.extend_from_slice(id.as_bytes());
    out
}

fn key(coll: &str, id: &str, rev: u64) -> Vec<u8> {
    let mut out = prefix(coll, id);
    out.extend_from_slice(&rev.to_be_bytes());
    out
}

fn parse_key(raw: &[u8]) -> Option<(&str, &str, u64)> {
    if raw.len() < 6 {
        return None;
    }
    let coll_len = u16::from_be_bytes([raw[0], raw[1]]) as usize;
    let coll_start: usize = 2;
    let coll_end = coll_start.checked_add(coll_len)?;
    if raw.len() < coll_end + 4 {
        return None;
    }
    let id_len = u32::from_be_bytes([
        raw[coll_end],
        raw[coll_end + 1],
        raw[coll_end + 2],
        raw[coll_end + 3],
    ]) as usize;
    let id_start = coll_end + 4;
    let id_end = id_start.checked_add(id_len)?;
    if raw.len() != id_end + 8 {
        return None;
    }
    let coll = std::str::from_utf8(&raw[coll_start..coll_end]).ok()?;
    let id = std::str::from_utf8(&raw[id_start..id_end]).ok()?;
    let rev = u64::from_be_bytes(raw[id_end..id_end + 8].try_into().ok()?);
    Some((coll, id, rev))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn next_rev(meta: &Database<Bytes, Bytes>, wtxn: &mut RwTxn<'_>) -> Result<u64> {
    let current = meta
        .get(&*wtxn, HISTORY_SEQ_KEY)?
        .and_then(|raw| std::str::from_utf8(raw).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let next = current
        .checked_add(1)
        .ok_or_else(|| Error::Query("history revision counter exhausted".into()))?;
    meta.put(&mut *wtxn, HISTORY_SEQ_KEY, next.to_string().as_bytes())?;
    Ok(next)
}

/// Appends one revision. The caller must pass the same transaction used for the
/// current-document write, which makes the pair atomic.
pub(crate) fn append(
    history: &Database<Bytes, Bytes>,
    meta: &Database<Bytes, Bytes>,
    wtxn: &mut RwTxn<'_>,
    coll: &str,
    id: &str,
    op: &str,
    doc: Option<Value>,
) -> Result<u64> {
    let rev = next_rev(meta, wtxn)?;
    let entry = HistoryEntry {
        rev,
        op: op.to_string(),
        at: now_ms(),
        deleted: op == "delete",
        doc,
    };
    let bytes = serde_json::to_vec(&entry)?;
    history.put(&mut *wtxn, key(coll, id, rev).as_slice(), &bytes)?;
    Ok(rev)
}

/// Reads revisions for one document, newest first.
pub(crate) fn list(
    history: &Database<Bytes, Bytes>,
    rtxn: &RoTxn<'_>,
    coll: &str,
    id: &str,
    limit: Option<usize>,
    include_docs: bool,
) -> Result<Vec<HistoryEntry>> {
    let page = limit.unwrap_or(50).min(MAX_HISTORY_PAGE);
    if page == 0 {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for item in history.prefix_iter(rtxn, &prefix(coll, id))? {
        let (_key, raw) = item?;
        let mut entry: HistoryEntry = serde_json::from_slice(raw)?;
        if !include_docs {
            entry.doc = None;
        }
        out.push(entry);
    }
    out.reverse();
    out.truncate(page);
    Ok(out)
}

/// Reads one exact revision.
pub(crate) fn get(
    history: &Database<Bytes, Bytes>,
    rtxn: &RoTxn<'_>,
    coll: &str,
    id: &str,
    rev: u64,
) -> Result<Option<HistoryEntry>> {
    let Some(raw) = history.get(rtxn, &key(coll, id, rev))? else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_slice(raw)?))
}

/// Deletes every stored revision for one document.
///
/// Used only when a collection opts into `ttl_purge_history` for TTL deletes.
/// The scan is batched so a document with a very long history does not require
/// one huge temporary vector.
pub(crate) fn purge(
    history: &Database<Bytes, Bytes>,
    wtxn: &mut RwTxn<'_>,
    coll: &str,
    id: &str,
) -> Result<usize> {
    let prefix = prefix(coll, id);
    let mut deleted = 0usize;
    loop {
        let keys: Vec<Vec<u8>> = history
            .prefix_iter(&*wtxn, prefix.as_slice())?
            .take(1000)
            .map(|item| item.map(|(key, _)| key.to_vec()))
            .collect::<std::result::Result<_, _>>()?;
        if keys.is_empty() {
            break;
        }
        for key in &keys {
            history.delete(&mut *wtxn, key.as_slice())?;
        }
        deleted += keys.len();
    }
    Ok(deleted)
}

fn queue_oldest(group: &[Vec<u8>], max_versions: usize, limit: usize, out: &mut Vec<Vec<u8>>) {
    if group.len() <= max_versions {
        return;
    }
    let take = (group.len() - max_versions).min(limit.saturating_sub(out.len()));
    out.extend(group.iter().take(take).cloned());
}

/// Deletes the oldest revisions above `max_versions`.
///
/// This is a bounded background maintenance pass, not part of the hot write
/// path. It walks a persisted cursor so each call inspects at most
/// `PRUNE_SCAN_LIMIT` keys and deletes at most `limit` revisions.
pub(crate) fn prune(
    history: &Database<Bytes, Bytes>,
    meta: &Database<Bytes, Bytes>,
    wtxn: &mut RwTxn<'_>,
    max_versions_for: &HashMap<String, usize>,
    limit: usize,
) -> Result<usize> {
    if limit == 0 {
        return Ok(0);
    }

    let cursor = meta.get(&*wtxn, HISTORY_PRUNE_CURSOR_KEY)?.map(Vec::from);
    let start = cursor
        .as_deref()
        .map(Bound::Excluded)
        .unwrap_or(Bound::Unbounded);

    let mut deletions: Vec<Vec<u8>> = Vec::new();
    let mut group_keys: Vec<Vec<u8>> = Vec::new();
    let mut group_prefix: Option<Vec<u8>> = None;
    let mut group_coll: Option<String> = None;
    let mut committed: Option<Vec<u8>> = None;
    let mut scanned = 0usize;
    let mut stop_after_group = false;

    for item in history.range(&*wtxn, &(start, Bound::Unbounded))? {
        let (raw_key, _) = item?;
        let Some((coll, id, _rev)) = parse_key(raw_key) else {
            continue;
        };
        let p = prefix(coll, id);
        if group_prefix.as_ref() != Some(&p) {
            if !group_keys.is_empty() {
                let max_versions = group_coll
                    .as_deref()
                    .and_then(|coll| max_versions_for.get(coll))
                    .copied()
                    .unwrap_or(0);
                queue_oldest(&group_keys, max_versions, limit, &mut deletions);
                committed = group_keys.last().cloned();
                group_keys.clear();
            }
            if stop_after_group {
                break;
            }
            group_prefix = Some(p);
            group_coll = Some(coll.to_string());
        }
        group_keys.push(raw_key.to_vec());
        scanned += 1;
        if scanned >= PRUNE_SCAN_LIMIT {
            stop_after_group = true;
        }
    }

    if !group_keys.is_empty() {
        let max_versions = group_coll
            .as_deref()
            .and_then(|coll| max_versions_for.get(coll))
            .copied()
            .unwrap_or(0);
        queue_oldest(&group_keys, max_versions, limit, &mut deletions);
        committed = group_keys.last().cloned();
    }

    if let Some(key) = committed {
        meta.put(&mut *wtxn, HISTORY_PRUNE_CURSOR_KEY, key.as_slice())?;
    }
    if !stop_after_group {
        // The scan reached the end; start again from the beginning next time so
        // revisions written for earlier collections are still considered.
        meta.delete(&mut *wtxn, HISTORY_PRUNE_CURSOR_KEY)?;
    }

    let deleted = deletions.len();
    for key in deletions {
        history.delete(&mut *wtxn, key.as_slice())?;
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_roundtrip() {
        let key = key("users", "abc", 42);
        assert_eq!(parse_key(&key), Some(("users", "abc", 42)));
        assert!(parse_key(&key[..key.len() - 1]).is_none());
    }
}
