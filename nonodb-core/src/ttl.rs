//! Document TTL metadata.
//!
//! TTL timestamps live outside the document in the `ttl` sub-database. The
//! document itself only keeps user data; the reserved `_ttl` input field is
//! removed before the document is stored.
//!
//! Layout:
//!   `e <expire_ms> <coll_len> <coll> <id_len> <id>` -> empty
//!   `i <coll_len> <coll> <id_len> <id>`             -> `expire_ms`
//!
//! The `e` entries are ordered by expiry, so the sweeper can find due
//! documents with a prefix scan. The `i` entries make updates/deletes by id
//! direct lookups.

use std::time::{SystemTime, UNIX_EPOCH};

use heed::types::Bytes;
use heed::{Database, RoTxn, RwTxn};
use serde_json::Value;

use crate::error::{Error, Result};

/// Name of the TTL sub-database.
pub const TTL_DB: &str = "ttl";

const EXPIRY_PREFIX: u8 = b'e';
const ID_PREFIX: u8 = b'i';

/// Current Unix time in milliseconds.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn ttl_after(seconds: u64) -> Result<u64> {
    let millis = seconds
        .checked_mul(1000)
        .ok_or_else(|| Error::InvalidTtl("TTL is too large".into()))?;
    now_ms()
        .checked_add(millis)
        .ok_or_else(|| Error::InvalidTtl("TTL overflows the timestamp".into()))
}

fn id_key(coll: &str, id: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 2 + coll.len() + 4 + id.len());
    out.push(ID_PREFIX);
    out.extend_from_slice(&(coll.len() as u16).to_be_bytes());
    out.extend_from_slice(coll.as_bytes());
    out.extend_from_slice(&(id.len() as u32).to_be_bytes());
    out.extend_from_slice(id.as_bytes());
    out
}

fn expiry_key(at: u64, coll: &str, id: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 8 + 2 + coll.len() + 4 + id.len());
    out.push(EXPIRY_PREFIX);
    out.extend_from_slice(&at.to_be_bytes());
    out.extend_from_slice(&(coll.len() as u16).to_be_bytes());
    out.extend_from_slice(coll.as_bytes());
    out.extend_from_slice(&(id.len() as u32).to_be_bytes());
    out.extend_from_slice(id.as_bytes());
    out
}

pub(crate) fn parse_expiry_key(raw: &[u8]) -> Option<(u64, &str, &str)> {
    if raw.len() < 1 + 8 + 2 + 4 || raw[0] != EXPIRY_PREFIX {
        return None;
    }
    let at = u64::from_be_bytes(raw[1..9].try_into().ok()?);
    let coll_len = u16::from_be_bytes([raw[9], raw[10]]) as usize;
    let coll_start: usize = 11;
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
    if raw.len() != id_end {
        return None;
    }
    let coll = std::str::from_utf8(&raw[coll_start..coll_end]).ok()?;
    let id = std::str::from_utf8(&raw[id_start..id_end]).ok()?;
    Some((at, coll, id))
}

pub(crate) fn parse_id_key(raw: &[u8]) -> Option<(&str, &str)> {
    if raw.len() < 1 + 2 + 4 || raw[0] != ID_PREFIX {
        return None;
    }
    let coll_len = u16::from_be_bytes([raw[1], raw[2]]) as usize;
    let coll_start: usize = 3;
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
    if raw.len() != id_end {
        return None;
    }
    let coll = std::str::from_utf8(&raw[coll_start..coll_end]).ok()?;
    let id = std::str::from_utf8(&raw[id_start..id_end]).ok()?;
    Some((coll, id))
}

fn parse_at(raw: &[u8]) -> Option<u64> {
    if raw.len() != 8 {
        return None;
    }
    Some(u64::from_be_bytes(raw.try_into().ok()?))
}

/// TTL directive produced by an update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TtlUpdate {
    /// No `_ttl` in the update; keep the existing expiry.
    Preserve,
    /// `_ttl: null` or `_ttl: 0`; remove the expiry.
    Clear,
    /// An absolute expiry timestamp in milliseconds.
    Set(u64),
}

/// Removes a reserved `_ttl` field from an inserted document and computes its
/// absolute expiry. If the field is absent, `default_seconds` applies.
pub fn take_insert_ttl(doc: &mut Value, default_seconds: Option<u64>) -> Result<Option<u64>> {
    let Some(obj) = doc.as_object_mut() else {
        return Err(Error::Query("document must be an object".into()));
    };
    match obj.remove("_ttl") {
        None => match default_seconds {
            Some(seconds) if seconds > 0 => Ok(Some(ttl_after(seconds)?)),
            _ => Ok(None),
        },
        Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => {
            let seconds = number
                .as_u64()
                .ok_or_else(|| Error::InvalidTtl("`_ttl` must be a non-negative integer".into()))?;
            if seconds == 0 {
                Ok(None)
            } else {
                Ok(Some(ttl_after(seconds)?))
            }
        }
        Some(_) => Err(Error::InvalidTtl(
            "`_ttl` must be a non-negative integer or null".into(),
        )),
    }
}

/// Removes a reserved `_ttl` field from an updated document and returns the
/// requested TTL change.
pub fn take_update_ttl(doc: &mut Value) -> Result<TtlUpdate> {
    let Some(obj) = doc.as_object_mut() else {
        return Err(Error::Query("document must be an object".into()));
    };
    match obj.remove("_ttl") {
        None => Ok(TtlUpdate::Preserve),
        Some(Value::Null) => Ok(TtlUpdate::Clear),
        Some(Value::Number(number)) => {
            let seconds = number
                .as_u64()
                .ok_or_else(|| Error::InvalidTtl("`_ttl` must be a non-negative integer".into()))?;
            if seconds == 0 {
                Ok(TtlUpdate::Clear)
            } else {
                Ok(TtlUpdate::Set(ttl_after(seconds)?))
            }
        }
        Some(_) => Err(Error::InvalidTtl(
            "`_ttl` must be a non-negative integer or null".into(),
        )),
    }
}

/// Removes the old TTL entries for a document and optionally writes a new one.
pub(crate) fn set_entry(
    db: &Database<Bytes, Bytes>,
    wtxn: &mut RwTxn<'_>,
    coll: &str,
    id: &str,
    at: Option<u64>,
) -> Result<()> {
    remove_entry(db, wtxn, coll, id)?;
    if let Some(at) = at {
        let key = expiry_key(at, coll, id);
        db.put(&mut *wtxn, key.as_slice(), &[])?;
        db.put(
            &mut *wtxn,
            id_key(coll, id).as_slice(),
            at.to_be_bytes().as_slice(),
        )?;
    }
    Ok(())
}

/// Removes TTL entries for one document. Missing entries are ignored.
pub(crate) fn remove_entry(
    db: &Database<Bytes, Bytes>,
    wtxn: &mut RwTxn<'_>,
    coll: &str,
    id: &str,
) -> Result<()> {
    let key = id_key(coll, id);
    if let Some(raw) = db.get(&*wtxn, key.as_slice())? {
        if let Some(at) = parse_at(raw) {
            db.delete(&mut *wtxn, expiry_key(at, coll, id).as_slice())?;
        }
        db.delete(&mut *wtxn, key.as_slice())?;
    }
    Ok(())
}

/// Returns the current expiry for one document.
pub(crate) fn get_entry(
    db: &Database<Bytes, Bytes>,
    txn: &RoTxn<'_>,
    coll: &str,
    id: &str,
) -> Result<Option<u64>> {
    Ok(db.get(txn, id_key(coll, id).as_slice())?.and_then(parse_at))
}

/// One document whose TTL has elapsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueEntry {
    pub coll: String,
    pub id: String,
    pub expire_at: u64,
}

/// Reads up to `limit` TTL entries with `expire_at <= now`.
///
/// Entries are ordered by expiry, collection, and id. The caller must re-check
/// the expiry under the write transaction before deleting the document.
pub(crate) fn due_entries(
    db: &Database<Bytes, Bytes>,
    rtxn: &RoTxn<'_>,
    now: u64,
    limit: usize,
) -> Result<Vec<DueEntry>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for item in db.prefix_iter(rtxn, &[EXPIRY_PREFIX])? {
        let (key, _) = item?;
        let Some((at, coll, id)) = parse_expiry_key(key) else {
            continue;
        };
        if at > now {
            break;
        }
        out.push(DueEntry {
            coll: coll.to_string(),
            id: id.to_string(),
            expire_at: at,
        });
        if out.len() >= limit {
            break;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expiry_key_roundtrip() {
        let key = expiry_key(42, "coll", "id");
        assert_eq!(parse_expiry_key(&key), Some((42, "coll", "id")));
        assert!(parse_expiry_key(&key[..key.len() - 1]).is_none());
    }

    #[test]
    fn take_insert_ttl_removes_field() {
        let mut doc = serde_json::json!({"_id": "1", "_ttl": 1, "n": 2});
        let at = take_insert_ttl(&mut doc, None).unwrap().unwrap();
        assert!(at > now_ms());
        assert!(doc.get("_ttl").is_none());
    }

    #[test]
    fn take_insert_ttl_uses_default_and_allows_disable() {
        let mut doc = serde_json::json!({"_id": "1"});
        assert!(take_insert_ttl(&mut doc, Some(1)).unwrap().is_some());
        let mut doc = serde_json::json!({"_id": "1", "_ttl": 0});
        assert!(take_insert_ttl(&mut doc, Some(10)).unwrap().is_none());
        let mut doc = serde_json::json!({"_id": "1", "_ttl": null});
        assert!(take_insert_ttl(&mut doc, Some(10)).unwrap().is_none());
    }

    #[test]
    fn update_directive() {
        let mut doc = serde_json::json!({"_id": "1"});
        assert_eq!(take_update_ttl(&mut doc).unwrap(), TtlUpdate::Preserve);
        let mut doc = serde_json::json!({"_id": "1", "_ttl": 0});
        assert_eq!(take_update_ttl(&mut doc).unwrap(), TtlUpdate::Clear);
        let mut doc = serde_json::json!({"_id": "1", "_ttl": 1});
        assert!(matches!(
            take_update_ttl(&mut doc).unwrap(),
            TtlUpdate::Set(at) if at > now_ms()
        ));
    }
}
