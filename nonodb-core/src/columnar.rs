//! General-purpose bucketed columnar collections.
//!
//! This is the experimental `columnar` profile. Documents are appended to an
//! open bucket as JSON, then sealed into the shared block-column format from
//! [`crate::bucket_codec`]. Point access uses a compact 64-bit id hash locator
//! keyed by the canonical `_id` encoding; the full `_id` is still stored in the
//! document columns and verified on every point read.
//!
//! The profile is intentionally independent from time-series semantics: there
//! is no time field, no series grouping, no time-window pruning, and no rollup.

use std::collections::{BTreeMap, BTreeSet};

use heed::types::Bytes;
use heed::{Database, Env, RoTxn, RwTxn};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::bucket_codec::{
    decode_column, encode_column, get_bytes, get_uvarint, put_bytes, put_uvarint, Column,
    Compression, HARD_MAX_COLUMN_ROWS,
};
use crate::collection::{compare_docs, project_document, FindQuery, UpdateOutcome};
use crate::error::{Error, Result};

/// Metadata prefix for columnar settings.
pub const SETTINGS_META_PREFIX: &str = "col:";
/// Prefix of the dedicated LMDB sub-database.
pub const DB_PREFIX: &str = "colb:";

const MAGIC: &[u8; 4] = b"NCB1";
const FORMAT_VERSION: u8 = 1;
const BLOCK_ROWS: usize = 256;

const KEY_OPEN_ROW: u8 = 0x00;
const KEY_BUCKET: u8 = 0x01;
const KEY_ID: u8 = 0x02;
const KEY_POINTER: u8 = 0x03;
const KEY_OVERLAY: u8 = 0x04;
const KEY_DELETED: u8 = 0x05;

const DEFAULT_BUCKET_MAX_DOCS: usize = 512;
const DEFAULT_BUCKET_MAX_BYTES: usize = 512 * 1024;
const HARD_BUCKET_MAX_BYTES: usize = 4 * 1024 * 1024;
const POINTER_LEN: usize = 16;

/// Columnar collection settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Settings {
    #[serde(default = "default_bucket_max_docs", alias = "bucketMaxDocs")]
    pub bucket_max_docs: usize,
    #[serde(default = "default_bucket_max_bytes", alias = "bucketMaxBytes")]
    pub bucket_max_bytes: usize,
    #[serde(default)]
    pub compression: Compression,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            bucket_max_docs: DEFAULT_BUCKET_MAX_DOCS,
            bucket_max_bytes: DEFAULT_BUCKET_MAX_BYTES,
            compression: Compression::default(),
        }
    }
}

impl Settings {
    pub fn validate(&self) -> Result<()> {
        if self.bucket_max_docs == 0 || self.bucket_max_docs > HARD_MAX_COLUMN_ROWS {
            return Err(Error::Query(format!(
                "columnar `bucketMaxDocs` must be between 1 and {HARD_MAX_COLUMN_ROWS}"
            )));
        }
        if self.bucket_max_bytes == 0 || self.bucket_max_bytes > HARD_BUCKET_MAX_BYTES {
            return Err(Error::Query(format!(
                "columnar `bucketMaxBytes` must be between 1 and {HARD_BUCKET_MAX_BYTES}"
            )));
        }
        Ok(())
    }
}

fn default_bucket_max_docs() -> usize {
    DEFAULT_BUCKET_MAX_DOCS
}

fn default_bucket_max_bytes() -> usize {
    DEFAULT_BUCKET_MAX_BYTES
}

pub fn parse_settings(raw: &[u8]) -> Result<Settings> {
    let settings: Settings = serde_json::from_slice(raw)?;
    settings.validate()?;
    Ok(settings)
}

/// Storage statistics for a columnar collection.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Stats {
    pub rows: u64,
    pub buckets: u64,
    pub open_buckets: u64,
    pub sealed_buckets: u64,
    pub id_entries: u64,
    pub data_bytes: u64,
}

type Locator = (u64, u32);
type OverlayMap = BTreeMap<u64, Vec<(u32, Map<String, Value>)>>;

fn db_name(name: &str) -> String {
    format!("{DB_PREFIX}{name}")
}

fn corrupt(what: &str) -> Error {
    Error::Query(format!("corrupt columnar bucket: {what}"))
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn bucket_key(id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(KEY_BUCKET);
    key.extend_from_slice(&id.to_be_bytes());
    key
}

fn open_row_key(id: u64, row: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(13);
    key.push(KEY_OPEN_ROW);
    key.extend_from_slice(&id.to_be_bytes());
    key.extend_from_slice(&row.to_be_bytes());
    key
}

fn id_key(hash: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(KEY_ID);
    key.extend_from_slice(&hash.to_be_bytes());
    key
}

fn pointer_key() -> [u8; 1] {
    [KEY_POINTER]
}

fn overlay_key(bucket: u64, row: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(13);
    key.push(KEY_OVERLAY);
    key.extend_from_slice(&bucket.to_be_bytes());
    key.extend_from_slice(&row.to_be_bytes());
    key
}

fn deleted_key(bucket: u64, row: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(13);
    key.push(KEY_DELETED);
    key.extend_from_slice(&bucket.to_be_bytes());
    key.extend_from_slice(&row.to_be_bytes());
    key
}

fn key_locator(key: &[u8]) -> Option<Locator> {
    if key.len() != 13 {
        return None;
    }
    Some((
        u64::from_be_bytes(key[1..9].try_into().ok()?),
        u32::from_be_bytes(key[9..13].try_into().ok()?),
    ))
}

fn encode_pointer(id: u64, count: u32, bytes: u32) -> [u8; POINTER_LEN] {
    let mut out = [0u8; POINTER_LEN];
    out[..8].copy_from_slice(&id.to_be_bytes());
    out[8..12].copy_from_slice(&count.to_be_bytes());
    out[12..].copy_from_slice(&bytes.to_be_bytes());
    out
}

fn decode_pointer(raw: &[u8]) -> Option<(u64, u32, u32)> {
    if raw.len() != POINTER_LEN {
        return None;
    }
    Some((
        u64::from_be_bytes(raw[..8].try_into().ok()?),
        u32::from_be_bytes(raw[8..12].try_into().ok()?),
        u32::from_be_bytes(raw[12..].try_into().ok()?),
    ))
}

fn path_key(parts: &[String]) -> String {
    serde_json::to_string(parts).expect("string path")
}

fn flatten(prefix: &mut Vec<String>, value: &Value, out: &mut BTreeMap<String, Value>) {
    match value {
        Value::Object(map) if !map.is_empty() => {
            for (key, child) in map {
                prefix.push(key.clone());
                flatten(prefix, child, out);
                prefix.pop();
            }
        }
        other => {
            out.insert(path_key(prefix), other.clone());
        }
    }
}

fn set_path(doc: &mut Map<String, Value>, path: &str, value: Value) -> Result<()> {
    let parts: Vec<String> = serde_json::from_str(path)?;
    if parts.is_empty() {
        return Err(corrupt("empty document path"));
    }
    let mut current = doc;
    for part in &parts[..parts.len() - 1] {
        let entry = current
            .entry(part.clone())
            .or_insert_with(|| Value::Object(Map::new()));
        if !entry.is_object() {
            *entry = Value::Object(Map::new());
        }
        current = entry.as_object_mut().expect("object inserted");
    }
    current.insert(parts[parts.len() - 1].clone(), value);
    Ok(())
}

fn flatten_rows(rows: &[Map<String, Value>]) -> Vec<BTreeMap<String, Value>> {
    rows.iter()
        .map(|row| {
            let mut flat = BTreeMap::new();
            for (key, value) in row {
                let mut prefix = vec![key.clone()];
                flatten(&mut prefix, value, &mut flat);
            }
            flat
        })
        .collect()
}

fn encode_bucket(rows: &[Map<String, Value>], settings: &Settings) -> Result<Vec<u8>> {
    let flattened = flatten_rows(rows);
    let mut names = BTreeSet::new();
    for row in &flattened {
        names.extend(row.keys().cloned());
    }

    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(MAGIC);
    out.push(FORMAT_VERSION);
    put_uvarint(&mut out, rows.len() as u64);
    put_uvarint(&mut out, names.len() as u64);
    for name in names {
        put_bytes(&mut out, name.as_bytes());
        let values: Vec<Option<Value>> = flattened
            .iter()
            .map(|row| row.get(&name).cloned())
            .collect();
        let blocks = values.len().div_ceil(BLOCK_ROWS).max(1);
        put_uvarint(&mut out, blocks as u64);
        for chunk in values.chunks(BLOCK_ROWS) {
            let column = Column {
                field: name.clone(),
                values: chunk.to_vec(),
            };
            put_bytes(&mut out, &encode_column(&column, settings.compression)?);
        }
    }
    Ok(out)
}

fn decode_bucket(data: &[u8]) -> Result<Vec<Map<String, Value>>> {
    if data.len() < 6 || &data[..4] != MAGIC || data[4] != FORMAT_VERSION {
        return Err(corrupt("bad header"));
    }
    let mut pos = 5;
    let rows = get_uvarint(data, &mut pos)? as usize;
    if rows > HARD_MAX_COLUMN_ROWS {
        return Err(corrupt("row count out of range"));
    }
    let columns = get_uvarint(data, &mut pos)? as usize;
    let mut out = vec![Map::new(); rows];
    for _ in 0..columns {
        let name = std::str::from_utf8(get_bytes(data, &mut pos)?)
            .map_err(|_| corrupt("field name is not UTF-8"))?
            .to_string();
        let blocks = get_uvarint(data, &mut pos)? as usize;
        if blocks > HARD_MAX_COLUMN_ROWS {
            return Err(corrupt("block count out of range"));
        }
        let mut values: Vec<Option<Value>> = Vec::with_capacity(rows);
        for _ in 0..blocks {
            let blob = get_bytes(data, &mut pos)?;
            let mut block_pos = 0;
            let column = decode_column(blob, &mut block_pos)?;
            if column.field != name {
                return Err(corrupt("column field mismatch"));
            }
            values.extend(column.values);
        }
        values.resize(rows, None);
        for (row, value) in out.iter_mut().zip(values) {
            if let Some(value) = value {
                set_path(row, &name, value)?;
            }
        }
    }
    Ok(out)
}

fn read_pointer_rw(
    txn: &RwTxn<'_>,
    db: &Database<Bytes, Bytes>,
) -> Result<Option<(u64, u32, u32)>> {
    Ok(db.get(txn, &pointer_key())?.and_then(decode_pointer))
}

fn read_locators(txn: &RoTxn<'_>, db: &Database<Bytes, Bytes>, hash: u64) -> Result<Vec<Locator>> {
    let Some(raw) = db.get(txn, &id_key(hash))? else {
        return Ok(Vec::new());
    };
    if raw.len() < 4 {
        return Err(corrupt("malformed id locator"));
    }
    let count = u32::from_be_bytes(raw[..4].try_into().expect("4 bytes")) as usize;
    if raw.len() != 4 + count * 12 {
        return Err(corrupt("id locator length mismatch"));
    }
    let mut out = Vec::with_capacity(count);
    for chunk in raw[4..].as_chunks::<12>().0 {
        out.push((
            u64::from_be_bytes(chunk[..8].try_into().expect("8 bytes")),
            u32::from_be_bytes(chunk[8..].try_into().expect("4 bytes")),
        ));
    }
    Ok(out)
}

fn encode_locators(locators: &[Locator]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + locators.len() * 12);
    out.extend_from_slice(&(locators.len() as u32).to_be_bytes());
    for (bucket, row) in locators {
        out.extend_from_slice(&bucket.to_be_bytes());
        out.extend_from_slice(&row.to_be_bytes());
    }
    out
}

fn read_doc_at(
    txn: &RoTxn<'_>,
    db: &Database<Bytes, Bytes>,
    locator: Locator,
) -> Result<Option<Map<String, Value>>> {
    let (bucket, row) = locator;
    if let Some(raw) = db.get(txn, &overlay_key(bucket, row))? {
        return Ok(Some(serde_json::from_slice(raw)?));
    }
    if db.get(txn, &deleted_key(bucket, row))?.is_some() {
        return Ok(None);
    }
    if let Some(raw) = db.get(txn, &open_row_key(bucket, row))? {
        return Ok(Some(serde_json::from_slice(raw)?));
    }
    let Some(raw) = db.get(txn, &bucket_key(bucket))? else {
        return Ok(None);
    };
    Ok(decode_bucket(raw)?.into_iter().nth(row as usize))
}

fn write_doc_at(
    wtxn: &mut RwTxn<'_>,
    db: &Database<Bytes, Bytes>,
    locator: Locator,
    doc: &Map<String, Value>,
) -> Result<()> {
    let (bucket, row) = locator;
    let encoded = serde_json::to_vec(doc)?;
    if db.get(&*wtxn, &open_row_key(bucket, row))?.is_some() {
        db.put(&mut *wtxn, &open_row_key(bucket, row), &encoded)?;
    } else {
        db.put(&mut *wtxn, &overlay_key(bucket, row), &encoded)?;
        db.delete(&mut *wtxn, &deleted_key(bucket, row))?;
    }
    Ok(())
}

fn delete_doc_at(
    wtxn: &mut RwTxn<'_>,
    db: &Database<Bytes, Bytes>,
    locator: Locator,
) -> Result<()> {
    let (bucket, row) = locator;
    if db.get(&*wtxn, &open_row_key(bucket, row))?.is_some() {
        db.delete(&mut *wtxn, &open_row_key(bucket, row))?;
    } else {
        db.delete(&mut *wtxn, &overlay_key(bucket, row))?;
        db.put(&mut *wtxn, &deleted_key(bucket, row), b"")?;
    }
    Ok(())
}

fn remove_locator(
    wtxn: &mut RwTxn<'_>,
    db: &Database<Bytes, Bytes>,
    hash: u64,
    locator: Locator,
) -> Result<()> {
    let mut locators = read_locators(&*wtxn, db, hash)?;
    locators.retain(|item| *item != locator);
    if locators.is_empty() {
        db.delete(&mut *wtxn, &id_key(hash))?;
    } else {
        db.put(&mut *wtxn, &id_key(hash), &encode_locators(&locators))?;
    }
    Ok(())
}

fn all_located(
    txn: &RoTxn<'_>,
    env: &Env,
    name: &str,
) -> Result<Vec<(Locator, Map<String, Value>)>> {
    let Some(db) = env.open_database::<Bytes, Bytes>(txn, Some(&db_name(name)))? else {
        return Ok(Vec::new());
    };
    let mut docs: BTreeMap<Locator, Map<String, Value>> = BTreeMap::new();
    for item in db.iter(txn)? {
        let (key, value) = item?;
        match key.first() {
            Some(&KEY_OPEN_ROW) => {
                if let Some(locator) = key_locator(key) {
                    docs.insert(locator, serde_json::from_slice(value)?);
                }
            }
            Some(&KEY_BUCKET) => {
                if key.len() != 9 {
                    return Err(corrupt("malformed bucket key"));
                }
                let bucket = u64::from_be_bytes(key[1..9].try_into().expect("8 bytes"));
                for (row, doc) in decode_bucket(value)?.into_iter().enumerate() {
                    docs.insert((bucket, row as u32), doc);
                }
            }
            _ => {}
        }
    }
    for item in db.iter(txn)? {
        let (key, value) = item?;
        match key.first() {
            Some(&KEY_OVERLAY) => {
                if let Some(locator) = key_locator(key) {
                    docs.insert(locator, serde_json::from_slice(value)?);
                }
            }
            Some(&KEY_DELETED) => {
                if let Some(locator) = key_locator(key) {
                    docs.remove(&locator);
                }
            }
            _ => {}
        }
    }
    Ok(docs.into_iter().collect())
}

fn doc_id(doc: &Map<String, Value>) -> Result<String> {
    let value = doc.get("_id").ok_or(Error::InvalidId)?;
    if let Some(raw) = value.as_str() {
        crate::id::validate_string_key(raw)?;
    }
    crate::id::key_from_value(value).ok_or(Error::InvalidId)
}

/// Returns the canonical key when the filter is exactly `{"_id": ...}`.
pub fn exact_id_filter(filter: &Value) -> Option<String> {
    let object = filter.as_object()?;
    if object.len() != 1 {
        return None;
    }
    crate::id::key_from_value(object.get("_id")?)
}

fn find_located_id(
    txn: &RoTxn<'_>,
    db: &Database<Bytes, Bytes>,
    id: &str,
) -> Result<Option<(Locator, Map<String, Value>)>> {
    for locator in read_locators(txn, db, fnv1a64(id.as_bytes()))? {
        let Some(row) = read_doc_at(txn, db, locator)? else {
            continue;
        };
        if doc_id(&row)? == id {
            return Ok(Some((locator, row)));
        }
    }
    Ok(None)
}

/// Inserts one document and returns its `_id`.
pub fn insert(
    wtxn: &mut RwTxn<'_>,
    env: &Env,
    name: &str,
    settings: &Settings,
    mut doc: Value,
) -> Result<String> {
    settings.validate()?;
    let object = doc
        .as_object_mut()
        .ok_or_else(|| Error::Query("document must be an object".into()))?;
    let id = match object.get("_id") {
        Some(value) => {
            if let Some(raw) = value.as_str() {
                crate::id::validate_string_key(raw)?;
            }
            crate::id::key_from_value(value).ok_or(Error::InvalidId)?
        }
        None => {
            let id = uuid::Uuid::new_v4().to_string();
            object.insert("_id".into(), Value::String(id.clone()));
            id
        }
    };
    let hash = fnv1a64(id.as_bytes());
    let db = env.create_database::<Bytes, Bytes>(wtxn, Some(&db_name(name)))?;
    let existing = read_locators(&*wtxn, &db, hash)?;
    for locator in existing.iter().copied() {
        if let Some(row) = read_doc_at(&*wtxn, &db, locator)? {
            if doc_id(&row)? == id {
                return Err(Error::DuplicateKey(id));
            }
        }
    }

    let encoded = serde_json::to_vec(&doc)?;
    if encoded.len() > crate::store::MAX_DOCUMENT_BYTES {
        return Err(Error::DocumentTooLarge(
            encoded.len(),
            crate::store::MAX_DOCUMENT_BYTES,
        ));
    }
    let mut pointer = read_pointer_rw(&*wtxn, &db)?.unwrap_or((1, 0, 0));
    if pointer.1 >= settings.bucket_max_docs as u32
        || pointer.2 as usize >= settings.bucket_max_bytes
    {
        seal_current(wtxn, env, name, settings, &db)?;
        pointer = read_pointer_rw(&*wtxn, &db)?.unwrap_or((pointer.0 + 1, 0, 0));
    }
    let row = pointer.1;
    db.put(&mut *wtxn, &open_row_key(pointer.0, row), &encoded)?;
    db.put(
        &mut *wtxn,
        &pointer_key(),
        &encode_pointer(
            pointer.0,
            row + 1,
            pointer.2.saturating_add(encoded.len() as u32),
        ),
    )?;
    let mut locators = existing;
    locators.push((pointer.0, row));
    db.put(&mut *wtxn, &id_key(hash), &encode_locators(&locators))?;

    if row as usize + 1 >= settings.bucket_max_docs
        || pointer.2 as usize + encoded.len() >= settings.bucket_max_bytes
    {
        seal_current(wtxn, env, name, settings, &db)?;
    }
    Ok(id)
}

fn seal_current(
    wtxn: &mut RwTxn<'_>,
    _env: &Env,
    _name: &str,
    settings: &Settings,
    db: &Database<Bytes, Bytes>,
) -> Result<()> {
    let Some((bucket, count, _bytes)) = read_pointer_rw(&*wtxn, db)? else {
        return Ok(());
    };
    if count == 0 {
        return Ok(());
    }
    let mut rows = Vec::with_capacity(count as usize);
    for row in 0..count {
        let key = open_row_key(bucket, row);
        let raw = db
            .get(&*wtxn, &key)?
            .ok_or_else(|| corrupt("missing open row"))?;
        rows.push(serde_json::from_slice(raw)?);
    }
    let payload = encode_bucket(&rows, settings)?;
    db.put(&mut *wtxn, &bucket_key(bucket), &payload)?;
    for row in 0..count {
        db.delete(&mut *wtxn, &open_row_key(bucket, row))?;
    }
    db.put(
        &mut *wtxn,
        &pointer_key(),
        &encode_pointer(bucket + 1, 0, 0),
    )?;
    Ok(())
}

/// Seals the current open bucket. Returns the number of sealed rows.
pub fn seal_all(wtxn: &mut RwTxn<'_>, env: &Env, name: &str, settings: &Settings) -> Result<usize> {
    let Some(db) = env.open_database::<Bytes, Bytes>(wtxn, Some(&db_name(name)))? else {
        return Ok(0);
    };
    let count = read_pointer_rw(&*wtxn, &db)?.map_or(0, |(_, count, _)| count) as usize;
    seal_current(wtxn, env, name, settings, &db)?;
    compact_overlays(wtxn, env, name, settings)?;
    Ok(count)
}

/// Reads one document by `_id` through the compact hash locator.
pub fn find_by_id(rtxn: &RoTxn<'_>, env: &Env, name: &str, id: &str) -> Result<Option<Value>> {
    let Some(db) = env.open_database::<Bytes, Bytes>(rtxn, Some(&db_name(name)))? else {
        return Ok(None);
    };
    for locator in read_locators(rtxn, &db, fnv1a64(id.as_bytes()))? {
        let Some(row) = read_doc_at(rtxn, &db, locator)? else {
            continue;
        };
        if doc_id(&row)? == id {
            return Ok(Some(Value::Object(row)));
        }
    }
    Ok(None)
}

/// Replaces a stored document without changing its locator or `_id`.
pub fn replace_by_id(
    wtxn: &mut RwTxn<'_>,
    env: &Env,
    name: &str,
    id: &str,
    doc: &Map<String, Value>,
) -> Result<()> {
    let db = env.create_database::<Bytes, Bytes>(wtxn, Some(&db_name(name)))?;
    let Some((locator, _)) = find_located_id(&*wtxn, &db, id)? else {
        return Err(Error::Query(format!("document `{id}` disappeared")));
    };
    if doc_id(doc)? != id {
        return Err(Error::Query("columnar update cannot change `_id`".into()));
    }
    write_doc_at(wtxn, &db, locator, doc)
}

/// Scans every visible document, applying update overlays and tombstones.
pub fn all_docs(rtxn: &RoTxn<'_>, env: &Env, name: &str) -> Result<Vec<Value>> {
    Ok(all_located(rtxn, env, name)?
        .into_iter()
        .map(|(_, doc)| Value::Object(doc))
        .collect())
}

/// Runs the normal find query shape over a columnar collection.
pub fn find(rtxn: &RoTxn<'_>, env: &Env, name: &str, query: &FindQuery) -> Result<Vec<Value>> {
    find_visible(rtxn, env, name, query, &mut |_| Ok(true))
}

/// Runs a find query with an additional visibility predicate (TTL reads).
pub fn find_visible(
    rtxn: &RoTxn<'_>,
    env: &Env,
    name: &str,
    query: &FindQuery,
    visible: &mut dyn FnMut(&Value) -> Result<bool>,
) -> Result<Vec<Value>> {
    let mut docs = all_docs(rtxn, env, name)?;
    let mut kept = Vec::with_capacity(docs.len());
    for doc in docs.drain(..) {
        if !crate::query::matcher::matches(&doc, &query.filter) {
            continue;
        }
        if !visible(&doc)? {
            continue;
        }
        kept.push(doc);
    }
    docs = kept;
    if !query.sort.is_empty() {
        docs.sort_by(|a, b| compare_docs(a, b, &query.sort));
    }
    let limit = query.limit.unwrap_or(usize::MAX);
    Ok(docs
        .into_iter()
        .skip(query.skip)
        .take(limit)
        .map(|doc| project_document(&doc, query.projection.as_ref()))
        .collect())
}

/// Updates matching documents. Returns the standard update outcome.
#[allow(clippy::too_many_arguments)]
pub fn update(
    wtxn: &mut RwTxn<'_>,
    env: &Env,
    name: &str,
    settings: &Settings,
    filter: &Value,
    update: &Value,
    multi: bool,
    array_filters: &[Value],
    upsert: bool,
    default_id: Option<&Value>,
) -> Result<UpdateOutcome> {
    use crate::query::updater::{apply_upsert_with_options, apply_with_options, seed_from_filter};

    let db = env.create_database::<Bytes, Bytes>(wtxn, Some(&db_name(name)))?;
    let direct = if !multi {
        match exact_id_filter(filter) {
            Some(id) => find_located_id(&*wtxn, &db, &id)?,
            None => None,
        }
    } else {
        None
    };
    let candidates = match direct {
        Some(found) => vec![found],
        None => all_located(&*wtxn, env, name)?,
    };
    let mut changes = Vec::new();
    let mut matched = 0usize;
    for (locator, row) in candidates {
        let old = Value::Object(row.clone());
        if !crate::query::matcher::matches(&old, filter) {
            continue;
        }
        matched += 1;
        let mut next = old.clone();
        apply_with_options(&mut next, update, filter, array_filters)?;
        normalize_document(&mut next)?;
        let old_id = doc_id(&row)?;
        let next_object = next
            .as_object()
            .ok_or_else(|| Error::Query("updated document must be an object".into()))?;
        let next_id = doc_id(next_object)?;
        if old_id != next_id {
            return Err(Error::Query("columnar update cannot change `_id`".into()));
        }
        let bytes = serde_json::to_vec(&next)?;
        if bytes.len() > crate::store::MAX_DOCUMENT_BYTES {
            return Err(Error::DocumentTooLarge(
                bytes.len(),
                crate::store::MAX_DOCUMENT_BYTES,
            ));
        }
        changes.push((locator, next_object.clone()));
        if !multi {
            break;
        }
    }
    for (locator, doc) in &changes {
        write_doc_at(wtxn, &db, *locator, doc)?;
    }
    if matched == 0 && upsert {
        let mut doc = seed_from_filter(filter);
        let filter_id = doc.get("_id").cloned();
        apply_upsert_with_options(&mut doc, update, filter, array_filters)?;
        if let Some(id) = filter_id {
            doc["_id"] = id;
        }
        if doc.get("_id").is_none() {
            if let Some(id) = default_id {
                doc["_id"] = id.clone();
            }
        }
        normalize_document(&mut doc)?;
        let id = insert(wtxn, env, name, settings, doc)?;
        return Ok(UpdateOutcome {
            matched: 0,
            modified: 0,
            upserted_id: Some(id),
            moved: Vec::new(),
        });
    }
    Ok(UpdateOutcome {
        matched,
        modified: changes.len(),
        upserted_id: None,
        moved: Vec::new(),
    })
}

/// Deletes matching documents.
pub fn delete(
    wtxn: &mut RwTxn<'_>,
    env: &Env,
    name: &str,
    filter: &Value,
    multi: bool,
) -> Result<usize> {
    let db = env.create_database::<Bytes, Bytes>(wtxn, Some(&db_name(name)))?;
    let direct = if !multi {
        match exact_id_filter(filter) {
            Some(id) => find_located_id(&*wtxn, &db, &id)?,
            None => None,
        }
    } else {
        None
    };
    let candidates = match direct {
        Some(found) => vec![found],
        None => all_located(&*wtxn, env, name)?,
    };
    let mut removed = 0usize;
    for (locator, row) in candidates {
        let doc = Value::Object(row.clone());
        if !crate::query::matcher::matches(&doc, filter) {
            continue;
        }
        let id = doc_id(&row)?;
        delete_doc_at(wtxn, &db, locator)?;
        remove_locator(wtxn, &db, fnv1a64(id.as_bytes()), locator)?;
        removed += 1;
        if !multi {
            break;
        }
    }
    Ok(removed)
}

fn normalize_document(doc: &mut Value) -> Result<()> {
    crate::date::normalize_document(doc)?;
    crate::numeric::normalize_document(doc)?;
    crate::bson_types::normalize_document(doc)?;
    Ok(())
}

/// Merges update overlays into their sealed buckets. Delete markers remain
/// until the bucket is physically compacted, preserving row locators.
pub fn compact_overlays(
    wtxn: &mut RwTxn<'_>,
    env: &Env,
    name: &str,
    settings: &Settings,
) -> Result<usize> {
    let Some(db) = env.open_database::<Bytes, Bytes>(wtxn, Some(&db_name(name)))? else {
        return Ok(0);
    };
    let mut overlays: OverlayMap = BTreeMap::new();
    for item in db.iter(&*wtxn)? {
        let (key, value) = item?;
        if key.first() != Some(&KEY_OVERLAY) {
            continue;
        }
        let Some((bucket, row)) = key_locator(key) else {
            return Err(corrupt("malformed overlay key"));
        };
        overlays
            .entry(bucket)
            .or_default()
            .push((row, serde_json::from_slice(value)?));
    }
    let merged = overlays.values().map(Vec::len).sum();
    for (bucket, docs) in overlays {
        let Some(raw) = db.get(&*wtxn, &bucket_key(bucket))? else {
            return Err(corrupt("overlay points at a missing bucket"));
        };
        let mut rows = decode_bucket(raw)?;
        let mut keys = Vec::new();
        for (row, doc) in docs {
            let slot = rows
                .get_mut(row as usize)
                .ok_or_else(|| corrupt("overlay row out of range"))?;
            *slot = doc;
            keys.push(overlay_key(bucket, row));
        }
        let payload = encode_bucket(&rows, settings)?;
        db.put(&mut *wtxn, &bucket_key(bucket), &payload)?;
        for key in keys {
            db.delete(&mut *wtxn, &key)?;
        }
    }
    Ok(merged)
}

/// Counts documents matching a filter. The filter uses the normal matcher.
pub fn count(rtxn: &RoTxn<'_>, env: &Env, name: &str, filter: &Value) -> Result<usize> {
    let mut count = 0;
    for doc in all_docs(rtxn, env, name)? {
        if crate::query::matcher::matches(&doc, filter) {
            count += 1;
        }
    }
    Ok(count)
}

/// Returns storage statistics.
pub fn stats(rtxn: &RoTxn<'_>, env: &Env, name: &str) -> Result<Stats> {
    let Some(db) = env.open_database::<Bytes, Bytes>(rtxn, Some(&db_name(name)))? else {
        return Ok(Stats::default());
    };
    let mut stats = Stats::default();
    for item in db.iter(rtxn)? {
        let (key, value) = item?;
        match key.first() {
            Some(&KEY_OPEN_ROW) => {
                stats.rows += 1;
                stats.data_bytes += value.len() as u64;
            }
            Some(&KEY_BUCKET) => {
                stats.sealed_buckets += 1;
                stats.data_bytes += value.len() as u64;
                stats.rows += decode_bucket(value)?.len() as u64;
            }
            Some(&KEY_POINTER) => {
                if let Some((_, count, _)) = decode_pointer(value) {
                    if count > 0 {
                        stats.open_buckets += 1;
                    }
                }
            }
            Some(&KEY_ID) => stats.id_entries += 1,
            _ => {}
        }
    }
    stats.buckets = stats.open_buckets + stats.sealed_buckets;
    stats.rows = all_docs(rtxn, env, name)?.len() as u64;
    Ok(stats)
}

/// Rewrites only visible documents into fresh sealed buckets.
///
/// This removes tombstones, update overlays, and unused bucket rows without
/// changing `_id`s or index entries.
pub fn compact(wtxn: &mut RwTxn<'_>, env: &Env, name: &str, settings: &Settings) -> Result<usize> {
    let docs = all_docs(&*wtxn, env, name)?;
    let rows = docs.len();
    clear(wtxn, env, name)?;
    for doc in docs {
        insert(wtxn, env, name, settings, doc)?;
    }
    seal_all(wtxn, env, name, settings)?;
    Ok(rows)
}

/// Removes all keys owned by a columnar collection.
pub fn clear(wtxn: &mut RwTxn<'_>, env: &Env, name: &str) -> Result<()> {
    if let Some(db) = env.open_database::<Bytes, Bytes>(wtxn, Some(&db_name(name)))? {
        db.clear(&mut *wtxn)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::FindAndModifyOptions;
    use crate::store::Store;
    use serde_json::json;

    #[test]
    fn open_and_sealed_buckets_roundtrip_documents() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let settings = Settings {
            bucket_max_docs: 2,
            ..Default::default()
        };
        let coll = store.create_columnar_collection("docs", &settings).unwrap();
        assert_eq!(coll.profile(), crate::vault::CollectionProfile::Columnar);
        assert!(store
            .collections()
            .unwrap()
            .iter()
            .any(|name| name == "docs"));
        for index in 0..5 {
            coll.insert_one(json!({
                "_id": format!("doc-{index}"),
                "nested": {"value": index, "flags": [true, null, "x"]},
                "counter": index,
                "tag": if index % 2 == 0 { "even" } else { "odd" },
            }))
            .unwrap();
        }
        for index in 0..5 {
            let found = coll.find_by_key(&format!("doc-{index}")).unwrap().unwrap();
            assert_eq!(found["_id"], format!("doc-{index}"));
            assert_eq!(found["nested"]["value"], index);
            assert_eq!(found["counter"], index);
        }
        assert_eq!(coll.count(&json!({})).unwrap(), 5);
        assert_eq!(coll.count(&json!({"tag": "even"})).unwrap(), 3);
        let stats = coll.columnar_stats().unwrap().unwrap();
        assert_eq!(stats.rows, 5);
        assert_eq!(stats.sealed_buckets, 2);
        store.seal_columnar("docs").unwrap();
        assert_eq!(coll.count(&json!({})).unwrap(), 5);
        let sealed_stats = coll.columnar_stats().unwrap().unwrap();
        assert_eq!(sealed_stats.open_buckets, 0);
        assert_eq!(sealed_stats.rows, 5);
    }

    #[test]
    fn empty_collection_and_drop_are_clean() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let coll = store
            .create_columnar_collection("empty", &Settings::default())
            .unwrap();
        assert_eq!(coll.count(&json!({})).unwrap(), 0);
        assert_eq!(store.seal_columnar("empty").unwrap(), 0);
        assert_eq!(coll.columnar_stats().unwrap().unwrap().rows, 0);
        assert!(store.drop_collection("empty").unwrap());
        assert!(store.collection_profile("empty").unwrap().is_none());
    }

    #[test]
    fn update_delete_and_find_modify_work_on_open_and_sealed_buckets() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let settings = Settings {
            bucket_max_docs: 2,
            ..Default::default()
        };
        let coll = store.create_columnar_collection("docs", &settings).unwrap();
        for index in 0..5 {
            coll.insert_one(json!({
                "_id": format!("doc-{index}"),
                "group": if index % 2 == 0 { "even" } else { "odd" },
                "value": index,
            }))
            .unwrap();
        }
        assert_eq!(
            coll.find(&FindQuery {
                filter: json!({"group": "even"}),
                sort: vec![("value".into(), -1)],
                ..Default::default()
            })
            .unwrap()
            .len(),
            3
        );
        assert_eq!(
            coll.update(
                &json!({"group": "even"}),
                &json!({"$set": {"value": 100}}),
                true
            )
            .unwrap(),
            3
        );
        assert_eq!(
            coll.find(&FindQuery {
                filter: json!({"value": 100}),
                ..Default::default()
            })
            .unwrap()
            .len(),
            3
        );
        assert_eq!(
            coll.update(
                &json!({"_id": "doc-0"}),
                &json!({"$inc": {"value": 1}}),
                false
            )
            .unwrap(),
            1
        );
        let mut upsert = FindAndModifyOptions {
            filter: &json!({"_id": "upserted"}),
            update: Some(&json!({"$set": {"value": 9}})),
            sort: &[],
            new: true,
            upsert: true,
            remove: false,
            array_filters: &[],
            projection: None,
        };
        let outcome = coll.find_and_modify(&upsert).unwrap();
        assert_eq!(outcome.upserted_id.as_deref(), Some("upserted"));
        assert_eq!(outcome.value.as_ref().unwrap()["value"], 9);
        upsert.new = false;
        let removed = {
            let mut opts = upsert;
            opts.remove = true;
            opts.upsert = false;
            opts.update = None;
            coll.find_and_modify(&opts).unwrap()
        };
        assert_eq!(removed.matched, 1);
        assert_eq!(coll.find(&FindQuery::default()).unwrap().len(), 5);
        assert_eq!(coll.delete(&json!({"group": "even"}), true).unwrap(), 3);
        assert_eq!(coll.find(&FindQuery::default()).unwrap().len(), 2);
        store.seal_columnar("docs").unwrap();
        assert_eq!(coll.find(&FindQuery::default()).unwrap().len(), 2);
    }

    #[test]
    fn duplicate_id_is_rejected_without_writing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let coll = store
            .create_columnar_collection("docs", &Settings::default())
            .unwrap();
        coll.insert_one(json!({"_id": "same", "value": 1})).unwrap();
        let error = coll
            .insert_one(json!({"_id": "same", "value": 2}))
            .unwrap_err();
        assert!(error.to_string().contains("duplicate"));
        assert_eq!(coll.count(&json!({})).unwrap(), 1);
    }

    #[test]
    fn invalid_settings_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let settings = Settings {
            bucket_max_docs: 0,
            ..Default::default()
        };
        assert!(store.create_columnar_collection("docs", &settings).is_err());
        assert!(store.collection_profile("docs").unwrap().is_none());
    }

    #[test]
    fn profile_cannot_be_converted_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store
            .create_columnar_collection("docs", &Settings::default())
            .unwrap();
        let error = match store.create_collection("docs", crate::vault::CollectionProfile::Standard)
        {
            Ok(_) => panic!("profile conversion unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("already exists"));
    }

    #[test]
    fn id_hash_collision_chain_keeps_both_docs() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let coll = store
            .create_columnar_collection("docs", &Settings::default())
            .unwrap();
        coll.insert_one(json!({"_id": "a", "value": 1})).unwrap();
        // Force a collision by inserting a second locator for a different id
        // directly through the public write transaction.
        let mut wtxn = store.write_txn().unwrap();
        let db = store
            .env()
            .create_database::<Bytes, Bytes>(&mut wtxn, Some(&db_name("docs")))
            .unwrap();
        let encoded = serde_json::to_vec(&json!({"_id": "b", "value": 2})).unwrap();
        db.put(&mut wtxn, &open_row_key(1, 1), &encoded).unwrap();
        let hash = fnv1a64(b"b");
        let mut locators = read_locators(&wtxn, &db, hash).unwrap();
        locators.push((1, 1));
        db.put(&mut wtxn, &id_key(hash), &encode_locators(&locators))
            .unwrap();
        db.put(&mut wtxn, &pointer_key(), &encode_pointer(1, 2, 0))
            .unwrap();
        wtxn.commit().unwrap();
        assert_eq!(coll.find_by_key("b").unwrap().unwrap()["value"], 2);
        assert_eq!(coll.find_by_key("a").unwrap().unwrap()["value"], 1);
    }
}
