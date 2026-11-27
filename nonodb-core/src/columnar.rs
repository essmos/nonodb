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

use std::collections::{BTreeMap, HashMap, HashSet};

use heed::types::Bytes;
use heed::{Database, Env, RoTxn, RwTxn};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::bucket_codec::{
    decode_column, decode_column_at, encode_column_fast, encode_column_write, get_bytes,
    get_uvarint, put_bytes, put_uvarint, Compression, HARD_MAX_COLUMN_ROWS,
};
use crate::collection::{compare_docs, project_document, FindQuery, UpdateOutcome};
use crate::error::{Error, Result};

/// Metadata prefix for columnar settings.
pub const SETTINGS_META_PREFIX: &str = "col:";
/// Prefix of the dedicated LMDB sub-database.
pub const DB_PREFIX: &str = "colb:";

const MAGIC: &[u8; 4] = b"NCB1";
const WRITE_MAGIC: &[u8; 4] = b"NCWR";
const FORMAT_VERSION: u8 = 2;
const LEGACY_FORMAT_VERSION: u8 = 1;
const BLOCK_ROWS: usize = 32;
const LEGACY_BLOCK_ROWS: usize = 256;

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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
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

fn set_path_parts(doc: &mut Map<String, Value>, parts: &[String], value: Value) -> Result<()> {
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

#[derive(Default)]
struct PathInterner {
    parts: Vec<Vec<String>>,
    names: Vec<String>,
    children: Vec<HashMap<String, usize>>,
}

impl PathInterner {
    fn new() -> Self {
        Self {
            parts: vec![Vec::new()],
            names: vec![String::new()],
            children: vec![HashMap::new()],
        }
    }

    fn intern(&mut self, parent: usize, key: &str) -> usize {
        if let Some(existing) = self.children[parent].get(key) {
            return *existing;
        }
        let id = self.names.len();
        let mut parts = self.parts[parent].clone();
        parts.push(key.to_string());
        self.names.push(path_key(&parts));
        self.parts.push(parts);
        self.children.push(HashMap::new());
        self.children[parent].insert(key.to_string(), id);
        id
    }
}

fn ensure_column(columns: &mut Vec<Vec<Option<Value>>>, id: usize) {
    if columns.len() <= id {
        columns.resize_with(id + 1, Vec::new);
    }
}

fn flatten_value(
    parent: usize,
    value: &Value,
    columns: &mut Vec<Vec<Option<Value>>>,
    row: usize,
    interner: &mut PathInterner,
) {
    match value {
        Value::Object(map) if !map.is_empty() => {
            for (key, child) in map {
                let id = interner.intern(parent, key);
                ensure_column(columns, id);
                flatten_value(id, child, columns, row, interner);
            }
        }
        other => {
            if parent != 0 {
                ensure_column(columns, parent);
                let column = &mut columns[parent];
                if column.len() < row {
                    column.resize(row, None);
                }
                column.push(Some(other.clone()));
            }
        }
    }
}

fn flatten_rows(rows: &[Map<String, Value>]) -> (Vec<Vec<Option<Value>>>, Vec<String>) {
    let mut interner = PathInterner::new();
    let mut columns: Vec<Vec<Option<Value>>> = vec![Vec::new()];
    for (row_index, row) in rows.iter().enumerate() {
        for (key, value) in row {
            let id = interner.intern(0, key);
            ensure_column(&mut columns, id);
            flatten_value(id, value, &mut columns, row_index, &mut interner);
        }
    }
    for column in &mut columns {
        column.resize(rows.len(), None);
    }
    (columns, interner.names)
}

fn encode_bucket(rows: &[Map<String, Value>], settings: &Settings) -> Result<Vec<u8>> {
    encode_bucket_with(rows, settings, false)
}

fn encode_bucket_compact(rows: &[Map<String, Value>], settings: &Settings) -> Result<Vec<u8>> {
    encode_bucket_with(rows, settings, true)
}

fn encode_bucket_with(
    rows: &[Map<String, Value>],
    settings: &Settings,
    compact: bool,
) -> Result<Vec<u8>> {
    if !compact {
        return encode_bucket_raw(rows);
    }
    let (columns, names) = flatten_rows(rows);
    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(MAGIC);
    out.push(FORMAT_VERSION);
    put_uvarint(&mut out, BLOCK_ROWS as u64);
    put_uvarint(&mut out, rows.len() as u64);
    put_uvarint(&mut out, names.len().saturating_sub(1) as u64);
    for (name_id, name) in names.iter().enumerate().skip(1) {
        put_bytes(&mut out, name.as_bytes());
        let values = &columns[name_id];
        let blocks = values.len().div_ceil(BLOCK_ROWS).max(1);
        put_uvarint(&mut out, blocks as u64);
        for chunk in values.chunks(BLOCK_ROWS) {
            let blob = if compact {
                encode_column_fast(name, chunk, settings.compression)?
            } else {
                encode_column_write(name, chunk)?
            };
            put_bytes(&mut out, &blob);
        }
    }
    Ok(out)
}

fn encode_bucket_raw(rows: &[Map<String, Value>]) -> Result<Vec<u8>> {
    let mut parts = Vec::with_capacity(rows.len());
    for row in rows {
        parts.push(encode_json_fast(row)?);
    }
    Ok(encode_bucket_raw_parts(&parts))
}

fn encode_bucket_raw_parts(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(parts.iter().map(|part| part.len() + 4).sum());
    out.extend_from_slice(WRITE_MAGIC);
    put_uvarint(&mut out, parts.len() as u64);
    for part in parts {
        put_bytes(&mut out, part);
    }
    out
}

fn decode_bucket_raw(data: &[u8]) -> Result<Vec<Map<String, Value>>> {
    if data.len() < 4 || &data[..4] != WRITE_MAGIC {
        return Err(corrupt("bad raw header"));
    }
    let mut pos = 4;
    let rows = get_uvarint(data, &mut pos)? as usize;
    if rows > HARD_MAX_COLUMN_ROWS {
        return Err(corrupt("row count out of range"));
    }
    let mut out = Vec::with_capacity(rows);
    for _ in 0..rows {
        out.push(
            sonic_rs::from_slice(get_bytes(data, &mut pos)?)
                .map_err(|e| Error::JsonFast(e.to_string()))?,
        );
    }
    Ok(out)
}

fn decode_bucket_header(data: &[u8]) -> Result<(usize, usize, usize)> {
    if data.len() < 6 || &data[..4] != MAGIC {
        return Err(corrupt("bad header"));
    }
    let version = data[4];
    let mut pos = 5;
    let block_rows = match version {
        LEGACY_FORMAT_VERSION => LEGACY_BLOCK_ROWS,
        FORMAT_VERSION => get_uvarint(data, &mut pos)? as usize,
        _ => return Err(corrupt("bad header")),
    };
    if block_rows == 0 || block_rows > HARD_MAX_COLUMN_ROWS {
        return Err(corrupt("block row count out of range"));
    }
    let rows = get_uvarint(data, &mut pos)? as usize;
    if rows > HARD_MAX_COLUMN_ROWS {
        return Err(corrupt("row count out of range"));
    }
    Ok((block_rows, rows, pos))
}

fn decode_bucket(data: &[u8]) -> Result<Vec<Map<String, Value>>> {
    if data.starts_with(WRITE_MAGIC) {
        return decode_bucket_raw(data);
    }
    let (_block_rows, rows, mut pos) = decode_bucket_header(data)?;
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
        let parts: Vec<String> = serde_json::from_str(&name)?;
        let mut row = 0usize;
        for _ in 0..blocks {
            let blob = get_bytes(data, &mut pos)?;
            let mut block_pos = 0;
            let column = decode_column(blob, &mut block_pos)?;
            if column.field != name {
                return Err(corrupt("column field mismatch"));
            }
            let end = row.saturating_add(column.values.len());
            if end > rows {
                return Err(corrupt("column block exceeds row count"));
            }
            for (target, value) in out[row..end].iter_mut().zip(column.values) {
                if let Some(value) = value {
                    set_path_parts(target, &parts, value)?;
                }
            }
            row = end;
        }
        if row != rows {
            return Err(corrupt("column blocks do not cover every row"));
        }
    }
    Ok(out)
}

/// Decodes only the column block that contains `row`.
fn decode_bucket_row(data: &[u8], row: usize) -> Result<Option<Map<String, Value>>> {
    if data.starts_with(WRITE_MAGIC) {
        if data.len() < 4 {
            return Err(corrupt("bad raw header"));
        }
        let mut pos = 4;
        let rows = get_uvarint(data, &mut pos)? as usize;
        if rows > HARD_MAX_COLUMN_ROWS {
            return Err(corrupt("row count out of range"));
        }
        if row >= rows {
            return Ok(None);
        }
        for _ in 0..row {
            get_bytes(data, &mut pos)?;
        }
        return Ok(Some(
            sonic_rs::from_slice(get_bytes(data, &mut pos)?)
                .map_err(|e| Error::JsonFast(e.to_string()))?,
        ));
    }
    let (block_rows, rows, mut pos) = decode_bucket_header(data)?;
    if row >= rows {
        return Ok(None);
    }
    let target_block = row / block_rows;
    let offset = row % block_rows;
    let columns = get_uvarint(data, &mut pos)? as usize;
    let mut out = Map::new();
    for _ in 0..columns {
        let name = std::str::from_utf8(get_bytes(data, &mut pos)?)
            .map_err(|_| corrupt("field name is not UTF-8"))?
            .to_string();
        let blocks = get_uvarint(data, &mut pos)? as usize;
        if blocks > HARD_MAX_COLUMN_ROWS {
            return Err(corrupt("block count out of range"));
        }
        let parts: Vec<String> = serde_json::from_str(&name)?;
        let mut value = None;
        for block in 0..blocks {
            let blob = get_bytes(data, &mut pos)?;
            if block != target_block {
                continue;
            }
            let mut block_pos = 0;
            let (field, row_value) = decode_column_at(blob, &mut block_pos, offset)?;
            if field != name {
                return Err(corrupt("column field mismatch"));
            }
            value = row_value;
        }
        if let Some(value) = value {
            set_path_parts(&mut out, &parts, value)?;
        }
    }
    Ok(Some(out))
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
    decode_bucket_row(raw, row as usize)
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
        db.delete(&mut *wtxn, &deleted_key(bucket, row))?;
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
        // Keep the open row until the bucket is sealed; sealing walks every
        // row between the pointer and the end. The tombstone hides it now.
        db.put(&mut *wtxn, &deleted_key(bucket, row), b"")?;
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

enum RawBlock<'a> {
    Open(Locator, &'a [u8]),
    Bucket(u64, &'a [u8]),
}

fn decode_blocks(blocks: &[RawBlock<'_>]) -> Result<Vec<(Locator, Map<String, Value>)>> {
    let mut docs = Vec::new();
    for block in blocks {
        match block {
            RawBlock::Open(locator, raw) => {
                docs.push((*locator, serde_json::from_slice(raw)?));
            }
            RawBlock::Bucket(bucket, raw) => {
                for (row, doc) in decode_bucket(raw)?.into_iter().enumerate() {
                    docs.push(((*bucket, row as u32), doc));
                }
            }
        }
    }
    Ok(docs)
}

fn visit_block<F>(
    block: &RawBlock<'_>,
    prefilter: Option<&crate::collection::RawPrefilter>,
    mut emit: F,
) -> Result<()>
where
    F: FnMut(Locator, Map<String, Value>) -> Result<()>,
{
    match block {
        RawBlock::Open(locator, raw) => {
            let Some(value) = crate::collection::parse_prefiltered(raw, prefilter)? else {
                return Ok(());
            };
            let doc = value
                .as_object()
                .cloned()
                .ok_or_else(|| corrupt("open row is not an object"))?;
            emit(*locator, doc)
        }
        RawBlock::Bucket(bucket, raw) => {
            if raw.starts_with(WRITE_MAGIC) {
                let mut pos = 4;
                let rows = get_uvarint(raw, &mut pos)? as usize;
                if rows > HARD_MAX_COLUMN_ROWS {
                    return Err(corrupt("row count out of range"));
                }
                for row in 0..rows {
                    let Some(value) =
                        crate::collection::parse_prefiltered(get_bytes(raw, &mut pos)?, prefilter)?
                    else {
                        continue;
                    };
                    let doc = value
                        .as_object()
                        .cloned()
                        .ok_or_else(|| corrupt("staged row is not an object"))?;
                    emit((*bucket, row as u32), doc)?;
                }
                Ok(())
            } else {
                for (row, doc) in decode_bucket(raw)?.into_iter().enumerate() {
                    emit((*bucket, row as u32), doc)?;
                }
                Ok(())
            }
        }
    }
}

fn all_located(
    txn: &RoTxn<'_>,
    env: &Env,
    name: &str,
) -> Result<Vec<(Locator, Map<String, Value>)>> {
    let Some(db) = env.open_database::<Bytes, Bytes>(txn, Some(&db_name(name)))? else {
        return Ok(Vec::new());
    };
    let mut blocks: Vec<RawBlock<'_>> = Vec::new();
    for item in db.iter(txn)? {
        let (key, value) = item?;
        match key.first() {
            Some(&KEY_OPEN_ROW) => {
                if let Some(locator) = key_locator(key) {
                    blocks.push(RawBlock::Open(locator, value));
                }
            }
            Some(&KEY_BUCKET) => {
                if key.len() != 9 {
                    return Err(corrupt("malformed bucket key"));
                }
                let bucket = u64::from_be_bytes(key[1..9].try_into().expect("8 bytes"));
                blocks.push(RawBlock::Bucket(bucket, value));
            }
            _ => {}
        }
    }
    let mut docs = decode_blocks(&blocks)?;
    docs.sort_unstable_by_key(|(locator, _)| *locator);
    let mut overlays: HashMap<Locator, Map<String, Value>> = HashMap::new();
    let mut deleted: HashSet<Locator> = HashSet::new();
    for item in db.iter(txn)? {
        let (key, value) = item?;
        match key.first() {
            Some(&KEY_OVERLAY) => {
                if let Some(locator) = key_locator(key) {
                    overlays.insert(locator, serde_json::from_slice(value)?);
                }
            }
            Some(&KEY_DELETED) => {
                if let Some(locator) = key_locator(key) {
                    deleted.insert(locator);
                }
            }
            _ => {}
        }
    }
    if !overlays.is_empty() {
        for (locator, doc) in &mut docs {
            if let Some(overlay) = overlays.remove(locator) {
                *doc = overlay;
            }
        }
    }
    if !deleted.is_empty() {
        docs.retain(|(locator, _)| !deleted.contains(locator));
    }
    Ok(docs)
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

fn encode_json_fast<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    sonic_rs::to_vec(value).map_err(|error| Error::JsonFast(error.to_string()))
}

fn encode_batch_parallel(docs: &[Value]) -> Result<Vec<Vec<u8>>> {
    let workers = crate::collection::scan_workers().min(docs.len()).max(1);
    if workers <= 1 || docs.len() < 64 {
        return docs.iter().map(encode_json_fast).collect();
    }
    let chunk_size = docs.len().div_ceil(workers);
    let results = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for chunk in docs.chunks(chunk_size) {
            handles.push(scope.spawn(move || -> Result<Vec<Vec<u8>>> {
                chunk.iter().map(encode_json_fast).collect()
            }));
        }
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .unwrap_or_else(|_| Err(Error::Query("columnar encode worker panicked".into())))
            })
            .collect::<Vec<_>>()
    });
    let mut out = Vec::with_capacity(docs.len());
    for result in results {
        out.extend(result?);
    }
    Ok(out)
}

/// Inserts a batch of already-normalized documents, buffering the active
/// bucket in memory. Returns the assigned ids and the stored documents.
pub fn insert_batch(
    wtxn: &mut RwTxn<'_>,
    env: &Env,
    name: &str,
    settings: &Settings,
    mut docs: Vec<Value>,
) -> Result<Vec<(String, Value)>> {
    settings.validate()?;
    let db = env.create_database::<Bytes, Bytes>(wtxn, Some(&db_name(name)))?;
    let mut pointer = read_pointer_rw(&*wtxn, &db)?.unwrap_or((1, 0, 0));
    let mut disk_rows = pointer.1;
    let mut open: Vec<Vec<u8>> = Vec::with_capacity(pointer.1 as usize + docs.len());
    for row in 0..pointer.1 {
        let key = open_row_key(pointer.0, row);
        let raw = db
            .get(&*wtxn, &key)?
            .ok_or_else(|| corrupt("missing open row"))?;
        open.push(raw.to_vec());
    }
    let mut known: HashMap<u64, Vec<(String, Locator)>> = HashMap::new();
    let mut out = Vec::with_capacity(docs.len());
    let mut ids = Vec::with_capacity(docs.len());
    for doc in docs.iter_mut() {
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
        ids.push(id);
    }
    let encoded_docs = encode_batch_parallel(&docs)?;
    let mut encoded_iter = encoded_docs.into_iter();
    let mut ids_iter = ids.into_iter();
    for doc in docs.drain(..) {
        let id = ids_iter.next().expect("id per document");
        let encoded = encoded_iter.next().expect("encoded per document");
        if encoded.len() > crate::store::MAX_DOCUMENT_BYTES {
            return Err(Error::DocumentTooLarge(
                encoded.len(),
                crate::store::MAX_DOCUMENT_BYTES,
            ));
        }
        let hash = fnv1a64(id.as_bytes());
        if let std::collections::hash_map::Entry::Vacant(entry) = known.entry(hash) {
            let mut entries = Vec::new();
            for locator in read_locators(&*wtxn, &db, hash)? {
                if let Some(row) = read_doc_at(&*wtxn, &db, locator)? {
                    entries.push((doc_id(&row)?, locator));
                }
            }
            if entries.iter().any(|(existing, _)| *existing == id) {
                return Err(Error::DuplicateKey(id));
            }
            entry.insert(entries);
        } else if known
            .get(&hash)
            .is_some_and(|entries| entries.iter().any(|(existing, _)| *existing == id))
        {
            return Err(Error::DuplicateKey(id));
        }
        if pointer.1 >= settings.bucket_max_docs as u32
            || pointer.2 as usize + encoded.len() > settings.bucket_max_bytes
        {
            if !open.is_empty() {
                let payload = encode_bucket_raw_parts(&open);
                db.put(&mut *wtxn, &bucket_key(pointer.0), &payload)?;
                for row in 0..disk_rows {
                    db.delete(&mut *wtxn, &open_row_key(pointer.0, row))?;
                }
                open.clear();
            }
            pointer = (pointer.0 + 1, 0, 0);
            disk_rows = 0;
        }
        let row = pointer.1;
        let encoded_len = encoded.len() as u32;
        open.push(encoded);
        known
            .get_mut(&hash)
            .expect("hash inserted")
            .push((id.clone(), (pointer.0, row)));
        pointer = (
            pointer.0,
            pointer.1 + 1,
            pointer.2.saturating_add(encoded_len),
        );
        out.push((id, doc));

        if pointer.1 >= settings.bucket_max_docs as u32
            || pointer.2 as usize >= settings.bucket_max_bytes
        {
            let payload = encode_bucket_raw_parts(&open);
            db.put(&mut *wtxn, &bucket_key(pointer.0), &payload)?;
            for row in 0..disk_rows {
                db.delete(&mut *wtxn, &open_row_key(pointer.0, row))?;
            }
            open.clear();
            pointer = (pointer.0 + 1, 0, 0);
            disk_rows = 0;
        }
    }

    for (hash, entries) in known {
        let mut locators = Vec::with_capacity(entries.len());
        for (_id, locator) in entries {
            locators.push(locator);
        }
        if !locators.is_empty() {
            db.put(&mut *wtxn, &id_key(hash), &encode_locators(&locators))?;
        }
    }
    if !open.is_empty() {
        // Large batches leave a partially filled tail. Sealing it avoids
        // re-reading and re-encoding those rows on the next batch, and keeps
        // the next writer transaction on an empty active bucket.
        if pointer.1 >= (settings.bucket_max_docs as u32 / 2).max(1) {
            let payload = encode_bucket_raw_parts(&open);
            db.put(&mut *wtxn, &bucket_key(pointer.0), &payload)?;
            for row in 0..disk_rows {
                db.delete(&mut *wtxn, &open_row_key(pointer.0, row))?;
            }
            pointer = (pointer.0 + 1, 0, 0);
        } else {
            for (row, encoded) in open.iter().enumerate().skip(disk_rows as usize) {
                db.put(&mut *wtxn, &open_row_key(pointer.0, row as u32), encoded)?;
            }
        }
    }
    db.put(
        &mut *wtxn,
        &pointer_key(),
        &encode_pointer(pointer.0, pointer.1, pointer.2),
    )?;
    Ok(out)
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

/// Visits every visible document in on-disk order without materializing the
/// whole collection. Used by streaming aggregation.
pub fn for_each<F>(rtxn: &RoTxn<'_>, env: &Env, name: &str, mut emit: F) -> Result<()>
where
    F: FnMut(Value) -> Result<()>,
{
    let Some(db) = env.open_database::<Bytes, Bytes>(rtxn, Some(&db_name(name)))? else {
        return Ok(());
    };
    let mut overlays: HashMap<Locator, Map<String, Value>> = HashMap::new();
    let mut deleted: HashSet<Locator> = HashSet::new();
    for item in db.iter(rtxn)? {
        let (key, value) = item?;
        match key.first() {
            Some(&KEY_OVERLAY) => {
                if let Some(locator) = key_locator(key) {
                    overlays.insert(locator, serde_json::from_slice(value)?);
                }
            }
            Some(&KEY_DELETED) => {
                if let Some(locator) = key_locator(key) {
                    deleted.insert(locator);
                }
            }
            _ => {}
        }
    }
    for item in db.iter(rtxn)? {
        let (key, value) = item?;
        match key.first() {
            Some(&KEY_OPEN_ROW) => {
                let Some(locator) = key_locator(key) else {
                    continue;
                };
                if deleted.contains(&locator) {
                    continue;
                }
                let doc = overlays
                    .remove(&locator)
                    .map(Ok)
                    .unwrap_or_else(|| serde_json::from_slice(value))?;
                emit(Value::Object(doc))?;
            }
            Some(&KEY_BUCKET) => {
                if key.len() != 9 {
                    return Err(corrupt("malformed bucket key"));
                }
                let bucket = u64::from_be_bytes(key[1..9].try_into().expect("8 bytes"));
                for (row, doc) in decode_bucket(value)?.into_iter().enumerate() {
                    let locator = (bucket, row as u32);
                    if deleted.contains(&locator) {
                        continue;
                    }
                    let doc = overlays.remove(&locator).unwrap_or(doc);
                    emit(Value::Object(doc))?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

struct GroupProjection {
    tree: sonic_rs::PointerTree,
    paths: Vec<Vec<String>>,
}

fn collect_group_paths(expr: &Value, paths: &mut Vec<Vec<String>>) -> Option<()> {
    match expr {
        Value::String(text) => {
            let Some(path) = text.strip_prefix('$') else {
                return Some(());
            };
            if path.is_empty() || path.starts_with('$') || path.contains('$') {
                return None;
            }
            let parts: Vec<String> = path.split('.').map(str::to_string).collect();
            if parts.iter().any(|part| part.is_empty()) {
                return None;
            }
            if !paths.contains(&parts) {
                paths.push(parts);
            }
            Some(())
        }
        Value::Array(items) => {
            for item in items {
                collect_group_paths(item, paths)?;
            }
            Some(())
        }
        Value::Object(object) => {
            for value in object.values() {
                collect_group_paths(value, paths)?;
            }
            Some(())
        }
        _ => Some(()),
    }
}

fn group_projection(spec: &Value) -> Option<GroupProjection> {
    let object = spec.as_object()?;
    let mut paths = Vec::new();
    if let Some(id) = object.get("_id") {
        collect_group_paths(id, &mut paths)?;
    }
    for (field, accumulator) in object {
        if field == "_id" {
            continue;
        }
        let accumulator = accumulator.as_object()?;
        if accumulator.len() != 1 {
            return None;
        }
        collect_group_paths(accumulator.values().next()?, &mut paths)?;
    }
    let mut tree = sonic_rs::PointerTree::new();
    for path in &paths {
        tree.add_path(path.iter().map(String::as_str));
    }
    Some(GroupProjection { tree, paths })
}

fn project_raw_doc(raw: &[u8], projection: &GroupProjection) -> Result<Map<String, Value>> {
    if projection.paths.is_empty() {
        return Ok(Map::new());
    }
    let nodes = sonic_rs::get_many(raw, &projection.tree)
        .map_err(|error| Error::JsonFast(error.to_string()))?;
    let mut doc = Map::new();
    for (path, node) in projection.paths.iter().zip(nodes) {
        let Some(node) = node else {
            continue;
        };
        let value: Value = serde_json::from_str(node.as_raw_str())?;
        set_path_parts(&mut doc, path, value)?;
    }
    Ok(doc)
}

fn block_rows(block: &RawBlock<'_>) -> usize {
    match block {
        RawBlock::Open(_, _) => 1,
        RawBlock::Bucket(_, raw) => {
            if raw.starts_with(WRITE_MAGIC) {
                let mut pos = 4;
                get_uvarint(raw, &mut pos).unwrap_or(1) as usize
            } else {
                decode_bucket_header(raw).map_or(1, |(_, rows, _)| rows)
            }
        }
    }
}

fn balanced_ranges(blocks: &[RawBlock<'_>], workers: usize) -> Vec<(usize, usize)> {
    if blocks.is_empty() {
        return Vec::new();
    }
    let total: usize = blocks.iter().map(block_rows).sum();
    let target = total.div_ceil(workers.max(1));
    let mut ranges = Vec::with_capacity(workers);
    let mut start = 0usize;
    let mut accumulated = 0usize;
    for (index, block) in blocks.iter().enumerate() {
        let rows = block_rows(block);
        if accumulated > 0 && accumulated + rows > target && ranges.len() + 1 < workers {
            ranges.push((start, index));
            start = index;
            accumulated = 0;
        }
        accumulated += rows;
    }
    ranges.push((start, blocks.len()));
    ranges
}

fn block_is_raw(block: &RawBlock<'_>) -> bool {
    match block {
        RawBlock::Open(_, _) => true,
        RawBlock::Bucket(_, raw) => raw.starts_with(WRITE_MAGIC),
    }
}

fn visit_projected_block<F>(
    block: &RawBlock<'_>,
    projection: &GroupProjection,
    mut emit: F,
) -> Result<bool>
where
    F: FnMut(Map<String, Value>) -> Result<()>,
{
    match block {
        RawBlock::Open(_, raw) => {
            emit(project_raw_doc(raw, projection)?)?;
            Ok(true)
        }
        RawBlock::Bucket(_, raw) => {
            if !raw.starts_with(WRITE_MAGIC) {
                return Ok(false);
            }
            let mut pos = 4;
            let rows = get_uvarint(raw, &mut pos)? as usize;
            if rows > HARD_MAX_COLUMN_ROWS {
                return Err(corrupt("row count out of range"));
            }
            let mut docs = Vec::with_capacity(rows);
            for _ in 0..rows {
                docs.push(project_raw_doc(get_bytes(raw, &mut pos)?, projection)?);
            }
            for doc in docs {
                emit(doc)?;
            }
            Ok(true)
        }
    }
}

#[derive(Clone)]
struct FastSpec {
    id: Vec<String>,
    sums: Vec<(String, Option<Vec<String>>)>,
}

#[derive(Clone)]
enum FastSum {
    Count(u64),
    Sum(crate::aggregate::NumericSum),
}

struct FastState {
    id: Value,
    sums: Vec<FastSum>,
}

struct FastGroups {
    spec: FastSpec,
    strings: BTreeMap<String, FastState>,
    others: BTreeMap<String, FastState>,
}

fn field_path(value: &Value) -> Option<Vec<String>> {
    let text = value.as_str()?;
    let path = text.strip_prefix('$')?;
    if path.is_empty() || path.starts_with('$') || path.contains('$') {
        return None;
    }
    let parts: Vec<String> = path.split('.').map(str::to_string).collect();
    if parts.iter().any(|part| part.is_empty()) {
        return None;
    }
    Some(parts)
}

fn fast_group_spec(spec: &Value) -> Option<FastSpec> {
    let object = spec.as_object()?;
    let id = field_path(object.get("_id")?)?;
    let mut sums = Vec::new();
    for (field, accumulator) in object {
        if field == "_id" {
            continue;
        }
        let accumulator = accumulator.as_object()?;
        if accumulator.len() != 1 {
            return None;
        }
        let (op, arg) = accumulator.iter().next()?;
        match op.as_str() {
            "$count" => sums.push((field.clone(), None)),
            "$sum" => {
                if arg.as_u64() == Some(1) || arg.as_i64() == Some(1) {
                    sums.push((field.clone(), None));
                } else {
                    sums.push((field.clone(), Some(field_path(arg)?)));
                }
            }
            _ => return None,
        }
    }
    Some(FastSpec { id, sums })
}

fn value_at<'a>(value: &'a Value, parts: &[String]) -> Option<&'a Value> {
    let mut current = value;
    for part in parts {
        current = match current {
            Value::Object(object) => object.get(part)?,
            Value::Array(items) => items.get(part.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(current)
}

impl FastGroups {
    fn new(spec: FastSpec) -> Self {
        Self {
            spec,
            strings: BTreeMap::new(),
            others: BTreeMap::new(),
        }
    }

    fn state(&self, id: Value) -> FastState {
        FastState {
            id,
            sums: self
                .spec
                .sums
                .iter()
                .map(|(_, path)| match path {
                    Some(_) => FastSum::Sum(crate::aggregate::NumericSum::zero()),
                    None => FastSum::Count(0),
                })
                .collect(),
        }
    }

    fn apply_sums(state: &mut FastState, spec: &FastSpec, doc: &Value) {
        for ((_, path), sum) in spec.sums.iter().zip(state.sums.iter_mut()) {
            match sum {
                FastSum::Count(count) => *count += 1,
                FastSum::Sum(total) => {
                    if let Some(path) = path {
                        if let Some(value) = value_at(doc, path) {
                            total.add(value);
                        }
                    }
                }
            }
        }
    }

    fn update(&mut self, doc: &Value) {
        let id = value_at(doc, &self.spec.id);
        if let Some(text) = id.and_then(Value::as_str) {
            if let Some(state) = self.strings.get_mut(text) {
                Self::apply_sums(state, &self.spec, doc);
                return;
            }
            let id = Value::String(text.to_string());
            let mut state = self.state(id);
            Self::apply_sums(&mut state, &self.spec, doc);
            self.strings.insert(text.to_string(), state);
            return;
        }
        let id = id.cloned().unwrap_or(Value::Null);
        let key = serde_json::to_string(&id).unwrap_or_default();
        if let Some(state) = self.others.get_mut(&key) {
            Self::apply_sums(state, &self.spec, doc);
            return;
        }
        let mut state = self.state(id);
        Self::apply_sums(&mut state, &self.spec, doc);
        self.others.insert(key, state);
    }

    fn merge(&mut self, other: FastGroups) -> Result<()> {
        for (key, incoming) in other.strings {
            match self.strings.entry(key) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert(incoming);
                }
                std::collections::btree_map::Entry::Occupied(mut slot) => {
                    merge_fast_state(slot.get_mut(), incoming)?;
                }
            }
        }
        for (key, incoming) in other.others {
            match self.others.entry(key) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert(incoming);
                }
                std::collections::btree_map::Entry::Occupied(mut slot) => {
                    merge_fast_state(slot.get_mut(), incoming)?;
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<Vec<Value>> {
        let mut rows = Vec::with_capacity(self.strings.len() + self.others.len());
        for state in self.strings.into_values().chain(self.others.into_values()) {
            let key = serde_json::to_string(&state.id)?;
            let mut out = Map::new();
            out.insert("_id".into(), state.id);
            for ((field, _), sum) in self.spec.sums.iter().zip(state.sums) {
                let value = match sum {
                    FastSum::Count(count) => Value::from(count),
                    FastSum::Sum(total) => total.finish()?,
                };
                out.insert(field.clone(), value);
            }
            rows.push((key, Value::Object(out)));
        }
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(rows.into_iter().map(|(_, value)| value).collect())
    }
}

fn merge_fast_state(target: &mut FastState, incoming: FastState) -> Result<()> {
    for (left, right) in target.sums.iter_mut().zip(incoming.sums) {
        match (left, right) {
            (FastSum::Count(left), FastSum::Count(right)) => *left += right,
            (FastSum::Sum(left), FastSum::Sum(right)) => left.merge(right),
            _ => return Err(Error::Query("incompatible fast group state".into())),
        }
    }
    Ok(())
}

fn visit_fast_block(block: &RawBlock<'_>, groups: &mut FastGroups) -> Result<bool> {
    match block {
        RawBlock::Open(_, raw) => {
            let value: Value =
                sonic_rs::from_slice(raw).map_err(|error| Error::JsonFast(error.to_string()))?;
            groups.update(&value);
            Ok(true)
        }
        RawBlock::Bucket(_, raw) => {
            if !raw.starts_with(WRITE_MAGIC) {
                return Ok(false);
            }
            let mut pos = 4;
            let rows = get_uvarint(raw, &mut pos)? as usize;
            if rows > HARD_MAX_COLUMN_ROWS {
                return Err(corrupt("row count out of range"));
            }
            for _ in 0..rows {
                let value: Value = sonic_rs::from_slice(get_bytes(raw, &mut pos)?)
                    .map_err(|error| Error::JsonFast(error.to_string()))?;
                groups.update(&value);
            }
            Ok(true)
        }
    }
}

fn fast_group_parallel(
    blocks: &[RawBlock<'_>],
    group_spec: &Value,
    workers: usize,
) -> Result<Option<Vec<Value>>> {
    let Some(spec) = fast_group_spec(group_spec) else {
        return Ok(None);
    };
    if !blocks.iter().all(block_is_raw) {
        return Ok(None);
    }
    let workers = workers.min(blocks.len().max(1));
    if workers <= 1 || blocks.len() < 2 {
        let mut groups = FastGroups::new(spec);
        for block in blocks {
            visit_fast_block(block, &mut groups)?;
        }
        return groups.finish().map(Some);
    }
    let results = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for (start, end) in balanced_ranges(blocks, workers) {
            let chunk = &blocks[start..end];
            let spec = spec.clone();
            handles.push(scope.spawn(move || -> Result<FastGroups> {
                let mut groups = FastGroups::new(spec);
                for block in chunk {
                    visit_fast_block(block, &mut groups)?;
                }
                Ok(groups)
            }));
        }
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .unwrap_or_else(|_| Err(Error::Query("columnar group worker panicked".into())))
            })
            .collect::<Vec<_>>()
    });
    let mut merged = FastGroups::new(spec);
    for result in results {
        merged.merge(result?)?;
    }
    merged.finish().map(Some)
}

/// Runs an order-independent `$group` over decoded buckets in parallel.
pub fn group_parallel(
    rtxn: &RoTxn<'_>,
    env: &Env,
    name: &str,
    filter: &Value,
    group_spec: &Value,
    workers: usize,
) -> Result<Vec<Value>> {
    use crate::aggregate::GroupBuilder;

    let Some(db) = env.open_database::<Bytes, Bytes>(rtxn, Some(&db_name(name)))? else {
        return GroupBuilder::new(group_spec)?.finish();
    };
    let mut overlays: HashMap<Locator, Map<String, Value>> = HashMap::new();
    let mut deleted: HashSet<Locator> = HashSet::new();
    for item in db.iter(rtxn)? {
        let (key, value) = item?;
        match key.first() {
            Some(&KEY_OVERLAY) => {
                if let Some(locator) = key_locator(key) {
                    overlays.insert(locator, serde_json::from_slice(value)?);
                }
            }
            Some(&KEY_DELETED) => {
                if let Some(locator) = key_locator(key) {
                    deleted.insert(locator);
                }
            }
            _ => {}
        }
    }
    let mut blocks: Vec<RawBlock<'_>> = Vec::new();
    for item in db.iter(rtxn)? {
        let (key, value) = item?;
        match key.first() {
            Some(&KEY_OPEN_ROW) => {
                if let Some(locator) = key_locator(key) {
                    blocks.push(RawBlock::Open(locator, value));
                }
            }
            Some(&KEY_BUCKET) => {
                if key.len() != 9 {
                    return Err(corrupt("malformed bucket key"));
                }
                let bucket = u64::from_be_bytes(key[1..9].try_into().expect("8 bytes"));
                blocks.push(RawBlock::Bucket(bucket, value));
            }
            _ => {}
        }
    }

    let has_deleted = !deleted.is_empty();
    let has_overlays = !overlays.is_empty();
    let empty_filter =
        filter.is_null() || filter.as_object().is_some_and(|object| object.is_empty());
    let workers = workers.min(blocks.len().max(1));
    if empty_filter && !has_deleted && !has_overlays {
        if let Some(groups) = fast_group_parallel(&blocks, group_spec, workers)? {
            return Ok(groups);
        }
    }
    let projection = if empty_filter && !has_deleted && !has_overlays {
        group_projection(group_spec)
    } else {
        None
    };
    let process = |chunk: &[RawBlock], builder: &mut GroupBuilder| -> Result<()> {
        if let Some(projection) = &projection {
            if chunk.iter().all(block_is_raw) {
                for block in chunk {
                    visit_projected_block(block, projection, |doc| {
                        builder.update(Value::Object(doc))
                    })?;
                }
                return Ok(());
            }
        }
        for block in chunk {
            visit_block(block, None, |locator, doc| {
                if has_deleted && deleted.contains(&locator) {
                    return Ok(());
                }
                let doc = if has_overlays {
                    overlays.get(&locator).cloned().unwrap_or(doc)
                } else {
                    doc
                };
                let value = Value::Object(doc);
                if crate::query::matcher::matches(&value, filter) {
                    builder.update(value)?;
                }
                Ok(())
            })?;
        }
        Ok(())
    };

    if workers <= 1 || blocks.len() < 2 {
        let mut builder = GroupBuilder::new(group_spec)?;
        process(&blocks, &mut builder)?;
        return builder.finish();
    }
    let results = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for (start, end) in balanced_ranges(&blocks, workers) {
            let chunk = &blocks[start..end];
            let overlays = &overlays;
            let deleted = &deleted;
            let projection = projection.as_ref();
            handles.push(scope.spawn(move || -> Result<GroupBuilder> {
                let mut builder = GroupBuilder::new(group_spec)?;
                let has_deleted = !deleted.is_empty();
                let has_overlays = !overlays.is_empty();
                if let Some(projection) = projection {
                    if chunk.iter().all(block_is_raw) {
                        for block in chunk {
                            visit_projected_block(block, projection, |doc| {
                                builder.update(Value::Object(doc))
                            })?;
                        }
                        return Ok(builder);
                    }
                }
                for block in chunk {
                    visit_block(block, None, |locator, doc| {
                        if has_deleted && deleted.contains(&locator) {
                            return Ok(());
                        }
                        let doc = if has_overlays {
                            overlays.get(&locator).cloned().unwrap_or(doc)
                        } else {
                            doc
                        };
                        let value = Value::Object(doc);
                        if crate::query::matcher::matches(&value, filter) {
                            builder.update(value)?;
                        }
                        Ok(())
                    })?;
                }
                Ok(builder)
            }));
        }
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .unwrap_or_else(|_| Err(Error::Query("columnar group worker panicked".into())))
            })
            .collect::<Vec<_>>()
    });
    let mut merged = GroupBuilder::new(group_spec)?;
    for result in results {
        merged.merge(result?)?;
    }
    merged.finish()
}

/// Scans every visible document, applying update overlays and tombstones.
pub fn all_docs(rtxn: &RoTxn<'_>, env: &Env, name: &str) -> Result<Vec<Value>> {
    Ok(all_located(rtxn, env, name)?
        .into_iter()
        .map(|(_, doc)| Value::Object(doc))
        .collect())
}

fn find_parallel(rtxn: &RoTxn<'_>, env: &Env, name: &str, query: &FindQuery) -> Result<Vec<Value>> {
    let Some(db) = env.open_database::<Bytes, Bytes>(rtxn, Some(&db_name(name)))? else {
        return Ok(Vec::new());
    };
    let mut overlays: HashMap<Locator, Map<String, Value>> = HashMap::new();
    let mut deleted: HashSet<Locator> = HashSet::new();
    for item in db.iter(rtxn)? {
        let (key, value) = item?;
        match key.first() {
            Some(&KEY_OVERLAY) => {
                if let Some(locator) = key_locator(key) {
                    overlays.insert(locator, serde_json::from_slice(value)?);
                }
            }
            Some(&KEY_DELETED) => {
                if let Some(locator) = key_locator(key) {
                    deleted.insert(locator);
                }
            }
            _ => {}
        }
    }
    let mut blocks: Vec<RawBlock<'_>> = Vec::new();
    for item in db.iter(rtxn)? {
        let (key, value) = item?;
        match key.first() {
            Some(&KEY_OPEN_ROW) => {
                if let Some(locator) = key_locator(key) {
                    blocks.push(RawBlock::Open(locator, value));
                }
            }
            Some(&KEY_BUCKET) => {
                if key.len() != 9 {
                    return Err(corrupt("malformed bucket key"));
                }
                let bucket = u64::from_be_bytes(key[1..9].try_into().expect("8 bytes"));
                blocks.push(RawBlock::Bucket(bucket, value));
            }
            _ => {}
        }
    }

    let workers = crate::collection::scan_workers().min(blocks.len().max(1));
    let filter = &query.filter;
    let prefilter = if overlays.is_empty() {
        crate::collection::raw_prefilter(filter)
    } else {
        None
    };
    let prefilter_ref = prefilter.as_ref();
    let hits = if workers <= 1 || blocks.len() < 2 {
        let mut hits = Vec::new();
        let has_deleted = !deleted.is_empty();
        let has_overlays = !overlays.is_empty();
        for block in &blocks {
            visit_block(block, prefilter_ref, |locator, doc| {
                if has_deleted && deleted.contains(&locator) {
                    return Ok(());
                }
                let doc = if has_overlays {
                    overlays.get(&locator).cloned().unwrap_or(doc)
                } else {
                    doc
                };
                let value = Value::Object(doc);
                if crate::query::matcher::matches(&value, filter) {
                    hits.push(value);
                }
                Ok(())
            })?;
        }
        hits
    } else {
        let parts = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for (start, end) in balanced_ranges(&blocks, workers) {
                let chunk = &blocks[start..end];
                let overlays = &overlays;
                let deleted = &deleted;
                let prefilter = prefilter_ref;
                handles.push(scope.spawn(move || -> Result<Vec<Value>> {
                    let mut hits = Vec::new();
                    let has_deleted = !deleted.is_empty();
                    let has_overlays = !overlays.is_empty();
                    for block in chunk {
                        visit_block(block, prefilter, |locator, doc| {
                            if has_deleted && deleted.contains(&locator) {
                                return Ok(());
                            }
                            let doc = if has_overlays {
                                overlays.get(&locator).cloned().unwrap_or(doc)
                            } else {
                                doc
                            };
                            let value = Value::Object(doc);
                            if crate::query::matcher::matches(&value, filter) {
                                hits.push(value);
                            }
                            Ok(())
                        })?;
                    }
                    Ok(hits)
                }));
            }
            handles
                .into_iter()
                .map(|handle| {
                    handle.join().unwrap_or_else(|_| {
                        Err(Error::Query("columnar find worker panicked".into()))
                    })
                })
                .collect::<Vec<_>>()
        });
        let mut hits = Vec::new();
        for part in parts {
            hits.extend(part?);
        }
        hits
    };
    let limit = query.limit.unwrap_or(usize::MAX);
    Ok(hits
        .into_iter()
        .skip(query.skip)
        .take(limit)
        .map(|doc| project_document(&doc, query.projection.as_ref()))
        .collect())
}

/// Runs the normal find query shape over a columnar collection.
pub fn find(rtxn: &RoTxn<'_>, env: &Env, name: &str, query: &FindQuery) -> Result<Vec<Value>> {
    if query.sort.is_empty() {
        return find_parallel(rtxn, env, name, query);
    }
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
    let db = env.create_database::<Bytes, Bytes>(wtxn, Some(&db_name(name)))?;
    let mut open = Vec::new();
    let mut bucket = 1u64;
    for doc in docs {
        open.push(
            doc.as_object()
                .ok_or_else(|| Error::Query("columnar document must be an object".into()))?
                .clone(),
        );
        if open.len() >= settings.bucket_max_docs {
            let payload = encode_bucket_compact(&open, settings)?;
            db.put(&mut *wtxn, &bucket_key(bucket), &payload)?;
            open.clear();
            bucket += 1;
        }
    }
    if !open.is_empty() {
        let payload = encode_bucket_compact(&open, settings)?;
        db.put(&mut *wtxn, &bucket_key(bucket), &payload)?;
        bucket += 1;
    }
    db.put(&mut *wtxn, &pointer_key(), &encode_pointer(bucket, 0, 0))?;
    // Rebuild the locator map for the rewritten rows.
    for (row, doc) in all_docs(&*wtxn, env, name)?.into_iter().enumerate() {
        let object = doc
            .as_object()
            .ok_or_else(|| Error::Query("columnar document must be an object".into()))?;
        let id = doc_id(object)?;
        let hash = fnv1a64(id.as_bytes());
        let db = env.create_database::<Bytes, Bytes>(wtxn, Some(&db_name(name)))?;
        let mut locators = read_locators(&*wtxn, &db, hash)?;
        let bucket = row as u64 / settings.bucket_max_docs as u64 + 1;
        let bucket_row = row as u32 % settings.bucket_max_docs as u32;
        locators.push((bucket, bucket_row));
        db.put(&mut *wtxn, &id_key(hash), &encode_locators(&locators))?;
    }
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
    fn float_values_roundtrip_bit_exactly() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let coll = store
            .create_columnar_collection(
                "docs",
                &Settings {
                    bucket_max_docs: 7,
                    ..Default::default()
                },
            )
            .unwrap();
        for (id, f) in [
            ("a", 69.0f64),
            ("b", 76.14285714285714),
            ("c", 5.142857142857143),
            ("d", 63.142857142857146),
            ("e", 107.42857142857143),
            ("f", 1.5),
            ("g", 2.5),
        ] {
            coll.insert_one(json!({"_id": id, "f": f})).unwrap();
        }
        assert_eq!(
            coll.find_by_key("e").unwrap().unwrap()["f"]
                .as_f64()
                .unwrap()
                .to_bits(),
            107.42857142857143f64.to_bits()
        );
    }

    #[test]
    fn malformed_bucket_payloads_never_panic() {
        let mut state = 0x1234_5678_9abc_def0u64;
        let mut next = || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state
        };
        for case in 0..5_000 {
            let len = (next() % 300) as usize;
            let mut payload = Vec::with_capacity(len + 5);
            payload.extend_from_slice(MAGIC);
            payload.push(FORMAT_VERSION);
            for _ in 0..len {
                payload.push((next() & 0xff) as u8);
            }
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| decode_bucket(&payload)));
            assert!(result.is_ok(), "decoder panicked on case {case}");
        }

        // Mutate a valid bucket so the fuzz reaches column and block decoders.
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let settings = Settings {
            bucket_max_docs: 4,
            ..Default::default()
        };
        let coll = store.create_columnar_collection("docs", &settings).unwrap();
        for index in 0..4 {
            coll.insert_one(json!({
                "_id": format!("d{index}"),
                "n": index,
                "f": index as f64 / 3.0,
                "s": "text",
                "arr": [1, 2, 3],
            }))
            .unwrap();
        }
        let wtxn = store.read_txn().unwrap();
        let db = store
            .env()
            .open_database::<Bytes, Bytes>(&wtxn, Some(&db_name("docs")))
            .unwrap()
            .unwrap();
        let valid = db.get(&wtxn, &bucket_key(1)).unwrap().unwrap().to_vec();
        drop(wtxn);
        for _ in 0..2_000 {
            let mut payload = valid.clone();
            let mutations = 1 + (next() % 4) as usize;
            for _ in 0..mutations {
                let index = 5 + (next() as usize % (payload.len() - 5));
                payload[index] ^= 1 << (next() % 8);
            }
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| decode_bucket(&payload)));
            assert!(result.is_ok(), "decoder panicked on mutated bucket");
        }
    }

    #[test]
    fn unknown_bucket_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let settings = Settings {
            bucket_max_docs: 1,
            ..Default::default()
        };
        let coll = store.create_columnar_collection("docs", &settings).unwrap();
        coll.insert_one(json!({"_id": "a", "value": 1})).unwrap();
        coll.insert_one(json!({"_id": "b", "value": 2})).unwrap();
        store.compact_columnar("docs").unwrap();
        let mut wtxn = store.write_txn().unwrap();
        let db = store
            .env()
            .open_database::<Bytes, Bytes>(&wtxn, Some(&db_name("docs")))
            .unwrap()
            .unwrap();
        let mut payload = db.get(&wtxn, &bucket_key(1)).unwrap().unwrap().to_vec();
        assert_eq!(&payload[..4], MAGIC);
        payload[4] = FORMAT_VERSION + 1;
        db.put(&mut wtxn, &bucket_key(1), &payload).unwrap();
        wtxn.commit().unwrap();
        let error = coll.find_by_key("a").unwrap_err();
        assert!(error.to_string().contains("bad header"), "{error}");
        let error = coll.find(&FindQuery::default()).unwrap_err();
        assert!(error.to_string().contains("bad header"), "{error}");
        let mut wtxn = store.write_txn().unwrap();
        let error = compact(&mut wtxn, store.env(), "docs", &settings).unwrap_err();
        assert!(error.to_string().contains("bad header"), "{error}");
    }

    #[test]
    fn compaction_reencodes_with_current_version_and_settings() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let settings = Settings {
            bucket_max_docs: 2,
            ..Default::default()
        };
        let coll = store.create_columnar_collection("docs", &settings).unwrap();
        for index in 0..5 {
            coll.insert_one(json!({"_id": format!("doc-{index}"), "value": index}))
                .unwrap();
        }
        coll.update(
            &json!({"_id": "doc-1"}),
            &json!({"$set": {"value": 100}}),
            false,
        )
        .unwrap();
        coll.delete(&json!({"_id": "doc-2"}), false).unwrap();
        let changed = Settings {
            bucket_max_docs: 3,
            bucket_max_bytes: 128 * 1024,
            ..Default::default()
        };
        store.set_columnar_settings("docs", &changed).unwrap();
        assert_eq!(store.compact_columnar("docs").unwrap(), 4);
        assert_eq!(coll.count(&json!({})).unwrap(), 4);
        assert_eq!(coll.find_by_key("doc-1").unwrap().unwrap()["value"], 100);
        assert!(coll.find_by_key("doc-2").unwrap().is_none());
        let wtxn = store.write_txn().unwrap();
        let db = store
            .env()
            .open_database::<Bytes, Bytes>(&wtxn, Some(&db_name("docs")))
            .unwrap()
            .unwrap();
        let mut sealed = 0;
        for item in db.iter(&wtxn).unwrap() {
            let (key, value) = item.unwrap();
            if key.first() != Some(&KEY_BUCKET) {
                continue;
            }
            sealed += 1;
            assert_eq!(&value[..4], MAGIC);
            assert_eq!(value[4], FORMAT_VERSION);
        }
        assert!(sealed > 0);
        drop(wtxn);
        assert_eq!(store.columnar_settings("docs").unwrap(), Some(changed));
        drop(coll);
        drop(store);
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.columnar_stats("docs").unwrap().unwrap().rows, 4);
    }

    #[test]
    fn invalid_settings_leave_previous_settings_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let initial = Settings {
            bucket_max_docs: 64,
            ..Default::default()
        };
        store.create_columnar_collection("docs", &initial).unwrap();
        let invalid = Settings {
            bucket_max_docs: HARD_MAX_COLUMN_ROWS + 1,
            ..Default::default()
        };
        assert!(store.set_columnar_settings("docs", &invalid).is_err());
        assert_eq!(store.columnar_settings("docs").unwrap(), Some(initial));
        assert!(store
            .set_columnar_settings("plain", &Settings::default())
            .is_err());
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
