//! Time-series collections: bucketed, columnar, compressed storage.
//!
//! A time-series collection stores documents in *buckets* instead of one LMDB
//! entry per document. Each bucket covers one series (`metaField` value) and one
//! time window, and groups rows by column. Every column is encoded with the
//! cheapest of several codecs (delta, delta-of-delta, Gorilla XOR, dictionary,
//! frame-of-reference bit packing, byte shuffle) and optionally deflated.
//!
//! Layout in the dedicated `tsb:<collection>` sub-database:
//!
//! ```text
//! 0x00 | meta_hash(8) | salt(4)                 -> open bucket key
//! 0x01 | meta_hash(8) | salt(4) | window(8) | serial(2) -> bucket payload
//! ```
//!
//! The meta hash is a stable FNV-1a 64-bit hash of the canonical JSON of the
//! series value. The salt disambiguates (extremely unlikely) hash collisions and
//! keeps the format independent of the Rust hasher version.
//!
//! Bucket payload (version 1):
//!
//! ```text
//! "NTB1" | version(1) | flags(1) | n_rows(varint) | start(varint) | end(varint)
//! | meta_len(varint) | meta_json | col_count(varint) | column*
//!
//! column: field_len(varint) | field | kind(1) | codec(1) | flags(1)
//! | n(varint) | [null_bitmap] | payload_len(varint) | payload
//! ```
//!
//! `codec` has `0x80` set when the payload is zlib-deflated after the base
//! encoding. `flags` bit 0 means a null bitmap follows (bit set = present).

use std::collections::BTreeMap;
use std::io::{Read, Write};

use std::borrow::Cow;
use std::collections::BTreeSet;

use heed::types::Bytes;
use heed::{Database, RoTxn, RwTxn};
use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

use crate::collection::{compare_docs, project_document, FindQuery};
use crate::date;
use crate::error::{Error, Result};
use crate::query::matcher;
use crate::query::validate::validate_filter;

/// Prefix of the metadata key that stores time-series settings.
pub const SETTINGS_META_PREFIX: &str = "ts:";
/// Prefix of the dedicated LMDB sub-database that stores buckets.
pub const BUCKET_DB_PREFIX: &str = "tsb:";

const MAGIC: &[u8; 4] = b"NTB2";
const MAGIC_V1: &[u8; 4] = b"NTB1";
const FORMAT_VERSION: u8 = 3;
const FORMAT_VERSION_PREV: u8 = 2;
const FORMAT_VERSION_LEGACY: u8 = 1;
const BLOOM_BYTES: usize = 256;
const BLOOM_HASHES: u8 = 3;
const BLOCK_ROWS: usize = 256;
const KEY_OPEN: u8 = 0x00;
const KEY_BUCKET: u8 = 0x01;
const KEY_ROW: u8 = 0x02;
const KEY_TOMB: u8 = 0x03;
const KEY_USER: u8 = 0x05;
const KEY_INDEX_MARKER: u8 = 0x0F;
const KEY_INDEX_SERIES: u8 = 0x10;
const KEY_INDEX_TIME: u8 = 0x11;
const FLAG_SEALED: u8 = 0x01;

/// Default bucket width per granularity, in milliseconds.
const SPAN_SECONDS_MS: i64 = 3_600_000;
const SPAN_MINUTES_MS: i64 = 86_400_000;
const SPAN_HOURS_MS: i64 = 2_592_000_000;

const DEFAULT_BUCKET_MAX_DOCS: usize = 2_048;
const DEFAULT_BUCKET_MAX_BYTES: usize = 512 * 1024;
const HARD_BUCKET_MAX_DOCS: usize = 16_384;
const HARD_BUCKET_MAX_BYTES: usize = 4 * 1024 * 1024;
const MAX_SERIES_SALT: u32 = 64;

// Column kinds.
const KIND_I64: u8 = 0;
const KIND_F64: u8 = 1;
const KIND_BOOL: u8 = 2;
const KIND_STR: u8 = 3;
const KIND_JSON: u8 = 4;

// Column codecs (base encoding).
const CODEC_RAW: u8 = 0;
const CODEC_SHUFFLE: u8 = 1;
const CODEC_DELTA: u8 = 2;
const CODEC_DELTA_DELTA: u8 = 3;
const CODEC_XOR: u8 = 4;
const CODEC_DICT: u8 = 5;
const CODEC_BOOL_BITS: u8 = 6;
const CODEC_FOR: u8 = 7;
const CODEC_DEFLATE: u8 = 0x80;

/// Bucket width preset, mirroring MongoDB time-series granularity names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Granularity {
    /// One-hour buckets; best for high-frequency IoT samples.
    #[default]
    Seconds,
    /// One-day buckets.
    Minutes,
    /// Thirty-day buckets.
    Hours,
}

impl Granularity {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "seconds" => Ok(Self::Seconds),
            "minutes" => Ok(Self::Minutes),
            "hours" => Ok(Self::Hours),
            other => Err(Error::Query(format!(
                "unknown time-series granularity `{other}` (use seconds, minutes, or hours)"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Seconds => "seconds",
            Self::Minutes => "minutes",
            Self::Hours => "hours",
        }
    }

    pub fn span_ms(self) -> i64 {
        match self {
            Self::Seconds => SPAN_SECONDS_MS,
            Self::Minutes => SPAN_MINUTES_MS,
            Self::Hours => SPAN_HOURS_MS,
        }
    }
}

/// Compression effort. All levels are adaptive: they try the same codec set and
/// keep the smallest result, but higher levels spend more CPU on deflate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    /// Deflate level 1; lowest CPU.
    Fast,
    /// Deflate level 3; default.
    #[default]
    Balanced,
    /// Deflate level 6; smallest payloads.
    Max,
}

impl Compression {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "balanced" => Ok(Self::Balanced),
            "fast" => Ok(Self::Fast),
            "max" => Ok(Self::Max),
            other => Err(Error::Query(format!(
                "unknown time-series compression `{other}` (use fast, balanced, or max)"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fast => "fast",
            Self::Balanced => "balanced",
            Self::Max => "max",
        }
    }

    fn level(self) -> u32 {
        match self {
            Self::Fast => 1,
            Self::Balanced => 3,
            Self::Max => 6,
        }
    }
}

/// Time-series settings stored per collection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Settings {
    /// Field holding the event time.
    #[serde(alias = "timeField")]
    pub time_field: String,
    /// Optional field that groups rows into series. Stored once per bucket.
    #[serde(default, alias = "metaField", skip_serializing_if = "Option::is_none")]
    pub meta_field: Option<String>,
    #[serde(default)]
    pub granularity: Granularity,
    #[serde(default = "default_bucket_max_docs", alias = "bucketMaxDocs")]
    pub bucket_max_docs: usize,
    #[serde(default = "default_bucket_max_bytes", alias = "bucketMaxBytes")]
    pub bucket_max_bytes: usize,
    #[serde(default)]
    pub compression: Compression,
    /// Optional read-time expiry in seconds, relative to the event time.
    #[serde(
        default,
        alias = "expireAfterSeconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub expire_after_seconds: Option<u64>,
}

fn default_bucket_max_docs() -> usize {
    DEFAULT_BUCKET_MAX_DOCS
}

fn default_bucket_max_bytes() -> usize {
    DEFAULT_BUCKET_MAX_BYTES
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            time_field: "ts".to_string(),
            meta_field: None,
            granularity: Granularity::default(),
            bucket_max_docs: DEFAULT_BUCKET_MAX_DOCS,
            bucket_max_bytes: DEFAULT_BUCKET_MAX_BYTES,
            compression: Compression::default(),
            expire_after_seconds: None,
        }
    }
}

impl Settings {
    pub fn new(time_field: &str) -> Self {
        Self {
            time_field: time_field.to_string(),
            ..Self::default()
        }
    }

    pub fn validate(&self) -> Result<()> {
        let time_field = self.time_field.trim();
        if time_field.is_empty() {
            return Err(Error::Query(
                "time-series `timeField` must be a non-empty field name".into(),
            ));
        }
        if time_field == "_id" {
            return Err(Error::Query(
                "time-series `timeField` cannot be `_id`".into(),
            ));
        }
        if let Some(meta) = &self.meta_field {
            let meta = meta.trim();
            if meta.is_empty() {
                return Err(Error::Query(
                    "time-series `metaField` must be a non-empty field name".into(),
                ));
            }
            if meta == time_field {
                return Err(Error::Query(
                    "time-series `metaField` must differ from `timeField`".into(),
                ));
            }
        }
        if self.bucket_max_docs == 0 || self.bucket_max_docs > HARD_BUCKET_MAX_DOCS {
            return Err(Error::Query(format!(
                "time-series `bucketMaxDocs` must be between 1 and {HARD_BUCKET_MAX_DOCS}"
            )));
        }
        if self.bucket_max_bytes < 1024 || self.bucket_max_bytes > HARD_BUCKET_MAX_BYTES {
            return Err(Error::Query(format!(
                "time-series `bucketMaxBytes` must be between 1024 and {HARD_BUCKET_MAX_BYTES}"
            )));
        }
        Ok(())
    }

    pub fn span_ms(&self) -> i64 {
        self.granularity.span_ms()
    }
}

/// Aggregated storage information for one time-series collection.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Stats {
    pub buckets: u64,
    pub sealed_buckets: u64,
    pub open_buckets: u64,
    pub series: u64,
    pub rows: u64,
    /// Bytes of the original documents, without any encoding.
    pub raw_bytes: u64,
    /// Bytes of sealed columnar buckets.
    pub encoded_bytes: u64,
    /// Bytes of unsealed rows, which are stored as raw JSON until the bucket
    /// is sealed (bounded by `bucket_max_docs` per series).
    pub open_bytes: u64,
    /// Rows marked deleted by tombstones in sealed buckets.
    pub deleted_rows: u64,
}

impl Stats {
    /// Sealed payloads plus pending raw rows.
    pub fn total_bytes(&self) -> u64 {
        self.encoded_bytes.saturating_add(self.open_bytes)
    }
}

impl Stats {
    pub fn compression_ratio(&self) -> f64 {
        let total = self.total_bytes();
        if total == 0 {
            return 0.0;
        }
        self.raw_bytes as f64 / total as f64
    }
}

// ---------------------------------------------------------------------------
// Varint and bit helpers
// ---------------------------------------------------------------------------

fn corrupt(what: &str) -> Error {
    Error::Query(format!("corrupt time-series bucket: {what}"))
}

fn put_uvarint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn get_uvarint(data: &[u8], pos: &mut usize) -> Result<u64> {
    let mut out = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *data.get(*pos).ok_or_else(|| corrupt("truncated varint"))?;
        *pos += 1;
        if shift >= 64 || (shift == 63 && byte > 1) {
            return Err(corrupt("varint overflow"));
        }
        out |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(out);
        }
        shift += 7;
    }
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn unzigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

fn put_ivarint(out: &mut Vec<u8>, value: i64) {
    put_uvarint(out, zigzag(value));
}

fn get_ivarint(data: &[u8], pos: &mut usize) -> Result<i64> {
    Ok(unzigzag(get_uvarint(data, pos)?))
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_uvarint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn get_bytes<'a>(data: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
    let len = get_uvarint(data, pos)? as usize;
    let end = pos
        .checked_add(len)
        .filter(|end| *end <= data.len())
        .ok_or_else(|| corrupt("truncated byte string"))?;
    let out = &data[*pos..end];
    *pos = end;
    Ok(out)
}

struct BitWriter {
    out: Vec<u8>,
    cur: u8,
    bits: u8,
}

impl BitWriter {
    fn new() -> Self {
        Self {
            out: Vec::new(),
            cur: 0,
            bits: 0,
        }
    }

    fn write(&mut self, value: u64, count: u8) {
        for shift in (0..count).rev() {
            let bit = ((value >> shift) & 1) as u8;
            self.cur = (self.cur << 1) | bit;
            self.bits += 1;
            if self.bits == 8 {
                self.out.push(self.cur);
                self.cur = 0;
                self.bits = 0;
            }
        }
    }

    fn finish(mut self) -> Vec<u8> {
        if self.bits > 0 {
            self.cur <<= 8 - self.bits;
            self.out.push(self.cur);
        }
        self.out
    }
}

struct BitReader<'a> {
    data: &'a [u8],
    pos: usize,
    bit: u8,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            bit: 0,
        }
    }

    fn read(&mut self, count: u8) -> Result<u64> {
        let mut out = 0u64;
        for _ in 0..count {
            let byte = *self
                .data
                .get(self.pos)
                .ok_or_else(|| corrupt("truncated bit stream"))?;
            let bit = (byte >> (7 - self.bit)) & 1;
            out = (out << 1) | u64::from(bit);
            self.bit += 1;
            if self.bit == 8 {
                self.bit = 0;
                self.pos += 1;
            }
        }
        Ok(out)
    }
}

fn bits_needed(max: u64) -> u8 {
    if max == 0 {
        0
    } else {
        (64 - max.leading_zeros()) as u8
    }
}

fn deflate(data: &[u8], level: u32) -> Result<Vec<u8>> {
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::new(level));
    encoder.write_all(data)?;
    Ok(encoder.finish()?)
}

fn inflate(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = flate2::read::ZlibDecoder::new(data);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|err| Error::Query(format!("corrupt time-series bucket: {err}")))?;
    Ok(out)
}

/// Picks the smallest candidate and then tries deflate on top of it.
fn pick_codec(candidates: Vec<(u8, Vec<u8>)>, compression: Compression) -> Result<(u8, Vec<u8>)> {
    let mut best = candidates
        .into_iter()
        .min_by_key(|(_, payload)| payload.len())
        .ok_or_else(|| corrupt("no column codec candidate"))?;
    if best.1.len() > 48 {
        let packed = deflate(&best.1, compression.level())?;
        if packed.len() + 2 < best.1.len() {
            best = (best.0 | CODEC_DEFLATE, packed);
        }
    }
    Ok(best)
}

// ---------------------------------------------------------------------------
// Column encoders
// ---------------------------------------------------------------------------

fn encode_shuffle(values: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 8);
    for byte in 0..8 {
        for value in values {
            out.push((value >> (byte * 8)) as u8);
        }
    }
    out
}

fn decode_shuffle(data: &[u8], n: usize) -> Result<Vec<u64>> {
    if data.len() != n * 8 {
        return Err(corrupt("shuffled column length mismatch"));
    }
    let mut out = vec![0u64; n];
    for byte in 0..8 {
        for (idx, slot) in out.iter_mut().enumerate() {
            *slot |= u64::from(data[byte * n + idx]) << (byte * 8);
        }
    }
    Ok(out)
}

fn encode_delta(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 2);
    let mut prev = 0i64;
    for value in values {
        put_ivarint(&mut out, value.wrapping_sub(prev));
        prev = *value;
    }
    out
}

fn decode_delta(data: &[u8], n: usize) -> Result<Vec<i64>> {
    let mut out = Vec::with_capacity(n);
    let mut pos = 0;
    let mut prev = 0i64;
    for _ in 0..n {
        prev = prev.wrapping_add(get_ivarint(data, &mut pos)?);
        out.push(prev);
    }
    Ok(out)
}

fn encode_delta_delta(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 2);
    let mut prev = 0i64;
    let mut prev_delta = 0i64;
    for value in values {
        let delta = value.wrapping_sub(prev);
        put_ivarint(&mut out, delta.wrapping_sub(prev_delta));
        prev = *value;
        prev_delta = delta;
    }
    out
}

fn decode_delta_delta(data: &[u8], n: usize) -> Result<Vec<i64>> {
    let mut out = Vec::with_capacity(n);
    let mut pos = 0;
    let mut prev = 0i64;
    let mut prev_delta = 0i64;
    for _ in 0..n {
        let delta = prev_delta.wrapping_add(get_ivarint(data, &mut pos)?);
        prev = prev.wrapping_add(delta);
        out.push(prev);
        prev_delta = delta;
    }
    Ok(out)
}

fn encode_for(values: &[i64]) -> Vec<u8> {
    let min = values.iter().copied().min().unwrap_or(0);
    let max = values.iter().copied().max().unwrap_or(0);
    let width = bits_needed((max as i128 - min as i128) as u64);
    let mut out = Vec::new();
    put_ivarint(&mut out, min);
    out.push(width);
    if width > 0 {
        let mut writer = BitWriter::new();
        for value in values {
            writer.write((*value as i128 - min as i128) as u64, width);
        }
        out.extend_from_slice(&writer.finish());
    }
    out
}

fn decode_for(data: &[u8], n: usize) -> Result<Vec<i64>> {
    let mut pos = 0;
    let min = get_ivarint(data, &mut pos)?;
    let width = *data.get(pos).ok_or_else(|| corrupt("missing FOR width"))?;
    pos += 1;
    if width > 64 {
        return Err(corrupt("invalid FOR width"));
    }
    let mut out = Vec::with_capacity(n);
    if width == 0 {
        out.resize(n, min);
        return Ok(out);
    }
    let mut reader = BitReader::new(&data[pos..]);
    for _ in 0..n {
        let offset = reader.read(width)?;
        out.push(((min as i128) + offset as i128) as i64);
    }
    Ok(out)
}

fn encode_xor(values: &[f64]) -> Vec<u8> {
    let mut writer = BitWriter::new();
    let mut prev = 0u64;
    for (idx, value) in values.iter().enumerate() {
        let bits = value.to_bits();
        if idx == 0 {
            writer.write(bits, 64);
        } else {
            let xor = prev ^ bits;
            if xor == 0 {
                writer.write(0, 1);
            } else {
                writer.write(1, 1);
                let leading = xor.leading_zeros().min(63) as u8;
                let trailing = xor.trailing_zeros();
                let meaningful = 64 - leading - trailing as u8;
                writer.write(u64::from(leading), 6);
                writer.write(u64::from(meaningful - 1), 6);
                writer.write(xor >> trailing, meaningful);
            }
        }
        prev = bits;
    }
    writer.finish()
}

fn decode_xor(data: &[u8], n: usize) -> Result<Vec<f64>> {
    let mut reader = BitReader::new(data);
    let mut out = Vec::with_capacity(n);
    let mut prev = 0u64;
    for idx in 0..n {
        let bits = if idx == 0 {
            reader.read(64)?
        } else if reader.read(1)? == 0 {
            prev
        } else {
            let leading = reader.read(6)? as u8;
            let meaningful = reader.read(6)? as u8 + 1;
            if u32::from(leading) + u32::from(meaningful) > 64 {
                return Err(corrupt("invalid XOR window"));
            }
            let trailing = 64 - leading - meaningful;
            let payload = reader.read(meaningful)?;
            prev ^ (payload << trailing)
        };
        out.push(f64::from_bits(bits));
        prev = bits;
    }
    Ok(out)
}

fn encode_dict(values: &[String]) -> Vec<u8> {
    let mut dict: Vec<&str> = Vec::new();
    let mut index: BTreeMap<&str, u32> = BTreeMap::new();
    let mut indices = Vec::with_capacity(values.len());
    for value in values {
        let key = value.as_str();
        let slot = *index.entry(key).or_insert_with(|| {
            let slot = dict.len() as u32;
            dict.push(key);
            slot
        });
        indices.push(slot);
    }
    let mut out = Vec::new();
    put_uvarint(&mut out, dict.len() as u64);
    for entry in &dict {
        put_bytes(&mut out, entry.as_bytes());
    }
    if dict.len() <= 1 {
        return out;
    }
    let width = bits_needed(dict.len() as u64 - 1);
    out.push(width);
    let mut writer = BitWriter::new();
    for slot in indices {
        writer.write(u64::from(slot), width);
    }
    out.extend_from_slice(&writer.finish());
    out
}

fn decode_dict(data: &[u8], n: usize) -> Result<Vec<String>> {
    let mut pos = 0;
    let count = get_uvarint(data, &mut pos)? as usize;
    let mut dict = Vec::with_capacity(count);
    for _ in 0..count {
        let raw = get_bytes(data, &mut pos)?;
        dict.push(
            std::str::from_utf8(raw)
                .map_err(|_| corrupt("dictionary entry is not UTF-8"))?
                .to_string(),
        );
    }
    if count == 0 {
        return Ok(Vec::new());
    }
    if count == 1 {
        return Ok(vec![dict[0].clone(); n]);
    }
    let width = *data
        .get(pos)
        .ok_or_else(|| corrupt("missing dictionary width"))?;
    pos += 1;
    let mut reader = BitReader::new(&data[pos..]);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let slot = reader.read(width)? as usize;
        out.push(
            dict.get(slot)
                .cloned()
                .ok_or_else(|| corrupt("dictionary index out of range"))?,
        );
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Columns
// ---------------------------------------------------------------------------

struct Column {
    field: String,
    values: Vec<Option<Value>>,
}

fn classify(values: &[Option<Value>]) -> u8 {
    let mut kind = None;
    for value in values.iter().flatten() {
        let current = match value {
            Value::Bool(_) => KIND_BOOL,
            Value::String(_) => KIND_STR,
            Value::Number(number) => {
                if number.as_i64().is_some()
                    || number.as_u64().is_some_and(|v| v <= i64::MAX as u64)
                {
                    KIND_I64
                } else {
                    KIND_F64
                }
            }
            _ => KIND_JSON,
        };
        kind = Some(match (kind, current) {
            (None, current) => current,
            (Some(KIND_I64), KIND_F64) | (Some(KIND_F64), KIND_I64) => KIND_F64,
            (Some(existing), current) if existing == current => existing,
            _ => KIND_JSON,
        });
    }
    kind.unwrap_or(KIND_JSON)
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_u64().map(|v| v as i64)),
        _ => None,
    }
}

fn encode_column(column: &Column, compression: Compression) -> Result<Vec<u8>> {
    let kind = classify(&column.values);
    let n = column.values.len();
    let present: Vec<&Value> = column.values.iter().flatten().collect();
    let has_nulls = present.len() != n;

    let (codec, payload) = match kind {
        KIND_I64 => {
            let mut numbers = Vec::with_capacity(present.len());
            for value in &present {
                match value_as_i64(value) {
                    Some(number) => numbers.push(number),
                    None => return Err(corrupt("non-integer in integer column")),
                }
            }
            let raw: Vec<u8> = numbers.iter().flat_map(|v| v.to_le_bytes()).collect();
            let shuffled = encode_shuffle(&numbers.iter().map(|v| *v as u64).collect::<Vec<_>>());
            let candidates = vec![
                (CODEC_RAW, raw),
                (CODEC_SHUFFLE, shuffled),
                (CODEC_DELTA, encode_delta(&numbers)),
                (CODEC_DELTA_DELTA, encode_delta_delta(&numbers)),
                (CODEC_FOR, encode_for(&numbers)),
            ];
            pick_codec(candidates, compression)?
        }
        KIND_F64 => {
            let numbers: Vec<f64> = present.iter().filter_map(|value| value.as_f64()).collect();
            if numbers.len() != present.len() {
                return Err(corrupt("non-numeric value in float column"));
            }
            let raw: Vec<u8> = numbers
                .iter()
                .flat_map(|v| v.to_bits().to_le_bytes())
                .collect();
            let shuffled = encode_shuffle(&numbers.iter().map(|v| v.to_bits()).collect::<Vec<_>>());
            let candidates = vec![
                (CODEC_RAW, raw),
                (CODEC_SHUFFLE, shuffled),
                (CODEC_XOR, encode_xor(&numbers)),
            ];
            pick_codec(candidates, compression)?
        }
        KIND_BOOL => {
            let mut writer = BitWriter::new();
            for value in &present {
                writer.write(u64::from(value.as_bool().unwrap_or(false)), 1);
            }
            (CODEC_BOOL_BITS, writer.finish())
        }
        KIND_STR => {
            let strings: Vec<String> = present
                .iter()
                .filter_map(|value| value.as_str().map(str::to_string))
                .collect();
            if strings.len() != present.len() {
                return Err(corrupt("non-string value in string column"));
            }
            let mut raw = Vec::new();
            for value in &strings {
                put_bytes(&mut raw, value.as_bytes());
            }
            let candidates = vec![(CODEC_RAW, raw), (CODEC_DICT, encode_dict(&strings))];
            pick_codec(candidates, compression)?
        }
        _ => {
            let mut raw = Vec::new();
            for value in &present {
                put_bytes(&mut raw, &serde_json::to_vec(value)?);
            }
            pick_codec(vec![(CODEC_RAW, raw)], compression)?
        }
    };

    let mut out = Vec::with_capacity(payload.len() + column.field.len() + 16);
    put_bytes(&mut out, column.field.as_bytes());
    out.push(kind);
    out.push(codec);
    out.push(u8::from(has_nulls));
    put_uvarint(&mut out, n as u64);
    if has_nulls {
        let mut writer = BitWriter::new();
        for value in &column.values {
            writer.write(u64::from(value.is_some()), 1);
        }
        out.extend_from_slice(&writer.finish());
    }
    put_bytes(&mut out, &payload);
    Ok(out)
}

fn decode_column(data: &[u8], pos: &mut usize) -> Result<Column> {
    let field = std::str::from_utf8(get_bytes(data, pos)?)
        .map_err(|_| corrupt("field name is not UTF-8"))?
        .to_string();
    let kind = *data
        .get(*pos)
        .ok_or_else(|| corrupt("missing column kind"))?;
    let codec = *data
        .get(*pos + 1)
        .ok_or_else(|| corrupt("missing column codec"))?;
    let flags = *data
        .get(*pos + 2)
        .ok_or_else(|| corrupt("missing column flags"))?;
    *pos += 3;
    let n = get_uvarint(data, pos)? as usize;
    if n > HARD_BUCKET_MAX_DOCS {
        return Err(corrupt("column row count out of range"));
    }
    let mut present = vec![true; n];
    if flags & 1 != 0 {
        let mut reader = BitReader::new(
            data.get(*pos..)
                .ok_or_else(|| corrupt("missing null bitmap"))?,
        );
        for slot in present.iter_mut() {
            *slot = reader.read(1)? == 1;
        }
        *pos += n.div_ceil(8);
    }
    let raw = get_bytes(data, pos)?;
    let deflated = codec & CODEC_DEFLATE != 0;
    let base = codec & !CODEC_DEFLATE;
    let owned;
    let payload: &[u8] = if deflated {
        owned = inflate(raw)?;
        &owned
    } else {
        raw
    };
    let count = present.iter().filter(|value| **value).count();

    let mut decoded: Vec<Option<Value>> = Vec::with_capacity(n);
    match base {
        CODEC_RAW | CODEC_SHUFFLE => {
            if kind == KIND_I64 {
                let numbers = if base == CODEC_SHUFFLE {
                    decode_shuffle(payload, count)?
                } else {
                    if payload.len() != count * 8 {
                        return Err(corrupt("integer column length mismatch"));
                    }
                    payload
                        .as_chunks::<8>()
                        .0
                        .iter()
                        .map(|chunk| u64::from_le_bytes(*chunk))
                        .collect()
                };
                decoded.extend(numbers.into_iter().map(|v| Some(Value::from(v as i64))));
            } else if kind == KIND_F64 {
                let numbers = if base == CODEC_SHUFFLE {
                    decode_shuffle(payload, count)?
                } else {
                    if payload.len() != count * 8 {
                        return Err(corrupt("float column length mismatch"));
                    }
                    payload
                        .as_chunks::<8>()
                        .0
                        .iter()
                        .map(|chunk| u64::from_le_bytes(*chunk))
                        .collect()
                };
                decoded.extend(
                    numbers.into_iter().map(|bits| {
                        match serde_json::Number::from_f64(f64::from_bits(bits)) {
                            Some(number) => Some(Value::Number(number)),
                            None => Some(Value::Null),
                        }
                    }),
                );
            } else if kind == KIND_STR || kind == KIND_JSON {
                let mut reader = 0;
                for _ in 0..count {
                    let bytes = get_bytes(payload, &mut reader)?;
                    if kind == KIND_STR {
                        decoded.push(Some(Value::String(
                            std::str::from_utf8(bytes)
                                .map_err(|_| corrupt("string column is not UTF-8"))?
                                .to_string(),
                        )));
                    } else {
                        decoded.push(Some(serde_json::from_slice(bytes)?));
                    }
                }
            } else if kind == KIND_BOOL {
                let mut reader = BitReader::new(payload);
                for _ in 0..count {
                    decoded.push(Some(Value::Bool(reader.read(1)? == 1)));
                }
            } else {
                return Err(corrupt("unsupported column kind"));
            }
        }
        CODEC_DELTA | CODEC_DELTA_DELTA | CODEC_FOR => {
            let numbers = match base {
                CODEC_DELTA => decode_delta(payload, count)?,
                CODEC_DELTA_DELTA => decode_delta_delta(payload, count)?,
                _ => decode_for(payload, count)?,
            };
            decoded.extend(numbers.into_iter().map(|v| Some(Value::from(v))));
        }
        CODEC_XOR => {
            decoded.extend(decode_xor(payload, count)?.into_iter().map(|value| {
                match serde_json::Number::from_f64(value) {
                    Some(number) => Some(Value::Number(number)),
                    None => Some(Value::Null),
                }
            }));
        }
        CODEC_DICT => {
            decoded.extend(
                decode_dict(payload, count)?
                    .into_iter()
                    .map(|v| Some(Value::String(v))),
            );
        }
        CODEC_BOOL_BITS => {
            let mut reader = BitReader::new(payload);
            for _ in 0..count {
                decoded.push(Some(Value::Bool(reader.read(1)? == 1)));
            }
        }
        _ => return Err(corrupt("unknown column codec")),
    }

    if has_nulls_mask(&present) {
        let mut values = decoded.into_iter();
        let mut merged = Vec::with_capacity(n);
        for is_present in present {
            merged.push(if is_present {
                values.next().flatten()
            } else {
                None
            });
        }
        Ok(Column {
            field,
            values: merged,
        })
    } else {
        Ok(Column {
            field,
            values: decoded,
        })
    }
}

fn has_nulls_mask(present: &[bool]) -> bool {
    present.iter().any(|value| !*value)
}

// ---------------------------------------------------------------------------
// Buckets
// ---------------------------------------------------------------------------

struct Bucket {
    sealed: bool,
    window_start: i64,
    end: i64,
    meta: Option<Value>,
    rows: Vec<Map<String, Value>>,
    /// Row positions deleted by bitmap (new format). Old buckets keep 0x03 keys.
    deleted: BTreeSet<u32>,
}

fn valid_magic(data: &[u8]) -> bool {
    data.len() >= 6 && (&data[..4] == MAGIC || &data[..4] == MAGIC_V1)
}

fn has_bitmap(data: &[u8]) -> bool {
    data.len() >= 4 && &data[..4] == MAGIC
}

fn deleted_at(bucket: &Bucket, tombs: &BTreeSet<u32>, seq: u32) -> bool {
    tombs.contains(&seq) || bucket.deleted.contains(&seq)
}
/// Per-bucket min/max ranges and a compact Bloom filter for string equality.
#[derive(Clone, Default)]
struct ZoneMap {
    ranges: BTreeMap<String, (f64, f64)>,
    bloom: Vec<u8>,
}

impl ZoneMap {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(1u8);
        put_uvarint(&mut out, self.ranges.len() as u64);
        for (field, (min, max)) in &self.ranges {
            put_bytes(&mut out, field.as_bytes());
            out.extend_from_slice(&min.to_bits().to_be_bytes());
            out.extend_from_slice(&max.to_bits().to_be_bytes());
        }
        out.extend_from_slice(&self.bloom);
        out
    }

    fn decode(raw: &[u8]) -> Result<Self> {
        let mut pos = 0;
        let version = *raw.get(pos).ok_or_else(|| corrupt("empty zone map"))?;
        pos += 1;
        if version != 1 {
            return Err(corrupt("unsupported zone-map version"));
        }
        let count = get_uvarint(raw, &mut pos)? as usize;
        if count > 4096 {
            return Err(corrupt("zone-map column count out of range"));
        }
        let mut ranges = BTreeMap::new();
        for _ in 0..count {
            let field = std::str::from_utf8(get_bytes(raw, &mut pos)?)
                .map_err(|_| corrupt("zone-map field is not UTF-8"))?
                .to_string();
            let min_bytes: [u8; 8] = raw
                .get(pos..pos + 8)
                .ok_or_else(|| corrupt("malformed zone-map minimum"))?
                .try_into()
                .map_err(|_| corrupt("malformed zone-map minimum"))?;
            pos += 8;
            let max_bytes: [u8; 8] = raw
                .get(pos..pos + 8)
                .ok_or_else(|| corrupt("malformed zone-map maximum"))?
                .try_into()
                .map_err(|_| corrupt("malformed zone-map maximum"))?;
            pos += 8;
            let min = f64::from_bits(u64::from_be_bytes(min_bytes));
            let max = f64::from_bits(u64::from_be_bytes(max_bytes));
            ranges.insert(field, (min, max));
        }
        if raw.len().saturating_sub(pos) != BLOOM_BYTES {
            return Err(corrupt("malformed zone-map Bloom filter"));
        }
        Ok(Self {
            ranges,
            bloom: raw[pos..].to_vec(),
        })
    }

    fn allows(&self, filter: &Value, settings: &Settings) -> bool {
        fn bloom_key(field: &str, value: &str) -> Vec<u8> {
            let mut key = Vec::with_capacity(field.len() + value.len() + 1);
            key.extend_from_slice(field.as_bytes());
            key.push(0);
            key.extend_from_slice(value.as_bytes());
            key
        }
        fn bloom_contains(bloom: &[u8], key: &[u8]) -> bool {
            if bloom.len() != BLOOM_BYTES {
                return true;
            }
            let first = fnv1a64(key);
            let mut second = 0x9e37_79b9_7f4a_7c15u64;
            for byte in key {
                second ^= u64::from(*byte);
                second = second.wrapping_mul(0x0000_0100_0000_01b3);
            }
            (0..BLOOM_HASHES).all(|i| {
                let hash = first.wrapping_add(u64::from(i).wrapping_mul(second));
                let bit = hash as usize % (BLOOM_BYTES * 8);
                bloom[bit / 8] & (1 << (bit % 8)) != 0
            })
        }
        fn string_operand(value: &Value) -> Option<String> {
            match value {
                Value::String(text) => Some(text.clone()),
                Value::Bool(flag) => Some(flag.to_string()),
                Value::Object(object) => object.get("$eq").and_then(string_operand),
                _ => None,
            }
        }
        fn numeric_operand(value: &Value, is_time: bool) -> Option<f64> {
            if is_time {
                time_value_ms(value).map(|value| value as f64)
            } else {
                value.as_f64().or_else(|| {
                    value
                        .as_object()
                        .and_then(|object| object.get("$eq"))
                        .and_then(Value::as_f64)
                })
            }
        }
        fn range_allows(
            ranges: &BTreeMap<String, (f64, f64)>,
            field: &str,
            low: Option<f64>,
            high: Option<f64>,
        ) -> bool {
            let Some((min, max)) = ranges.get(field) else {
                return true;
            };
            !low.is_some_and(|low| *max < low) && !high.is_some_and(|high| *min > high)
        }
        fn check(value: &Value, settings: &Settings, zone: &ZoneMap) -> bool {
            let Some(object) = value.as_object() else {
                return true;
            };
            for (field, operand) in object {
                if field == "$and" {
                    if let Some(items) = operand.as_array() {
                        if !items.iter().all(|item| check(item, settings, zone)) {
                            return false;
                        }
                    }
                    continue;
                }
                if field.starts_with('$') || settings.meta_field.as_deref() == Some(field.as_str())
                {
                    continue;
                }
                let is_time = field == &settings.time_field;
                if let Some(conditions) = operand.as_object() {
                    let mut low = None;
                    let mut high = None;
                    let mut had_numeric = false;
                    for (op, value) in conditions {
                        let Some(number) = numeric_operand(value, is_time) else {
                            continue;
                        };
                        had_numeric = true;
                        match op.as_str() {
                            "$gte" => {
                                low = Some(low.map_or(number, |current: f64| current.max(number)))
                            }
                            "$gt" => {
                                low = Some(low.map_or(number, |current: f64| current.max(number)))
                            }
                            "$lte" => {
                                high = Some(high.map_or(number, |current: f64| current.min(number)))
                            }
                            "$lt" => {
                                high = Some(high.map_or(number, |current: f64| current.min(number)))
                            }
                            "$eq" => {
                                low = Some(number);
                                high = Some(number);
                            }
                            _ => {}
                        }
                    }
                    if had_numeric && !range_allows(&zone.ranges, field, low, high) {
                        return false;
                    }
                    if let Some(string) = string_operand(operand) {
                        if !bloom_contains(&zone.bloom, &bloom_key(field, &string)) {
                            return false;
                        }
                    }
                    if let Some(items) = conditions.get("$in").and_then(Value::as_array) {
                        let any = items
                            .iter()
                            .filter_map(string_operand)
                            .any(|string| bloom_contains(&zone.bloom, &bloom_key(field, &string)));
                        if !any {
                            return false;
                        }
                    }
                } else if let Some(string) = string_operand(operand) {
                    if !bloom_contains(&zone.bloom, &bloom_key(field, &string)) {
                        return false;
                    }
                } else if let Some(number) = numeric_operand(operand, is_time) {
                    if !range_allows(&zone.ranges, field, Some(number), Some(number)) {
                        return false;
                    }
                }
            }
            true
        }
        check(filter, settings, self)
    }
}

fn bloom_insert(bloom: &mut [u8], key: &[u8]) {
    if bloom.len() != BLOOM_BYTES {
        return;
    }
    let first = fnv1a64(key);
    let mut second = 0x9e37_79b9_7f4a_7c15u64;
    for byte in key {
        second ^= u64::from(*byte);
        second = second.wrapping_mul(0x0000_0100_0000_01b3);
    }
    for i in 0..BLOOM_HASHES {
        let hash = first.wrapping_add(u64::from(i).wrapping_mul(second));
        let bit = hash as usize % (BLOOM_BYTES * 8);
        bloom[bit / 8] |= 1 << (bit % 8);
    }
}

fn zone_number(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| {
        value
            .as_object()
            .and_then(|object| object.get("$date"))
            .and_then(|_| time_value_ms(value))
            .map(|value| value as f64)
    })
}

fn zone_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

fn zone_from_flat(rows: &[BTreeMap<String, Value>]) -> ZoneMap {
    let mut ranges: BTreeMap<String, (f64, f64)> = BTreeMap::new();
    let mut keys = BTreeSet::new();
    for row in rows {
        for (field, value) in row {
            if let Some(number) = zone_number(value) {
                ranges
                    .entry(field.clone())
                    .and_modify(|(min, max)| {
                        *min = min.min(number);
                        *max = max.max(number);
                    })
                    .or_insert((number, number));
            }
            if let Some(text) = zone_string(value) {
                let mut key = field.as_bytes().to_vec();
                key.push(0);
                key.extend_from_slice(text.as_bytes());
                keys.insert(key);
            }
        }
    }
    let mut bloom = vec![0u8; BLOOM_BYTES];
    for key in keys {
        bloom_insert(&mut bloom, &key);
    }
    ZoneMap { ranges, bloom }
}

fn skip_zone_map(data: &[u8], pos: &mut usize, version: u8) -> Result<()> {
    if version >= 3 {
        let _ = get_bytes(data, pos)?;
    }
    Ok(())
}

fn zone_filter_relevant(filter: &Value, settings: &Settings) -> bool {
    fn check(value: &Value, settings: &Settings) -> bool {
        let Some(object) = value.as_object() else {
            return false;
        };
        for (field, operand) in object {
            if field == "$and" {
                if operand
                    .as_array()
                    .is_some_and(|items| items.iter().any(|item| check(item, settings)))
                {
                    return true;
                }
            } else if !field.starts_with('$')
                && field != &settings.time_field
                && settings.meta_field.as_deref() != Some(field.as_str())
                && operand.as_object().is_none_or(|condition| {
                    condition
                        .keys()
                        .any(|key| matches!(key.as_str(), "$eq" | "$in"))
                        || condition
                            .keys()
                            .any(|key| matches!(key.as_str(), "$gte" | "$gt" | "$lte" | "$lt"))
                })
            {
                return true;
            }
        }
        false
    }
    check(filter, settings)
}

fn bucket_zone_allows(data: &[u8], settings: &Settings, filter: &Value) -> Result<bool> {
    if !valid_magic(data) || data[4] < 3 || !zone_filter_relevant(filter, settings) {
        return Ok(true);
    }
    let mut pos = 6;
    let _n = get_uvarint(data, &mut pos)?;
    let _start = get_ivarint(data, &mut pos)?;
    let _end = get_ivarint(data, &mut pos)?;
    let _meta = get_bytes(data, &mut pos)?;
    let raw = get_bytes(data, &mut pos)?;
    let zone = ZoneMap::decode(raw)?;
    Ok(zone.allows(filter, settings))
}

fn encode_deleted(bucket: &Bucket) -> Vec<u8> {
    let mut bytes = vec![0u8; bucket.rows.len().div_ceil(8)];
    for seq in &bucket.deleted {
        let idx = *seq as usize;
        if idx < bucket.rows.len() {
            bytes[idx / 8] |= 1 << (idx % 8);
        }
    }
    bytes
}

fn flatten(prefix: &str, value: &Value, out: &mut BTreeMap<String, Value>) {
    match value {
        Value::Object(map) if !map.is_empty() && !map.contains_key("$date") => {
            for (key, child) in map {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten(&path, child, out);
            }
        }
        other => {
            out.insert(prefix.to_string(), other.clone());
        }
    }
}

fn get_path<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    if path.is_empty() {
        return Some(doc);
    }
    let mut current = doc;
    for part in path.split('.') {
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

fn set_path_parts(doc: &mut Map<String, Value>, parts: &[&str], value: Value) {
    let mut current = doc;
    for part in &parts[..parts.len() - 1] {
        let entry = current
            .entry((*part).to_string())
            .or_insert_with(|| Value::Object(Map::new()));
        if !entry.is_object() {
            *entry = Value::Object(Map::new());
        }
        current = entry.as_object_mut().expect("object just inserted");
    }
    current.insert(parts[parts.len() - 1].to_string(), value);
}

fn set_path(doc: &mut Map<String, Value>, path: &str, value: Value) {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = doc;
    for part in &parts[..parts.len() - 1] {
        let entry = current
            .entry((*part).to_string())
            .or_insert_with(|| Value::Object(Map::new()));
        if !entry.is_object() {
            *entry = Value::Object(Map::new());
        }
        current = entry.as_object_mut().expect("object just inserted");
    }
    current.insert(parts[parts.len() - 1].to_string(), value);
}

/// Canonical JSON with sorted object keys, used only for series hashing.
fn canonical_json(value: &Value) -> String {
    fn write(value: &Value, out: &mut String) {
        match value {
            Value::Object(map) => {
                out.push('{');
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                for (idx, key) in keys.iter().enumerate() {
                    if idx > 0 {
                        out.push(',');
                    }
                    out.push_str(&serde_json::to_string(key).unwrap_or_default());
                    out.push(':');
                    write(&map[*key], out);
                }
                out.push('}');
            }
            Value::Array(items) => {
                out.push('[');
                for (idx, item) in items.iter().enumerate() {
                    if idx > 0 {
                        out.push(',');
                    }
                    write(item, out);
                }
                out.push(']');
            }
            other => out.push_str(&serde_json::to_string(other).unwrap_or_default()),
        }
    }
    let mut out = String::new();
    write(value, &mut out);
    out
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn open_key(hash: u64, salt: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(13);
    key.push(KEY_OPEN);
    key.extend_from_slice(&hash.to_be_bytes());
    key.extend_from_slice(&salt.to_be_bytes());
    key
}

fn bucket_key(hash: u64, salt: u32, window_start: i64, serial: u16) -> Vec<u8> {
    let mut key = Vec::with_capacity(23);
    key.push(KEY_BUCKET);
    key.extend_from_slice(&hash.to_be_bytes());
    key.extend_from_slice(&salt.to_be_bytes());
    key.extend_from_slice(&window_start.to_be_bytes());
    key.extend_from_slice(&serial.to_be_bytes());
    key
}

fn key_parts(key: &[u8]) -> Option<(u64, u32, i64, u16)> {
    if key.len() != 23 || key[0] != KEY_BUCKET {
        return None;
    }
    let hash = u64::from_be_bytes(key[1..9].try_into().ok()?);
    let salt = u32::from_be_bytes(key[9..13].try_into().ok()?);
    let window = i64::from_be_bytes(key[13..21].try_into().ok()?);
    let serial = u16::from_be_bytes(key[21..23].try_into().ok()?);
    Some((hash, salt, window, serial))
}

/// Physical location and sequence of one time-series row.
pub type RowLocator = (u64, u32, i64, u16, u32);

/// Physical location of one time-series bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BucketLocator {
    pub hash: u64,
    pub salt: u32,
    pub window: i64,
    pub serial: u16,
}

fn ordered_window(window: i64) -> u64 {
    (window as u64) ^ (1u64 << 63)
}

fn unordered_window(encoded: u64) -> i64 {
    (encoded ^ (1u64 << 63)) as i64
}

fn series_index_key(loc: BucketLocator) -> Vec<u8> {
    let mut key = Vec::with_capacity(23);
    key.push(KEY_INDEX_SERIES);
    key.extend_from_slice(&loc.hash.to_be_bytes());
    key.extend_from_slice(&ordered_window(loc.window).to_be_bytes());
    key.extend_from_slice(&loc.salt.to_be_bytes());
    key.extend_from_slice(&loc.serial.to_be_bytes());
    key
}

fn time_index_key(loc: BucketLocator) -> Vec<u8> {
    let mut key = Vec::with_capacity(23);
    key.push(KEY_INDEX_TIME);
    key.extend_from_slice(&ordered_window(loc.window).to_be_bytes());
    key.extend_from_slice(&loc.hash.to_be_bytes());
    key.extend_from_slice(&loc.salt.to_be_bytes());
    key.extend_from_slice(&loc.serial.to_be_bytes());
    key
}

fn index_key_locator(key: &[u8]) -> Option<BucketLocator> {
    if key.len() != 23 {
        return None;
    }
    match key[0] {
        KEY_INDEX_SERIES => Some(BucketLocator {
            hash: u64::from_be_bytes(key[1..9].try_into().ok()?),
            window: unordered_window(u64::from_be_bytes(key[9..17].try_into().ok()?)),
            salt: u32::from_be_bytes(key[17..21].try_into().ok()?),
            serial: u16::from_be_bytes(key[21..23].try_into().ok()?),
        }),
        KEY_INDEX_TIME => Some(BucketLocator {
            window: unordered_window(u64::from_be_bytes(key[1..9].try_into().ok()?)),
            hash: u64::from_be_bytes(key[9..17].try_into().ok()?),
            salt: u32::from_be_bytes(key[17..21].try_into().ok()?),
            serial: u16::from_be_bytes(key[21..23].try_into().ok()?),
        }),
        _ => None,
    }
}

fn bucket_locator_key(loc: BucketLocator) -> Vec<u8> {
    bucket_key(loc.hash, loc.salt, loc.window, loc.serial)
}

fn index_put(wtxn: &mut RwTxn<'_>, db: &Database<Bytes, Bytes>, loc: BucketLocator) -> Result<()> {
    db.put(&mut *wtxn, &series_index_key(loc), b"")?;
    db.put(&mut *wtxn, &time_index_key(loc), b"")?;
    Ok(())
}

fn index_remove(
    wtxn: &mut RwTxn<'_>,
    db: &Database<Bytes, Bytes>,
    loc: BucketLocator,
) -> Result<()> {
    db.delete(&mut *wtxn, &series_index_key(loc))?;
    db.delete(&mut *wtxn, &time_index_key(loc))?;
    Ok(())
}

fn delete_bucket_payload(
    wtxn: &mut RwTxn<'_>,
    db: &Database<Bytes, Bytes>,
    key: &[u8],
) -> Result<()> {
    if let Some((hash, salt, window, serial)) = key_parts(key) {
        index_remove(
            wtxn,
            db,
            BucketLocator {
                hash,
                salt,
                window,
                serial,
            },
        )?;
    }
    db.delete(&mut *wtxn, key)?;
    Ok(())
}

fn bucket_index_ready(rtxn: &RoTxn<'_>, db: &Database<Bytes, Bytes>) -> Result<bool> {
    Ok(db.get(rtxn, &[KEY_INDEX_MARKER])?.is_some())
}

/// Marks a fresh time-series collection as having complete bucket indexes.
pub fn init_bucket_index(wtxn: &mut RwTxn<'_>, env: &heed::Env, name: &str) -> Result<()> {
    let db = ensure_bucket_db(wtxn, env, name)?;
    db.put(&mut *wtxn, &[KEY_INDEX_MARKER], b"1")?;
    Ok(())
}

/// Builds bucket indexes for an existing collection when they are missing.
/// Returns `true` when a backfill was performed.
pub fn ensure_bucket_index(wtxn: &mut RwTxn<'_>, env: &heed::Env, name: &str) -> Result<bool> {
    let db = ensure_bucket_db(wtxn, env, name)?;
    if bucket_index_ready(&*wtxn, &db)? {
        return Ok(false);
    }
    let mut locators = std::collections::BTreeSet::new();
    for item in db.iter(&*wtxn)? {
        let (key, value) = item?;
        match key.first() {
            Some(&KEY_BUCKET) => {
                if let Some((hash, salt, window, serial)) = key_parts(key) {
                    locators.insert(BucketLocator {
                        hash,
                        salt,
                        window,
                        serial,
                    });
                }
            }
            Some(&KEY_OPEN) => {
                if let Some((bucket, _)) = parse_open(value) {
                    if let Some((hash, salt, window, serial)) = key_parts(&bucket) {
                        locators.insert(BucketLocator {
                            hash,
                            salt,
                            window,
                            serial,
                        });
                    }
                }
            }
            _ => {}
        }
    }
    for loc in locators {
        index_put(wtxn, &db, loc)?;
    }
    db.put(&mut *wtxn, &[KEY_INDEX_MARKER], b"1")?;
    Ok(true)
}

/// Returns `(ready, entries, bytes)` for the bucket locator indexes.
pub fn bucket_index_stats(
    rtxn: &RoTxn<'_>,
    env: &heed::Env,
    name: &str,
) -> Result<(bool, u64, u64)> {
    let Some(db) =
        env.open_database::<Bytes, Bytes>(rtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?
    else {
        return Ok((false, 0, 0));
    };
    let ready = bucket_index_ready(rtxn, &db)?;
    let mut entries = 0u64;
    let mut bytes = 0u64;
    for item in db.iter(rtxn)? {
        let (key, value) = item?;
        if matches!(key.first(), Some(&KEY_INDEX_SERIES) | Some(&KEY_INDEX_TIME)) {
            entries += 1;
            bytes += (key.len() + value.len()) as u64;
        }
    }
    Ok((ready, entries, bytes))
}

/// Uses the bucket locator indexes when the filter can be narrowed by series
/// and/or time. Returns `None` for collections without a complete index.
pub fn lookup_indexed_buckets(
    rtxn: &RoTxn<'_>,
    env: &heed::Env,
    name: &str,
    filter: &Value,
    settings: &Settings,
) -> Result<Option<Vec<BucketLocator>>> {
    Ok(
        lookup_indexed_buckets_inner(rtxn, env, name, filter, settings, true)?
            .map(|(locators, _)| locators),
    )
}

/// Returns `(candidate buckets, buckets pruned by zone maps/Bloom)`.
pub fn bucket_prune_stats(
    rtxn: &RoTxn<'_>,
    env: &heed::Env,
    name: &str,
    filter: &Value,
    settings: &Settings,
) -> Result<Option<(usize, usize)>> {
    Ok(
        lookup_indexed_buckets_inner(rtxn, env, name, filter, settings, false)?
            .map(|(locators, pruned)| (locators.len(), pruned)),
    )
}

fn lookup_indexed_buckets_inner(
    rtxn: &RoTxn<'_>,
    env: &heed::Env,
    name: &str,
    filter: &Value,
    settings: &Settings,
    prune: bool,
) -> Result<Option<(Vec<BucketLocator>, usize)>> {
    let Some(db) =
        env.open_database::<Bytes, Bytes>(rtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?
    else {
        return Ok(None);
    };
    if !bucket_index_ready(rtxn, &db)? {
        return Ok(None);
    }
    let (low, high) = time_bounds(filter, &settings.time_field);
    let meta_hash = meta_equality_hash(filter, settings);
    if meta_hash.is_none() && low.is_none() && high.is_none() {
        return Ok(None);
    }
    let span = settings.span_ms();
    let start_window = low
        .map(|low| low.saturating_sub(span.saturating_sub(1)))
        .unwrap_or(i64::MIN);
    let end_window = high.unwrap_or(i64::MAX);
    let mut out = Vec::new();
    if let Some(hash) = meta_hash {
        let start = series_index_key(BucketLocator {
            hash,
            salt: 0,
            window: start_window,
            serial: 0,
        });
        let end = series_index_key(BucketLocator {
            hash,
            salt: u32::MAX,
            window: end_window,
            serial: u16::MAX,
        });
        for item in db.range(
            rtxn,
            &(
                std::ops::Bound::Included(start.as_slice()),
                std::ops::Bound::Included(end.as_slice()),
            ),
        )? {
            let (key, _) = item?;
            if let Some(loc) = index_key_locator(key) {
                out.push(loc);
            }
        }
    } else {
        let start = time_index_key(BucketLocator {
            hash: 0,
            salt: 0,
            window: start_window,
            serial: 0,
        });
        let end = time_index_key(BucketLocator {
            hash: u64::MAX,
            salt: u32::MAX,
            window: end_window,
            serial: u16::MAX,
        });
        for item in db.range(
            rtxn,
            &(
                std::ops::Bound::Included(start.as_slice()),
                std::ops::Bound::Included(end.as_slice()),
            ),
        )? {
            let (key, _) = item?;
            if let Some(loc) = index_key_locator(key) {
                out.push(loc);
            }
        }
    }
    let mut pruned = 0usize;
    out.retain(|loc| {
        let window_end = loc.window.saturating_add(span).saturating_sub(1);
        if low.is_some_and(|low| window_end < low) || high.is_some_and(|high| loc.window > high) {
            return false;
        }
        let key = bucket_locator_key(*loc);
        let allowed = match db.get(rtxn, &key) {
            Ok(Some(payload)) => bucket_zone_allows(payload, settings, filter).unwrap_or(true),
            _ => true,
        };
        if !allowed {
            pruned += 1;
        }
        allowed || !prune
    });
    Ok(Some((out, pruned)))
}

fn encode_bucket(bucket: &Bucket, settings: &Settings) -> Result<Vec<u8>> {
    let n = bucket.rows.len();
    let meta_field = settings.meta_field.as_deref();

    // Flatten every row once, then build row-aligned columns. Flattening keeps
    // nested objects as dotted paths so each leaf gets its own typed codec.
    let mut flattened: Vec<BTreeMap<String, Value>> = Vec::with_capacity(n);
    let mut names: BTreeMap<String, ()> = BTreeMap::new();
    for row in &bucket.rows {
        let mut flat = BTreeMap::new();
        for (key, value) in row {
            if meta_field == Some(key.as_str()) {
                continue;
            }
            flatten(key, value, &mut flat);
        }
        flat.retain(|key, _| meta_field != Some(key.as_str()));
        for key in flat.keys() {
            names.insert(key.clone(), ());
        }
        flattened.push(flat);
    }

    let encoded_columns: Vec<(String, Vec<Option<Value>>)> = names
        .into_keys()
        .map(|name| {
            let values = flattened
                .iter()
                .map(|row| row.get(&name).cloned())
                .collect();
            (name, values)
        })
        .collect();

    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(MAGIC);
    out.push(FORMAT_VERSION);
    out.push(if bucket.sealed { FLAG_SEALED } else { 0 });
    put_uvarint(&mut out, n as u64);
    put_ivarint(&mut out, bucket.window_start);
    put_ivarint(&mut out, bucket.end);
    match &bucket.meta {
        Some(meta) => put_bytes(&mut out, &serde_json::to_vec(meta)?),
        None => put_uvarint(&mut out, 0),
    }
    let zones = zone_from_flat(&flattened);
    put_bytes(&mut out, &zones.encode());
    put_uvarint(&mut out, BLOCK_ROWS as u64);
    put_uvarint(&mut out, encoded_columns.len() as u64);
    for (field, values) in encoded_columns {
        put_bytes(&mut out, field.as_bytes());
        let blocks = values.chunks(BLOCK_ROWS).count().max(1);
        put_uvarint(&mut out, blocks as u64);
        if values.is_empty() {
            let column = Column {
                field: field.clone(),
                values: Vec::new(),
            };
            put_bytes(&mut out, &encode_column(&column, settings.compression)?);
        } else {
            for chunk in values.chunks(BLOCK_ROWS) {
                let column = Column {
                    field: field.clone(),
                    values: chunk.to_vec(),
                };
                put_bytes(&mut out, &encode_column(&column, settings.compression)?);
            }
        }
    }
    let deleted = encode_deleted(bucket);
    put_uvarint(&mut out, deleted.len() as u64);
    out.extend_from_slice(&deleted);
    Ok(out)
}

#[cfg(test)]
fn bucket_bounds(data: &[u8]) -> Result<(bool, i64, i64)> {
    if !valid_magic(data) {
        return Err(corrupt("bad magic"));
    }
    let flags = data[5];
    let mut pos = 6;
    let _rows = get_uvarint(data, &mut pos)?;
    let start = get_ivarint(data, &mut pos)?;
    let end = get_ivarint(data, &mut pos)?;
    Ok((flags & FLAG_SEALED != 0, start, end))
}

/// Reads the header only: sealed flag, window bounds, and the series meta value.
fn bucket_header(data: &[u8], _settings: &Settings) -> Result<(bool, i64, i64, Option<Value>)> {
    if !valid_magic(data) {
        return Err(corrupt("bad magic"));
    }
    let flags = data[5];
    let mut pos = 6;
    let _rows = get_uvarint(data, &mut pos)?;
    let start = get_ivarint(data, &mut pos)?;
    let end = get_ivarint(data, &mut pos)?;
    let meta_raw = get_bytes(data, &mut pos)?;
    let meta = if meta_raw.is_empty() {
        None
    } else {
        Some(serde_json::from_slice(meta_raw)?)
    };
    Ok((flags & FLAG_SEALED != 0, start, end, meta))
}

#[cfg(test)]
fn decode_bucket(data: &[u8], settings: &Settings) -> Result<Bucket> {
    let (bucket, _selected) = decode_bucket_inner(data, settings, None, None)?;
    Ok(bucket)
}

/// Decodes a sealed bucket, optionally materializing only rows whose time
/// column falls in `[low, high]`.
///
/// Column payloads still decode sequentially (several codecs are stateful), but
/// path splitting and row map construction happen only for selected rows. A
/// one-minute query inside an hour bucket therefore pays for one column decode
/// instead of thousands of row builds.
fn decode_bucket_inner(
    data: &[u8],
    settings: &Settings,
    low: Option<i64>,
    high: Option<i64>,
) -> Result<(Bucket, Vec<usize>)> {
    decode_bucket_inner_take(data, settings, low, high, None)
}

fn decode_bucket_top(
    data: &[u8],
    settings: &Settings,
    low: Option<i64>,
    high: Option<i64>,
    direction: i32,
    limit: usize,
    tombs: &BTreeSet<u32>,
) -> Result<(Bucket, Vec<usize>)> {
    if !valid_magic(data) {
        return Err(corrupt("bad magic"));
    }
    if data[4] < FORMAT_VERSION_PREV {
        return decode_bucket_inner_take(data, settings, low, high, Some((direction, limit)));
    }
    let sealed = data[5] & FLAG_SEALED != 0;
    let mut pos = 6;
    let n = get_uvarint(data, &mut pos)? as usize;
    if n > HARD_BUCKET_MAX_DOCS {
        return Err(corrupt("row count out of range"));
    }
    let window_start = get_ivarint(data, &mut pos)?;
    let end = get_ivarint(data, &mut pos)?;
    let meta_raw = get_bytes(data, &mut pos)?;
    let meta: Option<Value> = if meta_raw.is_empty() {
        None
    } else {
        Some(serde_json::from_slice(meta_raw)?)
    };
    skip_zone_map(data, &mut pos, data[4])?;
    let block_rows = get_uvarint(data, &mut pos)? as usize;
    if block_rows == 0 || block_rows > HARD_BUCKET_MAX_DOCS {
        return Err(corrupt("column block size out of range"));
    }
    let column_count = get_uvarint(data, &mut pos)? as usize;
    if column_count > 4096 {
        return Err(corrupt("column count out of range"));
    }
    let mut columns: Vec<(String, Vec<&[u8]>)> = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let field = std::str::from_utf8(get_bytes(data, &mut pos)?)
            .map_err(|_| corrupt("field name is not UTF-8"))?
            .to_string();
        let blocks = get_uvarint(data, &mut pos)? as usize;
        if blocks > HARD_BUCKET_MAX_DOCS {
            return Err(corrupt("column block count out of range"));
        }
        let mut blobs = Vec::with_capacity(blocks);
        for _ in 0..blocks {
            blobs.push(get_bytes(data, &mut pos)?);
        }
        columns.push((field, blobs));
    }
    let mut deleted = BTreeSet::new();
    let bitmap_len = get_uvarint(data, &mut pos)? as usize;
    if bitmap_len != n.div_ceil(8) || pos + bitmap_len > data.len() {
        return Err(corrupt("malformed deletion bitmap"));
    }
    for idx in 0..n {
        if data[pos + idx / 8] & (1 << (idx % 8)) != 0 {
            deleted.insert(idx as u32);
        }
    }
    let mut time_values = vec![None; n];
    if let Some((_, blobs)) = columns
        .iter()
        .find(|(field, _)| field == &settings.time_field)
    {
        let mut base = 0;
        for blob in blobs {
            let mut block_pos = 0;
            let block = decode_column(blob, &mut block_pos)?;
            for (offset, value) in block.values.into_iter().enumerate() {
                if base + offset < n {
                    time_values[base + offset] = value;
                }
            }
            base += block_rows;
        }
    }
    let mut selected: Vec<usize> = (0..n)
        .filter(|idx| !deleted.contains(&(*idx as u32)) && !tombs.contains(&(*idx as u32)))
        .filter(|idx| {
            time_values[*idx]
                .as_ref()
                .and_then(time_value_ms)
                .is_some_and(|ts| {
                    low.is_none_or(|low| ts >= low) && high.is_none_or(|high| ts <= high)
                })
        })
        .collect();
    selected.sort_by(|a, b| {
        let (a, b) = (
            time_values[*a].as_ref().and_then(time_value_ms),
            time_values[*b].as_ref().and_then(time_value_ms),
        );
        if direction < 0 {
            b.cmp(&a)
        } else {
            a.cmp(&b)
        }
    });
    selected.truncate(limit);
    selected.sort_unstable();
    let mut positions = vec![None; n];
    for (room, idx) in selected.iter().enumerate() {
        positions[*idx] = Some(room);
    }
    let mut rows: Vec<Map<String, Value>> = vec![Map::new(); selected.len()];
    for (field, blobs) in &columns {
        let is_time = field == &settings.time_field;
        let mut values: Vec<Option<Value>> = if is_time {
            time_values.clone()
        } else {
            vec![None; n]
        };
        if !is_time {
            let mut base = 0;
            for blob in blobs {
                let block_end = (base + block_rows).min(n);
                let needed = selected.iter().any(|idx| (base..block_end).contains(idx));
                if needed {
                    let mut block_pos = 0;
                    let block = decode_column(blob, &mut block_pos)?;
                    if block.field != *field {
                        return Err(corrupt("column block field mismatch"));
                    }
                    for (offset, value) in block.values.into_iter().enumerate() {
                        if base + offset < block_end {
                            values[base + offset] = value;
                        }
                    }
                }
                base = block_end;
            }
        }
        let paths: Vec<&str> = field.split('.').collect();
        let simple = paths.len() == 1;
        for idx in &selected {
            let Some(room) = positions[*idx] else {
                continue;
            };
            let Some(value) = values[*idx].take() else {
                continue;
            };
            if simple {
                rows[room].insert(paths[0].to_string(), value);
            } else {
                set_path_parts(&mut rows[room], &paths, value);
            }
        }
    }
    if let Some(meta) = &meta {
        if let Some(field) = &settings.meta_field {
            for row in rows.iter_mut() {
                set_path(row, field, meta.clone());
            }
        }
    }
    Ok((
        Bucket {
            sealed,
            window_start,
            end,
            meta,
            rows,
            deleted,
        },
        selected,
    ))
}

fn decode_bucket_inner_take(
    data: &[u8],
    settings: &Settings,
    low: Option<i64>,
    high: Option<i64>,
    take: Option<(i32, usize)>,
) -> Result<(Bucket, Vec<usize>)> {
    if !valid_magic(data) {
        return Err(corrupt("bad magic"));
    }
    let version = data[4];
    if !(FORMAT_VERSION_LEGACY..=FORMAT_VERSION).contains(&version) {
        return Err(corrupt("unsupported format version"));
    }
    let sealed = data[5] & FLAG_SEALED != 0;
    let mut pos = 6;
    let n = get_uvarint(data, &mut pos)? as usize;
    if n > HARD_BUCKET_MAX_DOCS {
        return Err(corrupt("row count out of range"));
    }
    let window_start = get_ivarint(data, &mut pos)?;
    let end = get_ivarint(data, &mut pos)?;
    let meta_raw = get_bytes(data, &mut pos)?;
    let meta: Option<Value> = if meta_raw.is_empty() {
        None
    } else {
        Some(serde_json::from_slice(meta_raw)?)
    };
    skip_zone_map(data, &mut pos, version)?;
    let mut decoded = Vec::new();
    if version >= 2 {
        let block_rows = get_uvarint(data, &mut pos)? as usize;
        if block_rows == 0 || block_rows > HARD_BUCKET_MAX_DOCS {
            return Err(corrupt("column block size out of range"));
        }
        let columns = get_uvarint(data, &mut pos)? as usize;
        if columns > 4096 {
            return Err(corrupt("column count out of range"));
        }
        for _ in 0..columns {
            let field = std::str::from_utf8(get_bytes(data, &mut pos)?)
                .map_err(|_| corrupt("field name is not UTF-8"))?
                .to_string();
            let blocks = get_uvarint(data, &mut pos)? as usize;
            if blocks > HARD_BUCKET_MAX_DOCS {
                return Err(corrupt("column block count out of range"));
            }
            let mut values = Vec::with_capacity(n.min(block_rows.saturating_mul(blocks)));
            for _ in 0..blocks {
                let blob = get_bytes(data, &mut pos)?;
                let mut block_pos = 0;
                let block = decode_column(blob, &mut block_pos)?;
                if block.field != field {
                    return Err(corrupt("column block field mismatch"));
                }
                values.extend(block.values);
            }
            decoded.push(Column { field, values });
        }
        if decoded.iter().any(|column| column.values.len() != n) {
            return Err(corrupt("column length mismatch"));
        }
    } else {
        let columns = get_uvarint(data, &mut pos)? as usize;
        if columns > 4096 {
            return Err(corrupt("column count out of range"));
        }
        for _ in 0..columns {
            decoded.push(decode_column(data, &mut pos)?);
        }
    }
    let mut deleted = BTreeSet::new();
    if has_bitmap(data) {
        let bitmap_len = get_uvarint(data, &mut pos)? as usize;
        if bitmap_len != n.div_ceil(8) || pos + bitmap_len > data.len() {
            return Err(corrupt("malformed deletion bitmap"));
        }
        for idx in 0..n {
            if data[pos + idx / 8] & (1 << (idx % 8)) != 0 {
                deleted.insert(idx as u32);
            }
        }
        let _ = bitmap_len;
    }

    let mut keep = vec![true; n];
    let mut selected = Vec::new();
    if low.is_some() || high.is_some() {
        if let Some(time_column) = decoded
            .iter()
            .find(|column| column.field == settings.time_field)
        {
            for (idx, value) in time_column.values.iter().enumerate() {
                let ts = value.as_ref().and_then(time_value_ms);
                let inside = ts.is_some_and(|ts| {
                    low.is_none_or(|low| ts >= low) && high.is_none_or(|high| ts <= high)
                });
                keep[idx] = inside;
            }
        }
        for (idx, keep) in keep.iter().enumerate() {
            if *keep {
                selected.push(idx);
            }
        }
    } else {
        selected.extend(0..n);
    }
    if let Some((direction, limit)) = take {
        if limit < selected.len() && deleted.is_empty() {
            let times = decoded
                .iter()
                .find(|column| column.field == settings.time_field)
                .map(|column| column.values.as_slice());
            let timestamp = |idx: usize| {
                times
                    .and_then(|values| values.get(idx))
                    .and_then(|value| value.as_ref())
                    .and_then(time_value_ms)
                    .unwrap_or(i64::MIN)
            };
            selected.sort_by(|a, b| {
                let (a, b) = (timestamp(*a), timestamp(*b));
                if direction < 0 {
                    b.cmp(&a)
                } else {
                    a.cmp(&b)
                }
            });
            selected.truncate(limit);
            selected.sort_unstable();
            keep = vec![false; n];
            for idx in &selected {
                keep[*idx] = true;
            }
        }
    }

    let mut rows: Vec<Map<String, Value>> = vec![Map::new(); n];
    for column in decoded {
        let paths: Vec<&str> = column.field.split('.').collect();
        let simple = paths.len() == 1;
        for (idx, value) in column.values.into_iter().enumerate() {
            if !keep[idx] {
                continue;
            }
            let Some(value) = value else { continue };
            if simple {
                rows[idx].insert(paths[0].to_string(), value);
            } else {
                set_path_parts(&mut rows[idx], &paths, value);
            }
        }
    }
    // Drop rows filtered out by the time range so callers never materialize
    // documents outside the requested window.
    let mut rows: Vec<Map<String, Value>> = if selected.len() == n {
        rows
    } else {
        rows.into_iter()
            .zip(keep.iter())
            .filter(|(_, keep)| **keep)
            .map(|(row, _)| row)
            .collect()
    };
    if let Some(meta) = &meta {
        if let Some(field) = &settings.meta_field {
            for row in rows.iter_mut() {
                set_path(row, field, meta.clone());
            }
        }
    }
    Ok((
        Bucket {
            sealed,
            window_start,
            end,
            meta,
            rows,
            deleted,
        },
        selected,
    ))
}

/// Decodes only one row of a sealed bucket. Used by point lookups so a single
/// `_id` does not build thousands of maps.
fn decode_bucket_row(
    data: &[u8],
    settings: &Settings,
    index: usize,
) -> Result<Option<Map<String, Value>>> {
    if !valid_magic(data) {
        return Err(corrupt("bad magic"));
    }
    let mut pos = 6;
    let n = get_uvarint(data, &mut pos)? as usize;
    let _window_start = get_ivarint(data, &mut pos)?;
    let _end = get_ivarint(data, &mut pos)?;
    let meta_raw = get_bytes(data, &mut pos)?;
    let meta: Option<Value> = if meta_raw.is_empty() {
        None
    } else {
        Some(serde_json::from_slice(meta_raw)?)
    };
    let version = data[4];
    skip_zone_map(data, &mut pos, version)?;
    if index >= n {
        return Ok(None);
    }
    let mut row = Map::new();
    if version >= 2 {
        let block_rows = get_uvarint(data, &mut pos)? as usize;
        if block_rows == 0 {
            return Err(corrupt("column block size out of range"));
        }
        let columns = get_uvarint(data, &mut pos)? as usize;
        let target = index / block_rows;
        let local = index % block_rows;
        for _ in 0..columns {
            let field = std::str::from_utf8(get_bytes(data, &mut pos)?)
                .map_err(|_| corrupt("field name is not UTF-8"))?
                .to_string();
            let blocks = get_uvarint(data, &mut pos)? as usize;
            for block in 0..blocks {
                let blob = get_bytes(data, &mut pos)?;
                if block != target {
                    continue;
                }
                let mut block_pos = 0;
                let column = decode_column(blob, &mut block_pos)?;
                if column.field != field {
                    return Err(corrupt("column block field mismatch"));
                }
                if let Some(Some(value)) = column.values.into_iter().nth(local) {
                    set_path(&mut row, &field, value);
                }
            }
        }
    } else {
        let columns = get_uvarint(data, &mut pos)? as usize;
        for _ in 0..columns {
            let column = decode_column(data, &mut pos)?;
            if let Some(Some(value)) = column.values.into_iter().nth(index) {
                set_path(&mut row, &column.field, value);
            }
        }
    }
    if has_bitmap(data) {
        let bitmap_len = get_uvarint(data, &mut pos)? as usize;
        if bitmap_len != n.div_ceil(8) || pos + bitmap_len > data.len() {
            return Err(corrupt("malformed deletion bitmap"));
        }
        if data[pos + index / 8] & (1 << (index % 8)) != 0 {
            return Ok(None);
        }
    }
    if let Some(meta) = &meta {
        if let Some(field) = &settings.meta_field {
            set_path(&mut row, field, meta.clone());
        }
    }
    Ok(Some(row))
}

fn time_value_ms(value: &Value) -> Option<i64> {
    if let Some(ms) = date::parse_value(value) {
        return Some(ms);
    }
    match value {
        Value::Number(number) => number.as_i64(),
        _ => None,
    }
}

pub(crate) fn extract_time(doc: &Value, field: &str) -> Option<i64> {
    let raw = get_path(doc, field)?;
    if let Some(ms) = time_value_ms(raw) {
        return Some(ms);
    }
    raw.as_str().and_then(date::parse_rfc3339_ms)
}

fn row_json_bytes(doc: &Value) -> u64 {
    serde_json::to_vec(doc).map(|v| v.len() as u64).unwrap_or(0)
}

/// Synthetic `_id` for a row without a user-provided `_id`.
///
/// Encodes the full physical location (series hash, salt, window, bucket
/// serial, row sequence) so `find_by_key` can jump straight to the row instead
/// of scanning the collection.
pub(crate) fn row_id(hash: u64, salt: u32, window_start: i64, serial: u16, seq: u32) -> String {
    format!(
        "{hash:016x}{salt:08x}{:016x}{serial:04x}{seq:08x}",
        window_start as u64
    )
}

fn parse_row_id(id: &str) -> Option<(u64, u32, i64, u16, u32)> {
    if id.len() != 52 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let hash = u64::from_str_radix(&id[0..16], 16).ok()?;
    let salt = u32::from_str_radix(&id[16..24], 16).ok()?;
    let window = u64::from_str_radix(&id[24..40], 16).ok()? as i64;
    let serial = u16::from_str_radix(&id[40..44], 16).ok()?;
    let seq = u32::from_str_radix(&id[44..52], 16).ok()?;
    Some((hash, salt, window, serial, seq))
}

fn series_hash(settings: &Settings, meta: Option<&Value>) -> u64 {
    match (settings.meta_field.as_deref(), meta) {
        (Some(_), Some(value)) => fnv1a64(canonical_json(value).as_bytes()),
        (Some(_), None) => fnv1a64(b"<missing>"),
        _ => fnv1a64(b"<single-series>"),
    }
}

fn series_meta(doc: &Value, settings: &Settings) -> Option<Value> {
    settings
        .meta_field
        .as_deref()
        .and_then(|field| get_path(doc, field))
        .cloned()
}

/// True when the series stored under `(hash, salt)` has the same meta value.
fn series_matches(
    wtxn: &RwTxn<'_>,
    db: &Database<Bytes, Bytes>,
    settings: &Settings,
    hash: u64,
    salt: u32,
    meta: Option<&Value>,
) -> Result<bool> {
    let mut base = Vec::with_capacity(13);
    base.push(KEY_BUCKET);
    base.extend_from_slice(&hash.to_be_bytes());
    base.extend_from_slice(&salt.to_be_bytes());
    if let Some(item) = db.prefix_iter(wtxn, &base)?.next() {
        let (_key, payload) = item?;
        let mut pos = 6;
        let _rows = get_uvarint(payload, &mut pos)?;
        let _start = get_ivarint(payload, &mut pos)?;
        let _end = get_ivarint(payload, &mut pos)?;
        let stored = get_bytes(payload, &mut pos)?;
        let stored: Option<Value> = if stored.is_empty() {
            None
        } else {
            Some(serde_json::from_slice(stored)?)
        };
        return Ok(stored.as_ref() == meta);
    }
    // No sealed bucket yet: inspect the first pending row.
    let mut row_base = base;
    row_base[0] = KEY_ROW;
    if let Some(item) = db.prefix_iter(wtxn, &row_base)?.next() {
        let (_key, payload) = item?;
        let row: Map<String, Value> = serde_json::from_slice(payload)?;
        let doc = Value::Object(row);
        return Ok(series_meta(&doc, settings).as_ref() == meta);
    }
    Ok(true)
}

/// Resolves the series salt for a value, creating a new series on first write.
fn resolve_salt(
    wtxn: &mut RwTxn<'_>,
    db: &Database<Bytes, Bytes>,
    settings: &Settings,
    hash: u64,
    meta: Option<&Value>,
) -> Result<u32> {
    for salt in 0..MAX_SERIES_SALT {
        let key = open_key(hash, salt);
        if db.get(&*wtxn, &key)?.is_none() {
            return Ok(salt);
        }
        if series_matches(wtxn, db, settings, hash, salt, meta)? {
            return Ok(salt);
        }
    }
    Err(Error::Query(
        "time-series series hash table exhausted".into(),
    ))
}

fn ensure_bucket_db(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
) -> Result<Database<Bytes, Bytes>> {
    let db_name = format!("{BUCKET_DB_PREFIX}{name}");
    Ok(env.create_database::<Bytes, Bytes>(wtxn, Some(&db_name))?)
}

/// Open-bucket pointer: the bucket key plus the number of pending rows.
fn encode_open(bucket_key: &[u8], count: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(bucket_key.len() + 4);
    out.extend_from_slice(bucket_key);
    out.extend_from_slice(&count.to_be_bytes());
    out
}

fn parse_open(value: &[u8]) -> Option<(Vec<u8>, u32)> {
    if value.len() != 23 + 4 {
        return None;
    }
    let count = u32::from_be_bytes(value[23..27].try_into().ok()?);
    Some((value[..23].to_vec(), count))
}

fn row_base(hash: u64, salt: u32, window_start: i64, serial: u16) -> Vec<u8> {
    let mut key = bucket_key(hash, salt, window_start, serial);
    key[0] = KEY_ROW;
    key
}

fn user_key(id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + id.len());
    key.push(KEY_USER);
    key.extend_from_slice(id.as_bytes());
    key
}

fn user_locator(hash: u64, salt: u32, window: i64, serial: u16, seq: u32) -> Vec<u8> {
    let mut value = bucket_key(hash, salt, window, serial);
    value.extend_from_slice(&seq.to_be_bytes());
    value
}

#[allow(clippy::too_many_arguments)]
fn remember_user_id(
    db: &Database<Bytes, Bytes>,
    wtxn: &mut RwTxn<'_>,
    id: &str,
    hash: u64,
    salt: u32,
    window: i64,
    serial: u16,
    seq: u32,
) -> Result<()> {
    if parse_row_id(id).is_none() {
        db.put(
            &mut *wtxn,
            &user_key(id),
            &user_locator(hash, salt, window, serial, seq),
        )?;
    }
    Ok(())
}

fn row_key(hash: u64, salt: u32, window_start: i64, serial: u16, seq: u32) -> Vec<u8> {
    let mut key = row_base(hash, salt, window_start, serial);
    key.extend_from_slice(&seq.to_be_bytes());
    key
}

fn tomb_base(hash: u64, salt: u32, window_start: i64, serial: u16) -> Vec<u8> {
    let mut key = row_base(hash, salt, window_start, serial);
    key[0] = KEY_TOMB;
    key
}

fn tomb_key(hash: u64, salt: u32, window_start: i64, serial: u16, seq: u32) -> Vec<u8> {
    let mut key = tomb_base(hash, salt, window_start, serial);
    key.extend_from_slice(&seq.to_be_bytes());
    key
}

fn row_key_parts(key: &[u8]) -> Option<(u64, u32, i64, u16, u32)> {
    if key.len() != 27 || key[0] != KEY_ROW {
        return None;
    }
    let (hash, salt, window, serial) = key_parts(&{
        let mut copy = key[..23].to_vec();
        copy[0] = KEY_BUCKET;
        copy
    })?;
    let seq = u32::from_be_bytes(key[23..27].try_into().ok()?);
    Some((hash, salt, window, serial, seq))
}

/// Row indexes marked as deleted inside one sealed bucket.
///
/// Sealed buckets are immutable and synthetic `_id`s encode the row position,
/// so deleting a row writes a tombstone instead of rewriting (and renumbering)
/// the bucket. Tombstones are cheap `put`s and are dropped when the bucket is
/// rewritten by retention or compaction.
fn tombstones(
    txn: &RoTxn<'_>,
    db: &Database<Bytes, Bytes>,
    hash: u64,
    salt: u32,
    window: i64,
    serial: u16,
) -> Result<BTreeSet<u32>> {
    let prefix = tomb_base(hash, salt, window, serial);
    let mut out = BTreeSet::new();
    for item in db.prefix_iter(txn, &prefix)? {
        let (key, _) = item?;
        if key.len() == 27 {
            out.insert(u32::from_be_bytes(key[23..27].try_into().expect("4 bytes")));
        }
    }
    Ok(out)
}

fn drop_tombstones(
    wtxn: &mut RwTxn<'_>,
    db: &Database<Bytes, Bytes>,
    hash: u64,
    salt: u32,
    window: i64,
    serial: u16,
) -> Result<()> {
    let prefix = tomb_base(hash, salt, window, serial);
    let mut keys = Vec::new();
    for item in db.prefix_iter(&*wtxn, &prefix)? {
        let (key, _) = item?;
        keys.push(key.to_vec());
    }
    for key in keys {
        db.delete(&mut *wtxn, &key)?;
    }
    Ok(())
}

/// Folds legacy 0x03 tombstone keys into the bucket bitmap.
fn merge_tombstones(
    wtxn: &mut RwTxn<'_>,
    db: &Database<Bytes, Bytes>,
    bucket: &mut Bucket,
    hash: u64,
    salt: u32,
    window: i64,
    serial: u16,
) -> Result<()> {
    let tombs = tombstones(&*wtxn, db, hash, salt, window, serial)?;
    if tombs.is_empty() {
        return Ok(());
    }
    bucket.deleted.extend(tombs);
    drop_tombstones(wtxn, db, hash, salt, window, serial)?;
    Ok(())
}

/// Clears the payload of rows already marked deleted. Their sequence slots stay
/// in place so synthetic `_id`s keep pointing at the same physical row, while
/// column encoders no longer store their values.
fn clear_deleted_rows(bucket: &mut Bucket) {
    for seq in &bucket.deleted {
        if let Some(row) = bucket.rows.get_mut(*seq as usize) {
            row.clear();
        }
    }
}

/// Result of rewriting sealed buckets to drop deleted-row payloads.
#[derive(Debug, Clone, Default)]
pub struct BucketCompactReport {
    pub compacted_buckets: usize,
    pub before_bytes: u64,
    pub after_bytes: u64,
}

impl BucketCompactReport {
    pub fn reclaimed_bytes(&self) -> u64 {
        self.before_bytes.saturating_sub(self.after_bytes)
    }
}

/// Rewrites sealed buckets with at least `min_deleted` deleted rows, dropping
/// their column values without renumbering surviving rows.
pub fn compact_deleted_buckets(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    min_deleted: usize,
) -> Result<BucketCompactReport> {
    let db =
        env.create_database::<Bytes, Bytes>(wtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?;
    let mut report = BucketCompactReport::default();
    let mut candidates = Vec::new();
    for item in db.iter(&*wtxn)? {
        let (key, payload) = item?;
        if key.first() != Some(&KEY_BUCKET) {
            continue;
        }
        let (bucket, _) = decode_bucket_inner(payload, settings, None, None)?;
        if bucket.deleted.len() >= min_deleted {
            candidates.push((key.to_vec(), payload.to_vec(), bucket));
        }
    }
    for (key, payload, mut bucket) in candidates {
        let Some((hash, salt, window, serial)) = key_parts(&key) else {
            continue;
        };
        report.before_bytes += payload.len() as u64;
        clear_deleted_rows(&mut bucket);
        let encoded = encode_bucket(&bucket, settings)?;
        if encoded.len() < payload.len() {
            db.put(&mut *wtxn, &key, &encoded)?;
            report.compacted_buckets += 1;
            report.after_bytes += encoded.len() as u64;
        } else {
            report.after_bytes += payload.len() as u64;
        }
        let _ = (hash, salt, window, serial);
    }
    Ok(report)
}

/// Re-encodes a bucket with its deletion bitmap and drops legacy tombstone keys.
fn persist_bucket(
    wtxn: &mut RwTxn<'_>,
    db: &Database<Bytes, Bytes>,
    key: &[u8],
    bucket: &mut Bucket,
    settings: &Settings,
    loc: (u64, u32, i64, u16),
) -> Result<()> {
    merge_tombstones(wtxn, db, bucket, loc.0, loc.1, loc.2, loc.3)?;
    clear_deleted_rows(bucket);
    let payload = encode_bucket(bucket, settings)?;
    db.put(&mut *wtxn, key, &payload)?;
    index_put(
        wtxn,
        db,
        BucketLocator {
            hash: loc.0,
            salt: loc.1,
            window: loc.2,
            serial: loc.3,
        },
    )?;
    Ok(())
}

/// Converts every pending row of one bucket into a sealed columnar payload.
fn seal_bucket(
    wtxn: &mut RwTxn<'_>,
    db: &Database<Bytes, Bytes>,
    settings: &Settings,
    bucket_key_bytes: &[u8],
) -> Result<usize> {
    let Some((hash, salt, window, serial)) = key_parts(bucket_key_bytes) else {
        return Ok(0);
    };
    let prefix = row_base(hash, salt, window, serial);
    let mut source: Vec<(u32, Map<String, Value>, Vec<u8>)> = Vec::new();
    let mut max_seq = None;
    for item in db.prefix_iter(&*wtxn, &prefix)? {
        let (key, value) = item?;
        if key.len() != 27 {
            return Err(corrupt("malformed pending row key"));
        }
        let seq = u32::from_be_bytes(key[23..27].try_into().expect("4 bytes"));
        max_seq = Some(max_seq.map_or(seq, |current: u32| current.max(seq)));
        source.push((
            seq,
            serde_json::from_slice::<Map<String, Value>>(value)?,
            key.to_vec(),
        ));
    }
    let Some(max_seq) = max_seq else {
        return Ok(0);
    };
    let len = max_seq as usize + 1;
    if len > HARD_BUCKET_MAX_DOCS {
        return Err(corrupt("pending row sequence out of range"));
    }
    // Synthetic ids encode the pending row's sequence number. Deleting a
    // pending row leaves a gap; keep the gap (marked deleted) instead of
    // renumbering the surviving rows, so previously returned ids stay valid.
    let mut rows: Vec<Map<String, Value>> = vec![Map::new(); len];
    let mut deleted = BTreeSet::new();
    deleted.extend((0..len as u32).filter(|seq| !source.iter().any(|(s, _, _)| s == seq)));
    let mut end = window;
    let mut meta = None;
    for (seq, row, _) in &source {
        let doc = Value::Object(row.clone());
        if let Some(ts) = extract_time(&doc, &settings.time_field) {
            end = end.max(ts);
        }
        if meta.is_none() {
            meta = series_meta(&doc, settings);
        }
        rows[*seq as usize] = row.clone();
    }
    let mut bucket = Bucket {
        sealed: true,
        window_start: window,
        end,
        meta,
        rows,
        deleted,
    };
    let tombs = tombstones(wtxn, db, hash, salt, window, serial)?;
    bucket.deleted.extend(tombs);
    drop_tombstones(wtxn, db, hash, salt, window, serial)?;
    clear_deleted_rows(&mut bucket);
    let payload = encode_bucket(&bucket, settings)?;
    db.put(&mut *wtxn, bucket_key_bytes, &payload)?;
    index_put(
        wtxn,
        db,
        BucketLocator {
            hash,
            salt,
            window,
            serial,
        },
    )?;
    for (_, _, key) in source {
        db.delete(&mut *wtxn, &key)?;
    }
    Ok(payload.len())
}

/// Seals every open bucket. Returns the number of sealed buckets.
pub fn seal_all(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
) -> Result<usize> {
    let db =
        env.create_database::<Bytes, Bytes>(wtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?;
    let mut pointers: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for item in db.prefix_iter(&*wtxn, &[KEY_OPEN])? {
        let (key, value) = item?;
        if let Some((bucket_key, _count)) = parse_open(value) {
            pointers.push((key.to_vec(), bucket_key));
        }
    }
    let mut sealed = 0;
    for (pointer_key, bucket_key_bytes) in pointers {
        let Some((hash, salt, window, serial)) = key_parts(&bucket_key_bytes) else {
            continue;
        };
        if seal_bucket(wtxn, &db, settings, &bucket_key_bytes)? > 0 {
            sealed += 1;
        }
        let next = bucket_key(hash, salt, window, serial.saturating_add(1));
        db.put(&mut *wtxn, &pointer_key, &encode_open(&next, 0))?;
    }
    Ok(sealed)
}

/// Inserts one document into a time-series collection.
///
/// The row is appended under its own LMDB key, so an insert costs O(1) instead
/// of re-encoding the whole bucket. Sealing (columnar encoding) happens once
/// per bucket when it reaches `bucket_max_docs` or when the window advances.
pub fn insert(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    mut doc: Value,
) -> Result<String> {
    settings.validate()?;
    crate::date::normalize_document(&mut doc)?;
    let object = doc
        .as_object()
        .cloned()
        .ok_or_else(|| Error::Query("document must be an object".into()))?;
    if !object.contains_key(&settings.time_field) {
        return Err(Error::Query(format!(
            "time-series document is missing the time field `{}`",
            settings.time_field
        )));
    }
    let end = extract_time(&doc, &settings.time_field).ok_or_else(|| {
        Error::Query(format!(
            "time-series field `{}` must be a date or epoch milliseconds",
            settings.time_field
        ))
    })?;
    let meta = series_meta(&doc, settings);
    let hash = series_hash(settings, meta.as_ref());
    let span = settings.span_ms();
    let window_start = end.div_euclid(span) * span;
    let row_bytes = serde_json::to_vec(&doc)?;
    if row_bytes.len() > crate::store::MAX_DOCUMENT_BYTES {
        return Err(Error::DocumentTooLarge(
            row_bytes.len(),
            crate::store::MAX_DOCUMENT_BYTES,
        ));
    }

    let db = ensure_bucket_db(wtxn, env, name)?;
    let salt = resolve_salt(wtxn, &db, settings, hash, meta.as_ref())?;
    let pointer_key = open_key(hash, salt);

    let (mut key, mut count) = match db.get(&*wtxn, &pointer_key)? {
        Some(value) => parse_open(value).unwrap_or((bucket_key(hash, salt, window_start, 0), 0)),
        None => (bucket_key(hash, salt, window_start, 0), 0),
    };
    let (_, _, key_window, key_serial) = key_parts(&key).unwrap_or((hash, salt, window_start, 0));

    let stale = key_window != window_start;
    let full = count as usize >= settings.bucket_max_docs;
    if stale || full {
        seal_bucket(wtxn, &db, settings, &key)?;
        let mut serial = key_serial.saturating_add(1);
        loop {
            let candidate = bucket_key(hash, salt, window_start, serial);
            if db.get(&*wtxn, &candidate)?.is_none() {
                key = candidate;
                break;
            }
            serial = serial.saturating_add(1);
            if serial == u16::MAX {
                return Err(Error::Query("time-series bucket serial exhausted".into()));
            }
        }
        count = 0;
    }

    let (_, _, window, serial) = key_parts(&key).unwrap_or((hash, salt, window_start, 0));
    db.put(
        &mut *wtxn,
        &row_key(hash, salt, window, serial, count),
        &row_bytes,
    )?;
    db.put(&mut *wtxn, &pointer_key, &encode_open(&key, count + 1))?;
    index_put(
        wtxn,
        &db,
        BucketLocator {
            hash,
            salt,
            window,
            serial,
        },
    )?;

    let id = match object.get("_id") {
        Some(Value::String(id)) => id.clone(),
        Some(other) => crate::id::key_from_value(other).ok_or(Error::InvalidId)?,
        None => row_id(hash, salt, window, serial, count),
    };
    remember_user_id(&db, wtxn, &id, hash, salt, window, serial, count)?;
    update_rollups_for_doc(wtxn, env, name, settings, &doc)?;
    Ok(id)
}

/// Returns the string when the filter is exactly `{"_id": "<string>"}`.
pub(crate) fn exact_id_filter(filter: &Value) -> Option<&str> {
    let object = filter.as_object()?;
    if object.len() != 1 {
        return None;
    }
    object.get("_id")?.as_str()
}

/// Inserts many documents, keeping one open pointer per series in memory.
///
/// The ordinary `insert` reads and writes the open pointer for every row. In a
/// bulk load that is one extra LMDB get and put per document. This variant
/// resolves each series once and writes its pointer at the end of the batch.
pub fn insert_many(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    docs: &[Value],
) -> Result<Vec<String>> {
    settings.validate()?;
    let db = ensure_bucket_db(wtxn, env, name)?;
    struct OpenState {
        salt: u32,
        key: Vec<u8>,
        count: u32,
    }
    let mut series: std::collections::HashMap<(u64, String), OpenState> =
        std::collections::HashMap::new();
    let mut ids = Vec::with_capacity(docs.len());
    for doc in docs {
        let mut doc = doc.clone();
        crate::date::normalize_document(&mut doc)?;
        let object = doc
            .as_object()
            .cloned()
            .ok_or_else(|| Error::Query("document must be an object".into()))?;
        if !object.contains_key(&settings.time_field) {
            return Err(Error::Query(format!(
                "time-series document is missing the time field `{}`",
                settings.time_field
            )));
        }
        let end = extract_time(&doc, &settings.time_field).ok_or_else(|| {
            Error::Query(format!(
                "time-series field `{}` must be a date or epoch milliseconds",
                settings.time_field
            ))
        })?;
        let meta = series_meta(&doc, settings);
        let hash = series_hash(settings, meta.as_ref());
        let span = settings.span_ms();
        let window_start = end.div_euclid(span) * span;
        let row_bytes = serde_json::to_vec(&doc)?;
        if row_bytes.len() > crate::store::MAX_DOCUMENT_BYTES {
            return Err(Error::DocumentTooLarge(
                row_bytes.len(),
                crate::store::MAX_DOCUMENT_BYTES,
            ));
        }
        let series_key = (hash, canonical_json(meta.as_ref().unwrap_or(&Value::Null)));
        if !series.contains_key(&series_key) {
            let salt = resolve_salt(wtxn, &db, settings, hash, meta.as_ref())?;
            let pointer_key = open_key(hash, salt);
            let (key, count) = match db.get(&*wtxn, &pointer_key)? {
                Some(value) => {
                    parse_open(value).unwrap_or((bucket_key(hash, salt, window_start, 0), 0))
                }
                None => (bucket_key(hash, salt, window_start, 0), 0),
            };
            series.insert(series_key.clone(), OpenState { salt, key, count });
        }
        let state = series.get_mut(&series_key).expect("series inserted");
        let (_, _, key_window, key_serial) =
            key_parts(&state.key).unwrap_or((hash, state.salt, window_start, 0));
        let stale = key_window != window_start;
        let full = state.count as usize >= settings.bucket_max_docs;
        if stale || full {
            seal_bucket(wtxn, &db, settings, &state.key)?;
            let mut serial = key_serial.saturating_add(1);
            loop {
                let candidate = bucket_key(hash, state.salt, window_start, serial);
                if db.get(&*wtxn, &candidate)?.is_none() {
                    state.key = candidate;
                    break;
                }
                serial = serial.saturating_add(1);
                if serial == u16::MAX {
                    return Err(Error::Query("time-series bucket serial exhausted".into()));
                }
            }
            state.count = 0;
        }
        let (_, _, window, serial) =
            key_parts(&state.key).unwrap_or((hash, state.salt, window_start, 0));
        db.put(
            &mut *wtxn,
            &row_key(hash, state.salt, window, serial, state.count),
            &row_bytes,
        )?;
        let id = match object.get("_id") {
            Some(Value::String(id)) => id.clone(),
            Some(other) => crate::id::key_from_value(other).ok_or(Error::InvalidId)?,
            None => row_id(hash, state.salt, window, serial, state.count),
        };
        remember_user_id(
            &db,
            wtxn,
            &id,
            hash,
            state.salt,
            window,
            serial,
            state.count,
        )?;
        update_rollups_for_doc(wtxn, env, name, settings, &doc)?;
        state.count = state.count.saturating_add(1);
        ids.push(id);
    }
    for ((hash, _), state) in series {
        db.put(
            &mut *wtxn,
            &open_key(hash, state.salt),
            &encode_open(&state.key, state.count),
        )?;
        if let Some((_, _, window, serial)) = key_parts(&state.key) {
            index_put(
                wtxn,
                &db,
                BucketLocator {
                    hash,
                    salt: state.salt,
                    window,
                    serial,
                },
            )?;
        }
    }
    Ok(ids)
}

pub(crate) fn time_bounds(filter: &Value, field: &str) -> (Option<i64>, Option<i64>) {
    let mut low = None;
    let mut high = None;
    collect_time_bounds(filter, field, &mut low, &mut high);
    (low, high)
}

fn lower_bound(current: &mut Option<i64>, candidate: i64, inclusive: bool) {
    let candidate = if inclusive {
        candidate
    } else {
        candidate.saturating_add(1)
    };
    *current = Some(match *current {
        Some(existing) => existing.max(candidate),
        None => candidate,
    });
}

fn upper_bound(current: &mut Option<i64>, candidate: i64, inclusive: bool) {
    let candidate = if inclusive {
        candidate
    } else {
        candidate.saturating_sub(1)
    };
    *current = Some(match *current {
        Some(existing) => existing.min(candidate),
        None => candidate,
    });
}

fn collect_time_bounds(filter: &Value, field: &str, low: &mut Option<i64>, high: &mut Option<i64>) {
    let Some(object) = filter.as_object() else {
        return;
    };
    for (key, value) in object {
        if key == field {
            match value {
                Value::Object(conditions) => {
                    for (op, operand) in conditions {
                        let Some(ms) = time_value_ms(operand) else {
                            continue;
                        };
                        match op.as_str() {
                            "$gte" => lower_bound(low, ms, true),
                            "$gt" => lower_bound(low, ms, false),
                            "$lte" => upper_bound(high, ms, true),
                            "$lt" => upper_bound(high, ms, false),
                            "$eq" => {
                                lower_bound(low, ms, true);
                                upper_bound(high, ms, true);
                            }
                            _ => {}
                        }
                    }
                }
                other => {
                    if let Some(ms) = time_value_ms(other) {
                        lower_bound(low, ms, true);
                        upper_bound(high, ms, true);
                    }
                }
            }
        } else if key == "$and" {
            if let Some(items) = value.as_array() {
                for item in items {
                    collect_time_bounds(item, field, low, high);
                }
            }
        }
    }
}

/// Returns the series hash when the filter pins the meta field by equality.
fn meta_equality_hash(filter: &Value, settings: &Settings) -> Option<u64> {
    fn walk(
        value: &Value,
        field: &str,
        settings: &Settings,
    ) -> std::result::Result<Option<u64>, ()> {
        let object = value.as_object().ok_or(())?;
        let mut found = None;
        if let Some(value) = object.get(field) {
            if value.is_object() && !value.as_object().ok_or(())?.contains_key("$eq") {
                return Err(());
            }
            let operand = value
                .as_object()
                .and_then(|object| object.get("$eq"))
                .unwrap_or(value);
            found = Some(series_hash(settings, Some(operand)));
        }
        if let Some(items) = object.get("$and").and_then(Value::as_array) {
            for item in items {
                if let Some(hash) = walk(item, field, settings)? {
                    if found.is_some_and(|current| current != hash) {
                        return Err(());
                    }
                    found = Some(hash);
                }
            }
        }
        Ok(found)
    }
    let field = settings.meta_field.as_deref()?;
    walk(filter, field, settings).ok().flatten()
}

fn expired(settings: &Settings, doc: &Value, now_ms: i64) -> bool {
    let Some(ttl) = settings.expire_after_seconds else {
        return false;
    };
    let Some(ts) = extract_time(doc, &settings.time_field) else {
        return false;
    };
    ts.saturating_add((ttl as i64).saturating_mul(1000)) < now_ms
}

/// Extracts only the constraints that apply to the series meta value.
///
/// Buckets store the meta value once, so a filter such as
/// `{"device.id": "sensor-7"}` can prune every other series by checking the
/// bucket header without decoding a single column.
fn meta_only_filter(filter: &Value, settings: &Settings) -> Option<Value> {
    let field = settings.meta_field.as_deref()?;
    let prefix = format!("{field}.");
    let object = filter.as_object()?;
    let mut out = Map::new();
    for (key, value) in object {
        if key == field || key.starts_with(&prefix) {
            out.insert(key.clone(), value.clone());
        } else if key == "$and" {
            if let Some(items) = value.as_array() {
                let nested: Vec<Value> = items
                    .iter()
                    .filter_map(|item| meta_only_filter(item, settings))
                    .collect();
                if !nested.is_empty() {
                    out.insert("$and".into(), Value::Array(nested));
                }
            }
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(Value::Object(out))
    }
}

fn meta_bucket_matches(filter: &Value, settings: &Settings, meta: Option<&Value>) -> bool {
    let Some(field) = settings.meta_field.as_deref() else {
        return true;
    };
    let mut probe = Map::new();
    set_path(&mut probe, field, meta.cloned().unwrap_or(Value::Null));
    matcher::matches(&Value::Object(probe), filter)
}

/// Executes a `find` query against the bucket store.
pub fn find(
    rtxn: &RoTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    query: &FindQuery,
) -> Result<Vec<Value>> {
    find_with_ids(rtxn, env, name, settings, query, None)
}

/// Loads the rows for a window pipeline, decoding independent bucket
/// locators on worker threads when the bucket index can narrow the scan.
pub(crate) fn find_window_docs(
    rtxn: &RoTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    query: &FindQuery,
) -> Result<Vec<Value>> {
    let available_workers = crate::collection::scan_workers();
    if available_workers <= 1 {
        return find(rtxn, env, name, settings, query);
    }
    let Some(locators) = lookup_indexed_buckets(rtxn, env, name, &query.filter, settings)? else {
        return find(rtxn, env, name, settings, query);
    };
    if locators.len() < 2 {
        return find(rtxn, env, name, settings, query);
    }
    let workers = if locators.len() > 16 {
        available_workers.min(8)
    } else {
        available_workers.min(4)
    };
    let db_name = format!("{BUCKET_DB_PREFIX}{name}");
    if env
        .open_database::<Bytes, Bytes>(rtxn, Some(&db_name))?
        .is_none()
    {
        return Ok(Vec::new());
    }
    let (low, high) = time_bounds(&query.filter, &settings.time_field);
    let time_only = meta_equality_hash(&query.filter, settings).is_none()
        && filter_is_meta_time_only(&query.filter, settings);
    let now = crate::ttl::now_ms() as i64;
    let chunk = locators.len().div_ceil(workers);
    let parts = std::thread::scope(|scope| -> Result<Vec<Vec<Value>>> {
        let mut handles = Vec::new();
        for chunk in locators.chunks(chunk) {
            let chunk = chunk.to_vec();
            let db_name = db_name.clone();
            let settings = settings.clone();
            let filter = query.filter.clone();
            handles.push(scope.spawn(move || -> Result<Vec<Value>> {
                let rtxn = env.read_txn()?;
                let Some(db) = env.open_database::<Bytes, Bytes>(&rtxn, Some(&db_name))? else {
                    rtxn.commit()?;
                    return Ok(Vec::new());
                };
                let mut docs = Vec::new();
                for loc in chunk {
                    for doc in docs_for_bucket_window(&rtxn, &db, loc, &settings, low, high)? {
                        if expired(&settings, &doc, now)
                            || (!time_only && !matcher::matches(&doc, &filter))
                        {
                            continue;
                        }
                        docs.push(doc);
                    }
                }
                rtxn.commit()?;
                Ok(docs)
            }));
        }
        let mut parts = Vec::with_capacity(handles.len());
        for handle in handles {
            parts.push(
                handle
                    .join()
                    .map_err(|_| Error::Query("window scan worker panicked".into()))??,
            );
        }
        Ok(parts)
    })?;
    let mut docs: Vec<Value> = parts.into_iter().flatten().collect();
    if !query.sort.is_empty() {
        docs.sort_by(|left, right| compare_docs(left, right, &query.sort));
    }
    let start = query.skip.min(docs.len());
    let limit = query.limit.unwrap_or(usize::MAX);
    let mut docs: Vec<Value> = docs.into_iter().skip(start).take(limit).collect();
    if let Some(projection) = &query.projection {
        for doc in &mut docs {
            *doc = project_document(doc, Some(projection));
        }
    }
    Ok(docs)
}

/// Finds documents, optionally restricted to a set of `_id` values (index
/// candidates). Rows are loaded bucket-by-bucket, so a secondary index lookup
/// decodes each bucket once instead of once per id.
pub fn find_with_ids(
    rtxn: &RoTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    query: &FindQuery,
    candidates: Option<&std::collections::HashSet<String>>,
) -> Result<Vec<Value>> {
    validate_filter(&query.filter)?;
    let limit = query.limit.unwrap_or(usize::MAX);
    if limit == 0 {
        return Ok(Vec::new());
    }
    let db_name = format!("{BUCKET_DB_PREFIX}{name}");
    let Some(db) = env.open_database::<Bytes, Bytes>(rtxn, Some(&db_name))? else {
        return Ok(Vec::new());
    };
    let (low, high) = time_bounds(&query.filter, &settings.time_field);
    let meta_hash = meta_equality_hash(&query.filter, settings);
    let meta_filter = meta_only_filter(&query.filter, settings);
    let deadline = query
        .max_time_ms
        .filter(|ms| *ms > 0)
        .and_then(|ms| std::time::Instant::now().checked_add(std::time::Duration::from_millis(ms)));
    let now = crate::ttl::now_ms() as i64;
    let span = settings.span_ms();
    // Without a sort the query only needs `skip + limit` documents, so stop
    // decoding candidate buckets as soon as that many rows were produced.
    let scan_need = query.skip.saturating_add(limit);
    let need = if query.sort.is_empty() {
        scan_need
    } else {
        usize::MAX
    };

    let time_sort = query
        .sort
        .as_slice()
        .first()
        .filter(|_| query.sort.len() == 1)
        .and_then(|(field, dir)| {
            (field == &settings.time_field && (*dir == 1 || *dir == -1)).then_some(*dir)
        });

    let mut out: Vec<Value> = Vec::new();
    if let Some(direction) = time_sort {
        if meta_hash.is_none() || crate::geo::extract_near(&query.filter).is_some() {
            // Fall through to the generic paths below.
        } else if let Some(mut locators) =
            lookup_indexed_buckets(rtxn, env, name, &query.filter, settings)?
        {
            if direction < 0 {
                locators.reverse();
            }
            let mut index = 0;
            while index < locators.len() {
                let window = locators[index].window;
                while index < locators.len() && locators[index].window == window {
                    let loc = locators[index];
                    let mut docs = docs_for_bucket_locator(
                        rtxn,
                        &db,
                        loc,
                        settings,
                        low,
                        high,
                        Some((direction, scan_need)),
                    )?;
                    docs.retain(|doc| {
                        !expired(settings, doc, now) && matcher::matches(doc, &query.filter)
                    });
                    docs.sort_by(|a, b| compare_docs(a, b, &query.sort));
                    out.extend(docs);
                    index += 1;
                }
                if out.len() >= scan_need {
                    break;
                }
            }
            out.sort_by(|a, b| compare_docs(a, b, &query.sort));
            let start = query.skip.min(out.len());
            let mut out: Vec<Value> = out.into_iter().skip(start).take(limit).collect();
            if let Some(projection) = &query.projection {
                for doc in out.iter_mut() {
                    *doc = project_document(doc, Some(projection));
                }
            }
            return Ok(out);
        }
    }
    if candidates.is_none() {
        if let Some(locators) = lookup_indexed_buckets(rtxn, env, name, &query.filter, settings)? {
            for loc in locators {
                for doc in docs_for_bucket_locator(rtxn, &db, loc, settings, low, high, None)? {
                    if expired(settings, &doc, now) || !matcher::matches(&doc, &query.filter) {
                        continue;
                    }
                    out.push(doc);
                    if query.sort.is_empty() && out.len() >= need {
                        break;
                    }
                }
                if query.sort.is_empty() && out.len() >= need {
                    break;
                }
            }
            if !query.sort.is_empty() {
                out.sort_by(|a, b| compare_docs(a, b, &query.sort));
            }
            let start = query.skip.min(out.len());
            let mut out: Vec<Value> = out.into_iter().skip(start).take(limit).collect();
            if let Some(projection) = &query.projection {
                for doc in out.iter_mut() {
                    *doc = project_document(doc, Some(projection));
                }
            }
            return Ok(out);
        }
    }
    if let Some(candidates) = candidates {
        let mut by_bucket: std::collections::HashMap<(u64, u32, i64, u16), Vec<u32>> =
            std::collections::HashMap::new();
        let mut user_ids: Vec<&String> = Vec::new();
        for id in candidates {
            match parse_row_id(id) {
                Some((hash, salt, window, serial, seq)) => by_bucket
                    .entry((hash, salt, window, serial))
                    .or_default()
                    .push(seq),
                None => user_ids.push(id),
            }
        }
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        'candidates: for ((hash, salt, window, serial), seqs) in by_bucket {
            if out.len() >= need {
                break;
            }
            // Series and time pruning still applies to the candidate bucket.
            if let Some(expected) = meta_hash {
                if hash != expected {
                    continue;
                }
            }
            let window_end = window.saturating_add(span).saturating_sub(1);
            if low.is_some_and(|low| window_end < low) || high.is_some_and(|high| window > high) {
                continue;
            }
            let wanted: std::collections::HashSet<u32> = seqs.into_iter().collect();
            let key = bucket_key(hash, salt, window, serial);
            if let Some(payload) = db.get(rtxn, &key)? {
                let (bucket, selected) = decode_bucket_inner(payload, settings, low, high)?;
                let tombs = tombstones(rtxn, &db, hash, salt, window, serial)?;
                for (row, idx) in bucket.rows.iter().zip(selected.iter()) {
                    let seq = *idx as u32;
                    if deleted_at(&bucket, &tombs, seq) || !wanted.contains(&seq) {
                        continue;
                    }
                    let mut doc = Value::Object(row.clone());
                    if !row.contains_key("_id") {
                        set_path(
                            doc.as_object_mut().expect("object"),
                            "_id",
                            Value::String(row_id(hash, salt, window, serial, seq)),
                        );
                    }
                    if expired(settings, &doc, now) || !matcher::matches(&doc, &query.filter) {
                        continue;
                    }
                    let id = doc
                        .get("_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    if seen.insert(id) {
                        out.push(doc);
                        if out.len() >= need {
                            break 'candidates;
                        }
                    }
                }
            } else {
                for seq in wanted {
                    let Some(payload) = db.get(rtxn, &row_key(hash, salt, window, serial, seq))?
                    else {
                        continue;
                    };
                    let row: Map<String, Value> = serde_json::from_slice(payload)?;
                    let mut doc = Value::Object(row);
                    if !doc
                        .as_object()
                        .is_some_and(|object| object.contains_key("_id"))
                    {
                        set_path(
                            doc.as_object_mut().expect("object"),
                            "_id",
                            Value::String(row_id(hash, salt, window, serial, seq)),
                        );
                    }
                    if expired(settings, &doc, now) || !matcher::matches(&doc, &query.filter) {
                        continue;
                    }
                    let id = doc
                        .get("_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    if seen.insert(id) {
                        out.push(doc);
                        if out.len() >= need {
                            break 'candidates;
                        }
                    }
                }
            }
        }
        for id in user_ids {
            if out.len() >= need {
                break;
            }
            if let Some(doc) = find_by_id(rtxn, env, name, settings, id)? {
                if matcher::matches(&doc, &query.filter) && seen.insert(id.clone()) {
                    out.push(doc);
                }
            }
        }
    }
    if candidates.is_none() {
        for item in db.iter(rtxn)? {
            let (key, payload) = item?;
            if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
                return Err(Error::Timeout("operation exceeded maxTimeMS".into()));
            }
            match key.first() {
                Some(&KEY_BUCKET) => {
                    if let Some(expected) = meta_hash {
                        if key_parts(key).map(|parts| parts.0) != Some(expected) {
                            continue;
                        }
                    }
                    let (_, start, end, meta) = bucket_header(payload, settings)?;
                    if low.is_some_and(|low| end < low) || high.is_some_and(|high| start > high) {
                        continue;
                    }
                    // Prune whole series without decoding columns: check the header
                    // meta value against the meta-field part of the filter.
                    if let Some(filter) = &meta_filter {
                        if !meta_bucket_matches(filter, settings, meta.as_ref()) {
                            continue;
                        }
                    }
                    let (bucket, selected) = decode_bucket_inner(payload, settings, low, high)?;
                    let (hash, salt, window, serial) = key_parts(key).unwrap_or((0, 0, start, 0));
                    let tombs = tombstones(rtxn, &db, hash, salt, window, serial)?;
                    for (row, idx) in bucket.rows.iter().zip(selected.iter()) {
                        if deleted_at(&bucket, &tombs, *idx as u32) {
                            continue;
                        }
                        let mut doc = Value::Object(row.clone());
                        if !row.contains_key("_id") {
                            set_path(
                                doc.as_object_mut().expect("object"),
                                "_id",
                                Value::String(row_id(hash, salt, window, serial, *idx as u32)),
                            );
                        }
                        if expired(settings, &doc, now) || !matcher::matches(&doc, &query.filter) {
                            continue;
                        }
                        out.push(doc);
                        if out.len() >= need {
                            break;
                        }
                    }
                    if out.len() >= need {
                        break;
                    }
                }
                Some(&KEY_ROW) => {
                    let Some((hash, salt, window, serial, seq)) = row_key_parts(key) else {
                        continue;
                    };
                    if let Some(expected) = meta_hash {
                        if hash != expected {
                            continue;
                        }
                    }
                    let window_end = window.saturating_add(span).saturating_sub(1);
                    if low.is_some_and(|low| window_end < low)
                        || high.is_some_and(|high| window > high)
                    {
                        continue;
                    }
                    let row: Map<String, Value> = serde_json::from_slice(payload)?;
                    let mut doc = Value::Object(row);
                    if !doc
                        .as_object()
                        .is_some_and(|object| object.contains_key("_id"))
                    {
                        set_path(
                            doc.as_object_mut().expect("object"),
                            "_id",
                            Value::String(row_id(hash, salt, window, serial, seq)),
                        );
                    }
                    if expired(settings, &doc, now) || !matcher::matches(&doc, &query.filter) {
                        continue;
                    }
                    out.push(doc);
                    if out.len() >= need {
                        break;
                    }
                }
                _ => {}
            }
        }
    }

    if !query.sort.is_empty() {
        out.sort_by(|a, b| compare_docs(a, b, &query.sort));
    }
    let start = query.skip.min(out.len());
    let mut out: Vec<Value> = out.into_iter().skip(start).take(limit).collect();
    if let Some(projection) = &query.projection {
        for doc in out.iter_mut() {
            *doc = project_document(doc, Some(projection));
        }
    }
    Ok(out)
}

/// Streams a full-scan query in fixed-size batches.
///
/// The callback receives decoded documents and can stop early by returning
/// `Ok(false)`. Bucket pruning, expiry, and the residual filter are identical
/// to [`find`], but no full result vector is retained.
pub fn scan_batches<F>(
    rtxn: &RoTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    filter: &Value,
    batch_size: usize,
    mut emit: F,
) -> Result<()>
where
    F: FnMut(Vec<Value>) -> Result<bool>,
{
    validate_filter(filter)?;
    let Some(db) =
        env.open_database::<Bytes, Bytes>(rtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?
    else {
        return Ok(());
    };
    let (low, high) = time_bounds(filter, &settings.time_field);
    let meta_hash = meta_equality_hash(filter, settings);
    let meta_filter = meta_only_filter(filter, settings);
    let now = crate::ttl::now_ms() as i64;
    let span = settings.span_ms();
    let batch_size = batch_size.max(1);
    let mut batch = Vec::with_capacity(batch_size);
    let mut flush = |batch: &mut Vec<Value>| -> Result<bool> {
        if batch.is_empty() {
            return Ok(true);
        }
        let current = std::mem::take(batch);
        emit(current)
    };
    if let Some(locators) = lookup_indexed_buckets(rtxn, env, name, filter, settings)? {
        for loc in locators {
            for doc in docs_for_bucket_locator(rtxn, &db, loc, settings, low, high, None)? {
                if expired(settings, &doc, now) || !matcher::matches(&doc, filter) {
                    continue;
                }
                batch.push(doc);
                if batch.len() >= batch_size && !flush(&mut batch)? {
                    return Ok(());
                }
            }
        }
        flush(&mut batch)?;
        return Ok(());
    }
    for item in db.iter(rtxn)? {
        let (key, payload) = item?;
        match key.first() {
            Some(&KEY_BUCKET) => {
                if let Some(expected) = meta_hash {
                    if key_parts(key).map(|parts| parts.0) != Some(expected) {
                        continue;
                    }
                }
                let (_, start, end, meta) = bucket_header(payload, settings)?;
                if low.is_some_and(|low| end < low) || high.is_some_and(|high| start > high) {
                    continue;
                }
                if let Some(filter) = &meta_filter {
                    if !meta_bucket_matches(filter, settings, meta.as_ref()) {
                        continue;
                    }
                }
                let (bucket, selected) = decode_bucket_inner(payload, settings, low, high)?;
                let (hash, salt, window, serial) = key_parts(key).unwrap_or((0, 0, start, 0));
                let tombs = tombstones(rtxn, &db, hash, salt, window, serial)?;
                for (row, idx) in bucket.rows.iter().zip(selected.iter()) {
                    if deleted_at(&bucket, &tombs, *idx as u32) {
                        continue;
                    }
                    let mut doc = Value::Object(row.clone());
                    if !row.contains_key("_id") {
                        set_path(
                            doc.as_object_mut().expect("object"),
                            "_id",
                            Value::String(row_id(hash, salt, window, serial, *idx as u32)),
                        );
                    }
                    if expired(settings, &doc, now) || !matcher::matches(&doc, filter) {
                        continue;
                    }
                    batch.push(doc);
                    if batch.len() >= batch_size && !flush(&mut batch)? {
                        return Ok(());
                    }
                }
            }
            Some(&KEY_ROW) => {
                let Some((hash, salt, window, serial, seq)) = row_key_parts(key) else {
                    continue;
                };
                if let Some(expected) = meta_hash {
                    if hash != expected {
                        continue;
                    }
                }
                let window_end = window.saturating_add(span).saturating_sub(1);
                if low.is_some_and(|low| window_end < low) || high.is_some_and(|high| window > high)
                {
                    continue;
                }
                let row: Map<String, Value> = serde_json::from_slice(payload)?;
                let mut doc = Value::Object(row);
                if !doc
                    .as_object()
                    .is_some_and(|object| object.contains_key("_id"))
                {
                    set_path(
                        doc.as_object_mut().expect("object"),
                        "_id",
                        Value::String(row_id(hash, salt, window, serial, seq)),
                    );
                }
                if expired(settings, &doc, now) || !matcher::matches(&doc, filter) {
                    continue;
                }
                batch.push(doc);
                if batch.len() >= batch_size && !flush(&mut batch)? {
                    return Ok(());
                }
            }
            _ => {}
        }
    }
    flush(&mut batch)?;
    Ok(())
}

/// Returns every live row with its `_id`, decoded from buckets. Used to build
/// and rebuild secondary indexes.
fn docs_for_key(
    rtxn: &RoTxn<'_>,
    db: &Database<Bytes, Bytes>,
    key: &[u8],
    payload: &[u8],
    settings: &Settings,
) -> Result<Vec<(String, Value)>> {
    docs_for_key_range(rtxn, db, key, payload, settings, None, None)
}

fn docs_for_key_range(
    rtxn: &RoTxn<'_>,
    db: &Database<Bytes, Bytes>,
    key: &[u8],
    payload: &[u8],
    settings: &Settings,
    low: Option<i64>,
    high: Option<i64>,
) -> Result<Vec<(String, Value)>> {
    docs_for_key_range_take(rtxn, db, key, payload, settings, low, high, None)
}

#[allow(clippy::too_many_arguments)]
fn docs_for_key_range_take(
    rtxn: &RoTxn<'_>,
    db: &Database<Bytes, Bytes>,
    key: &[u8],
    payload: &[u8],
    settings: &Settings,
    low: Option<i64>,
    high: Option<i64>,
    take: Option<(i32, usize)>,
) -> Result<Vec<(String, Value)>> {
    let mut out = Vec::new();
    match key.first() {
        Some(&KEY_BUCKET) => {
            let Some((hash, salt, window, serial)) = key_parts(key) else {
                return Ok(out);
            };
            let tombs = tombstones(rtxn, db, hash, salt, window, serial)?;
            let (bucket, selected) = if let Some((direction, limit)) = take {
                decode_bucket_top(payload, settings, low, high, direction, limit, &tombs)?
            } else {
                decode_bucket_inner(payload, settings, low, high)?
            };
            for (row, idx) in bucket.rows.iter().zip(selected.iter()) {
                if deleted_at(&bucket, &tombs, *idx as u32) {
                    continue;
                }
                let mut doc = Value::Object(row.clone());
                if !row.contains_key("_id") {
                    set_path(
                        doc.as_object_mut().expect("object"),
                        "_id",
                        Value::String(row_id(hash, salt, window, serial, *idx as u32)),
                    );
                }
                let id = doc
                    .get("_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                out.push((id, doc));
            }
        }
        Some(&KEY_ROW) => {
            let Some((hash, salt, window, serial, seq)) = row_key_parts(key) else {
                return Ok(out);
            };
            let row: Map<String, Value> = serde_json::from_slice(payload)?;
            let mut doc = Value::Object(row);
            if !doc
                .as_object()
                .is_some_and(|object| object.contains_key("_id"))
            {
                set_path(
                    doc.as_object_mut().expect("object"),
                    "_id",
                    Value::String(row_id(hash, salt, window, serial, seq)),
                );
            }
            let id = doc
                .get("_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            out.push((id, doc));
        }
        _ => {}
    }
    Ok(out)
}

/// Streams time-series rows into a caller-provided batch callback.
///
/// Unlike [`all_docs`], this never materializes the full collection: only the
/// bucket keys are listed up front, and each decoded bucket is handed to
/// `visit` before the next one is read. The callback receives the write
/// transaction so it can update index databases in place.
pub fn scan_index_docs<F>(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    batch_size: usize,
    mut visit: F,
) -> Result<()>
where
    F: FnMut(&mut RwTxn<'_>, Vec<(String, Value)>) -> Result<()>,
{
    settings.validate()?;
    let db = ensure_bucket_db(wtxn, env, name)?;
    let keys: Vec<Vec<u8>> = {
        let mut keys = Vec::new();
        for item in db.iter(&*wtxn)? {
            let (key, _) = item?;
            if matches!(key.first(), Some(&KEY_BUCKET) | Some(&KEY_ROW)) {
                keys.push(key.to_vec());
            }
        }
        keys
    };
    let batch_size = batch_size.max(1);
    let mut batch = Vec::with_capacity(batch_size);
    for key in keys {
        let Some(payload) = db.get(&*wtxn, &key)? else {
            continue;
        };
        let docs = docs_for_key(&*wtxn, &db, &key, payload, settings)?;
        batch.extend(docs);
        while batch.len() >= batch_size {
            let chunk: Vec<_> = batch.drain(..batch_size).collect();
            visit(&mut *wtxn, chunk)?;
        }
    }
    if !batch.is_empty() {
        visit(&mut *wtxn, batch)?;
    }
    Ok(())
}

/// Decodes a time column directly into milliseconds, avoiding a JSON object
/// allocation for every row in a bucket when only a narrow range is selected.
fn decode_time_column_ms(data: &[u8]) -> Result<Vec<Option<i64>>> {
    let mut pos = 0;
    std::str::from_utf8(get_bytes(data, &mut pos)?)
        .map_err(|_| corrupt("field name is not UTF-8"))?;
    let kind = *data
        .get(pos)
        .ok_or_else(|| corrupt("missing column kind"))?;
    let codec = *data
        .get(pos + 1)
        .ok_or_else(|| corrupt("missing column codec"))?;
    let flags = *data
        .get(pos + 2)
        .ok_or_else(|| corrupt("missing column flags"))?;
    pos += 3;
    let n = get_uvarint(data, &mut pos)? as usize;
    if n > HARD_BUCKET_MAX_DOCS {
        return Err(corrupt("column row count out of range"));
    }
    let mut present = vec![true; n];
    if flags & 1 != 0 {
        let mut reader = BitReader::new(
            data.get(pos..)
                .ok_or_else(|| corrupt("missing null bitmap"))?,
        );
        for slot in present.iter_mut() {
            *slot = reader.read(1)? == 1;
        }
        pos += n.div_ceil(8);
    }
    let raw = get_bytes(data, &mut pos)?;
    let deflated = codec & CODEC_DEFLATE != 0;
    let base = codec & !CODEC_DEFLATE;
    let owned;
    let payload: &[u8] = if deflated {
        owned = inflate(raw)?;
        &owned
    } else {
        raw
    };
    let count = present.iter().filter(|value| **value).count();
    let mut values: Vec<Option<i64>> = Vec::with_capacity(n);
    match (kind, base) {
        (KIND_JSON, CODEC_RAW) => {
            let mut reader = 0;
            for _ in 0..count {
                let bytes = get_bytes(payload, &mut reader)?;
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| corrupt("time column is not UTF-8 JSON"))?;
                let ms = text
                    .find("\"$numberLong\":\"")
                    .and_then(|start| {
                        let digits = &text[start + 15..];
                        let end = digits.find('"')?;
                        digits[..end].parse::<i64>().ok()
                    })
                    .or_else(|| {
                        serde_json::from_str::<Value>(text)
                            .ok()
                            .and_then(|value| time_value_ms(&value))
                    });
                values.push(ms);
            }
        }
        (KIND_I64, CODEC_RAW | CODEC_SHUFFLE) => {
            let numbers = if base == CODEC_SHUFFLE {
                decode_shuffle(payload, count)?
            } else {
                if payload.len() != count * 8 {
                    return Err(corrupt("integer column length mismatch"));
                }
                payload
                    .as_chunks::<8>()
                    .0
                    .iter()
                    .map(|chunk| u64::from_le_bytes(*chunk))
                    .collect()
            };
            values.extend(numbers.into_iter().map(|value| Some(value as i64)));
        }
        (KIND_I64, CODEC_DELTA | CODEC_DELTA_DELTA | CODEC_FOR) => {
            let numbers = match base {
                CODEC_DELTA => decode_delta(payload, count)?,
                CODEC_DELTA_DELTA => decode_delta_delta(payload, count)?,
                _ => decode_for(payload, count)?,
            };
            values.extend(numbers.into_iter().map(Some));
        }
        _ => {
            let mut reader = 0;
            let column = decode_column(data, &mut reader)?;
            return Ok(column
                .values
                .into_iter()
                .map(|value| value.as_ref().and_then(time_value_ms))
                .collect());
        }
    }
    if values.len() != count {
        return Err(corrupt("time column length mismatch"));
    }
    let mut out = Vec::with_capacity(n);
    let mut values = values.into_iter();
    for present in present {
        out.push(if present {
            values.next().flatten()
        } else {
            None
        });
    }
    Ok(out)
}

/// Decodes only the rows selected by a time range from a sealed columnar
/// bucket. Column blocks for other rows are skipped entirely, so a narrow time
/// window over a day bucket never materializes the full bucket as JSON.
fn decode_bucket_window_rows(
    data: &[u8],
    settings: &Settings,
    low: Option<i64>,
    high: Option<i64>,
    tombs: &BTreeSet<u32>,
) -> Result<Vec<(usize, Map<String, Value>)>> {
    if !valid_magic(data) {
        return Err(corrupt("bad magic"));
    }
    let version = data[4];
    if version < FORMAT_VERSION_PREV {
        return Err(corrupt("columnar window path requires format v2+"));
    }
    let mut pos = 6;
    let n = get_uvarint(data, &mut pos)? as usize;
    if n > HARD_BUCKET_MAX_DOCS {
        return Err(corrupt("row count out of range"));
    }
    let _window_start = get_ivarint(data, &mut pos)?;
    let _end = get_ivarint(data, &mut pos)?;
    let meta_raw = get_bytes(data, &mut pos)?;
    let meta: Option<Value> = if meta_raw.is_empty() {
        None
    } else {
        Some(serde_json::from_slice(meta_raw)?)
    };
    skip_zone_map(data, &mut pos, version)?;
    let block_rows = get_uvarint(data, &mut pos)? as usize;
    if block_rows == 0 || block_rows > HARD_BUCKET_MAX_DOCS {
        return Err(corrupt("column block size out of range"));
    }
    let column_count = get_uvarint(data, &mut pos)? as usize;
    if column_count > 4096 {
        return Err(corrupt("column count out of range"));
    }
    let mut columns: Vec<(String, Vec<&[u8]>)> = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let field = std::str::from_utf8(get_bytes(data, &mut pos)?)
            .map_err(|_| corrupt("field name is not UTF-8"))?
            .to_string();
        let blocks = get_uvarint(data, &mut pos)? as usize;
        if blocks > HARD_BUCKET_MAX_DOCS {
            return Err(corrupt("column block count out of range"));
        }
        let mut blobs = Vec::with_capacity(blocks);
        for _ in 0..blocks {
            blobs.push(get_bytes(data, &mut pos)?);
        }
        columns.push((field, blobs));
    }
    let mut deleted = BTreeSet::new();
    let bitmap_len = get_uvarint(data, &mut pos)? as usize;
    if bitmap_len != n.div_ceil(8) || pos + bitmap_len > data.len() {
        return Err(corrupt("malformed deletion bitmap"));
    }
    for idx in 0..n {
        if data[pos + idx / 8] & (1 << (idx % 8)) != 0 {
            deleted.insert(idx as u32);
        }
    }
    let time_index = columns
        .iter()
        .position(|(field, _)| field == &settings.time_field);
    let mut time_values = vec![None; n];
    if let Some(time_index) = time_index {
        let mut base = 0;
        for blob in &columns[time_index].1 {
            let block_end = (base + block_rows).min(n);
            let block = decode_time_column_ms(blob)?;
            for (offset, value) in block.into_iter().enumerate() {
                if base + offset < block_end {
                    time_values[base + offset] = value;
                }
            }
            base = block_end;
        }
    }
    let selected: Vec<usize> = (0..n)
        .filter(|idx| !deleted.contains(&(*idx as u32)) && !tombs.contains(&(*idx as u32)))
        .filter(|idx| {
            time_values[*idx].is_some_and(|ts| {
                low.is_none_or(|low| ts >= low) && high.is_none_or(|high| ts <= high)
            })
        })
        .collect();
    if selected.is_empty() {
        return Ok(Vec::new());
    }
    let mut positions = vec![None; n];
    for (room, index) in selected.iter().enumerate() {
        positions[*index] = Some(room);
    }
    let mut rows: Vec<Map<String, Value>> = vec![Map::new(); selected.len()];
    for (field, blobs) in &columns {
        let is_time = field == &settings.time_field;
        let paths: Vec<&str> = field.split('.').collect();
        let simple = paths.len() == 1;
        if is_time {
            for index in &selected {
                let Some(room) = positions[*index] else {
                    continue;
                };
                let Some(ms) = time_values[*index] else {
                    continue;
                };
                let value = crate::date::canonical(ms);
                if simple {
                    rows[room].insert(paths[0].to_string(), value);
                } else {
                    set_path_parts(&mut rows[room], &paths, value);
                }
            }
            continue;
        }
        let mut base = 0;
        for blob in blobs {
            let block_end = (base + block_rows).min(n);
            if !selected
                .iter()
                .any(|index| (base..block_end).contains(index))
            {
                base = block_end;
                continue;
            }
            let mut block_pos = 0;
            let block = decode_column(blob, &mut block_pos)?;
            if block.field != *field {
                return Err(corrupt("column block field mismatch"));
            }
            for (offset, value) in block.values.into_iter().enumerate() {
                let index = base + offset;
                if index >= block_end {
                    break;
                }
                let Some(room) = positions[index] else {
                    continue;
                };
                let Some(value) = value else {
                    continue;
                };
                if simple {
                    rows[room].insert(paths[0].to_string(), value);
                } else {
                    set_path_parts(&mut rows[room], &paths, value);
                }
            }
            base = block_end;
        }
    }
    if let Some(meta) = &meta {
        if let Some(field) = &settings.meta_field {
            for row in &mut rows {
                set_path(row, field, meta.clone());
            }
        }
    }
    Ok(selected.into_iter().zip(rows).collect())
}

fn docs_from_window_rows(loc: BucketLocator, rows: Vec<(usize, Map<String, Value>)>) -> Vec<Value> {
    let mut out = Vec::with_capacity(rows.len());
    for (index, row) in rows {
        let mut doc = Value::Object(row);
        if !doc
            .as_object()
            .is_some_and(|object| object.contains_key("_id"))
        {
            set_path(
                doc.as_object_mut().expect("object"),
                "_id",
                Value::String(row_id(
                    loc.hash,
                    loc.salt,
                    loc.window,
                    loc.serial,
                    index as u32,
                )),
            );
        }
        out.push(doc);
    }
    out
}

fn docs_for_bucket_window(
    rtxn: &RoTxn<'_>,
    db: &Database<Bytes, Bytes>,
    loc: BucketLocator,
    settings: &Settings,
    low: Option<i64>,
    high: Option<i64>,
) -> Result<Vec<Value>> {
    let key = bucket_locator_key(loc);
    let Some(payload) = db.get(rtxn, &key)? else {
        return docs_for_bucket_locator(rtxn, db, loc, settings, low, high, None);
    };
    if !valid_magic(payload) || payload.get(4).is_none_or(|v| *v < FORMAT_VERSION_PREV) {
        return docs_for_bucket_locator(rtxn, db, loc, settings, low, high, None);
    }
    let tombs = if payload[4] >= FORMAT_VERSION {
        BTreeSet::new()
    } else {
        tombstones(rtxn, db, loc.hash, loc.salt, loc.window, loc.serial)?
    };
    let rows = decode_bucket_window_rows(payload, settings, low, high, &tombs)?;
    Ok(docs_from_window_rows(loc, rows))
}

struct TimeFieldVisitor<'a> {
    field: &'a str,
    value: Option<Value>,
}

impl<'de, 'a> Visitor<'de> for TimeFieldVisitor<'a> {
    type Value = Option<Value>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a time-series document object")
    }

    fn visit_map<M>(mut self, mut map: M) -> std::result::Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        while let Some(key) = map.next_key::<Cow<'de, str>>()? {
            if key == self.field {
                self.value = Some(map.next_value::<Value>()?);
            } else {
                let _: IgnoredAny = map.next_value()?;
            }
        }
        Ok(self.value)
    }
}

fn payload_time_ms(payload: &[u8], field: &str) -> Option<i64> {
    let mut deserializer = serde_json::Deserializer::from_slice(payload);
    let value = deserializer
        .deserialize_map(TimeFieldVisitor { field, value: None })
        .ok()?;
    value.as_ref().and_then(time_value_ms)
}

fn docs_for_bucket_locator(
    rtxn: &RoTxn<'_>,
    db: &Database<Bytes, Bytes>,
    loc: BucketLocator,
    settings: &Settings,
    low: Option<i64>,
    high: Option<i64>,
    take: Option<(i32, usize)>,
) -> Result<Vec<Value>> {
    let key = bucket_locator_key(loc);
    if let Some(payload) = db.get(rtxn, &key)? {
        return Ok(
            docs_for_key_range_take(rtxn, db, &key, payload, settings, low, high, take)?
                .into_iter()
                .map(|(_, doc)| doc)
                .collect(),
        );
    }
    let mut out = Vec::new();
    for item in db.prefix_iter(rtxn, &row_base(loc.hash, loc.salt, loc.window, loc.serial))? {
        let (key, payload) = item?;
        if key.first() != Some(&KEY_ROW) {
            continue;
        }
        let Some((hash, salt, window, serial, seq)) = row_key_parts(key) else {
            continue;
        };
        let simple_time = !settings.time_field.contains('.');
        let quick_time = if simple_time && (low.is_some() || high.is_some()) {
            payload_time_ms(payload, &settings.time_field)
        } else {
            None
        };
        if quick_time
            .is_some_and(|ts| low.is_some_and(|low| ts < low) || high.is_some_and(|high| ts > high))
        {
            continue;
        }
        let row: Map<String, Value> = serde_json::from_slice(payload)?;
        let ts = if simple_time {
            row.get(&settings.time_field).and_then(time_value_ms)
        } else {
            extract_time(&Value::Object(row.clone()), &settings.time_field)
        };
        if ts
            .is_some_and(|ts| low.is_some_and(|low| ts < low) || high.is_some_and(|high| ts > high))
        {
            continue;
        }
        let mut doc = Value::Object(row);
        if !doc
            .as_object()
            .is_some_and(|object| object.contains_key("_id"))
        {
            set_path(
                doc.as_object_mut().expect("object"),
                "_id",
                Value::String(row_id(hash, salt, window, serial, seq)),
            );
        }
        out.push(doc);
    }
    Ok(out)
}

/// Returns every live time-series document with its `_id`.
pub fn all_docs(
    rtxn: &RoTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
) -> Result<Vec<(String, Value)>> {
    let Some(db) =
        env.open_database::<Bytes, Bytes>(rtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?
    else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for item in db.iter(rtxn)? {
        let (key, payload) = item?;
        out.extend(docs_for_key(rtxn, &db, key, payload, settings)?);
    }
    Ok(out)
}

/// Counts matching documents.
pub(crate) fn filter_is_meta_time_only(filter: &Value, settings: &Settings) -> bool {
    fn simple_operand(value: &Value) -> bool {
        value
            .as_object()
            .is_none_or(|object| object.len() == 1 && object.get("$eq").is_some())
    }
    fn simple_time(value: &Value) -> bool {
        value.as_object().is_none_or(|object| {
            object
                .keys()
                .all(|key| matches!(key.as_str(), "$gte" | "$gt" | "$lte" | "$lt" | "$eq"))
        })
    }
    fn check(value: &Value, time_field: &str, meta_field: Option<&str>) -> bool {
        let Some(object) = value.as_object() else {
            return false;
        };
        for (key, operand) in object {
            if key == "$and" {
                if !operand.as_array().is_some_and(|items| {
                    items.iter().all(|item| check(item, time_field, meta_field))
                }) {
                    return false;
                }
            } else if key == time_field {
                if !simple_time(operand) {
                    return false;
                }
            } else if meta_field == Some(key.as_str()) {
                if !simple_operand(operand) {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }
    check(filter, &settings.time_field, settings.meta_field.as_deref())
}

/// Counts live rows in one encoded bucket without materializing documents.
fn bucket_count_inner(
    data: &[u8],
    settings: &Settings,
    low: Option<i64>,
    high: Option<i64>,
    tombs: &BTreeSet<u32>,
) -> Result<u64> {
    if !valid_magic(data) {
        return Err(corrupt("bad magic"));
    }
    let version = data[4];
    if !(FORMAT_VERSION_LEGACY..=FORMAT_VERSION).contains(&version) {
        return Err(corrupt("unsupported format version"));
    }
    let mut pos = 6;
    let n = get_uvarint(data, &mut pos)? as usize;
    if n > HARD_BUCKET_MAX_DOCS {
        return Err(corrupt("row count out of range"));
    }
    let start = get_ivarint(data, &mut pos)?;
    let end = get_ivarint(data, &mut pos)?;
    let _meta = get_bytes(data, &mut pos)?;
    skip_zone_map(data, &mut pos, version)?;
    let mut time_values: Option<Vec<Option<Value>>> = None;
    if version >= 2 {
        let block_rows = get_uvarint(data, &mut pos)? as usize;
        if block_rows == 0 || block_rows > HARD_BUCKET_MAX_DOCS {
            return Err(corrupt("column block size out of range"));
        }
        let columns = get_uvarint(data, &mut pos)? as usize;
        for _ in 0..columns {
            let field = std::str::from_utf8(get_bytes(data, &mut pos)?)
                .map_err(|_| corrupt("field name is not UTF-8"))?
                .to_string();
            let blocks = get_uvarint(data, &mut pos)? as usize;
            let want = field == settings.time_field;
            let mut values = Vec::new();
            for _ in 0..blocks {
                let blob = get_bytes(data, &mut pos)?;
                if want {
                    let mut block_pos = 0;
                    let block = decode_column(blob, &mut block_pos)?;
                    values.extend(block.values);
                }
            }
            if want {
                time_values = Some(values);
            }
        }
    } else {
        let columns = get_uvarint(data, &mut pos)? as usize;
        for _ in 0..columns {
            let column = decode_column(data, &mut pos)?;
            if column.field == settings.time_field {
                time_values = Some(column.values);
            }
        }
    }
    let mut deleted = BTreeSet::new();
    let bitmap_len = get_uvarint(data, &mut pos)? as usize;
    if bitmap_len != n.div_ceil(8) || pos + bitmap_len > data.len() {
        return Err(corrupt("malformed deletion bitmap"));
    }
    for idx in 0..n {
        if data[pos + idx / 8] & (1 << (idx % 8)) != 0 {
            deleted.insert(idx as u32);
        }
    }
    deleted.extend(tombs.iter().copied());
    let fully_covered = low.is_none_or(|low| start >= low) && high.is_none_or(|high| end <= high);
    if fully_covered {
        return Ok((n as u64).saturating_sub(deleted.len() as u64));
    }
    let Some(values) = time_values else {
        let (bucket, selected) = decode_bucket_inner(data, settings, low, high)?;
        return Ok(selected
            .iter()
            .filter(|idx| !deleted_at(&bucket, tombs, **idx as u32))
            .count() as u64);
    };
    let mut count = 0u64;
    for (idx, value) in values.iter().enumerate() {
        if deleted.contains(&(idx as u32)) {
            continue;
        }
        let inside = value.as_ref().and_then(time_value_ms).is_some_and(|ts| {
            low.is_none_or(|low| ts >= low) && high.is_none_or(|high| ts <= high)
        });
        if inside {
            count += 1;
        }
    }
    Ok(count)
}

fn count_indexed(
    rtxn: &RoTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    filter: &Value,
) -> Result<Option<usize>> {
    if !filter_is_meta_time_only(filter, settings) {
        return Ok(None);
    }
    let Some(locators) = lookup_indexed_buckets(rtxn, env, name, filter, settings)? else {
        return Ok(None);
    };
    let db = env
        .open_database::<Bytes, Bytes>(rtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?
        .ok_or_else(|| corrupt("bucket database disappeared"))?;
    let (low, high) = time_bounds(filter, &settings.time_field);
    let mut total = 0u64;
    for loc in locators {
        let key = bucket_locator_key(loc);
        if let Some(payload) = db.get(rtxn, &key)? {
            let tombs = tombstones(rtxn, &db, loc.hash, loc.salt, loc.window, loc.serial)?;
            total = total.saturating_add(bucket_count_inner(payload, settings, low, high, &tombs)?);
            continue;
        }
        for item in db.prefix_iter(rtxn, &row_base(loc.hash, loc.salt, loc.window, loc.serial))? {
            let (key, payload) = item?;
            if key.first() != Some(&KEY_ROW) {
                continue;
            }
            let row: Map<String, Value> = serde_json::from_slice(payload)?;
            let ts = extract_time(&Value::Object(row), &settings.time_field);
            if ts.is_some_and(|ts| {
                low.is_none_or(|low| ts >= low) && high.is_none_or(|high| ts <= high)
            }) {
                total += 1;
            }
        }
    }
    Ok(Some(total.min(usize::MAX as u64) as usize))
}

pub fn count(
    rtxn: &RoTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    filter: &Value,
) -> Result<usize> {
    validate_filter(filter)?;
    if let Some(count) = count_indexed(rtxn, env, name, settings, filter)? {
        return Ok(count);
    }
    let query = FindQuery {
        filter: filter.clone(),
        projection: None,
        sort: Vec::new(),
        skip: 0,
        limit: None,
        comment: None,
        max_time_ms: None,
        hint: None,
    };
    Ok(find(rtxn, env, name, settings, &query)?.len())
}

/// Result of a time-based retention pass.
#[derive(Debug, Clone, Default, Serialize)]
pub struct RetainReport {
    /// Whole sealed buckets removed without decoding their columns.
    pub dropped_buckets: u64,
    /// Boundary buckets that received tombstones for expired rows.
    pub tombstoned_buckets: u64,
    /// Pending (unsealed) rows removed.
    pub dropped_rows: u64,
    pub remaining_rows: u64,
    /// Documents removed by this pass. Skipped in JSON; used to clean indexes.
    #[serde(skip)]
    pub reaped: Vec<(String, Value)>,
}

/// Recomputes open-bucket row counts and rolls pointers whose bucket is gone.
fn refresh_pointers(wtxn: &mut RwTxn<'_>, db: &Database<Bytes, Bytes>) -> Result<()> {
    let mut pointers: Vec<(Vec<u8>, Vec<u8>, u32)> = Vec::new();
    for item in db.prefix_iter(&*wtxn, &[KEY_OPEN])? {
        let (key, value) = item?;
        if let Some((bucket_key, count)) = parse_open(value) {
            pointers.push((key.to_vec(), bucket_key, count));
        }
    }
    for (pointer_key, pointer_bucket, count) in pointers {
        let Some((hash, salt, window, serial)) = key_parts(&pointer_bucket) else {
            continue;
        };
        let prefix = row_base(hash, salt, window, serial);
        let mut next_seq = 0u32;
        let mut remaining = 0u32;
        for item in db.prefix_iter(&*wtxn, &prefix)? {
            let (key, _) = item?;
            remaining += 1;
            if key.len() == 27 {
                let seq = u32::from_be_bytes(key[23..27].try_into().expect("4 bytes"));
                next_seq = next_seq.max(seq.saturating_add(1));
            }
        }
        if remaining == 0 && count > 0 {
            db.put(&mut *wtxn, &pointer_key, &encode_open(&pointer_bucket, 0))?;
        } else if next_seq != count {
            db.put(
                &mut *wtxn,
                &pointer_key,
                &encode_open(&pointer_bucket, next_seq),
            )?;
        }
    }
    Ok(())
}

/// Deletes rows matching `filter`. Returns the number of removed rows.
///
/// Sealed buckets that still contain matches are decoded once and re-encoded;
/// buckets whose every row matches are dropped by key. Pending rows are deleted
/// individually and the open-bucket counters are recomputed. With
/// `multi = false` at most one row is removed.
#[derive(Default)]
struct WriteFilter {
    by_bucket: std::collections::HashMap<(u64, u32, i64, u16), std::collections::HashSet<u32>>,
}

impl WriteFilter {
    fn from_ids(
        db: &Database<Bytes, Bytes>,
        wtxn: &RwTxn<'_>,
        ids: &std::collections::HashSet<String>,
    ) -> Result<Option<Self>> {
        let mut out = Self::default();
        for id in ids {
            if let Some((hash, salt, window, serial, seq)) = parse_row_id(id) {
                out.by_bucket
                    .entry((hash, salt, window, serial))
                    .or_default()
                    .insert(seq);
            } else {
                let Some(value) = db.get(wtxn, &user_key(id))? else {
                    return Ok(None);
                };
                if value.len() != 27 {
                    return Ok(None);
                }
                let Some((hash, salt, window, serial)) = key_parts(&value[..23]) else {
                    return Ok(None);
                };
                let Ok(seq) = value[23..27].try_into().map(u32::from_be_bytes) else {
                    return Ok(None);
                };
                out.by_bucket
                    .entry((hash, salt, window, serial))
                    .or_default()
                    .insert(seq);
            }
        }
        Ok(Some(out))
    }

    fn bucket_ok(&self, key: &[u8]) -> bool {
        key_parts(key)
            .map(|parts| (parts.0, parts.1, parts.2, parts.3))
            .is_some_and(|loc| self.by_bucket.contains_key(&loc))
    }

    fn seq_ok(&self, key: &[u8], seq: u32) -> bool {
        let loc = if key.first() == Some(&KEY_ROW) {
            row_key_parts(key).map(|parts| (parts.0, parts.1, parts.2, parts.3))
        } else {
            key_parts(key).map(|parts| (parts.0, parts.1, parts.2, parts.3))
        };
        loc.is_some_and(|loc| {
            self.by_bucket
                .get(&loc)
                .is_some_and(|seqs| seqs.contains(&seq))
        })
    }
}

pub fn delete(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    filter: &Value,
    multi: bool,
) -> Result<usize> {
    delete_inner(wtxn, env, name, settings, filter, multi, None)
}

/// Deletes rows restricted to secondary-index candidate ids.
pub fn delete_with_ids(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    filter: &Value,
    multi: bool,
    ids: &std::collections::HashSet<String>,
) -> Result<usize> {
    delete_inner(wtxn, env, name, settings, filter, multi, Some(ids))
}

fn delete_inner(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    filter: &Value,
    multi: bool,
    candidate_ids: Option<&std::collections::HashSet<String>>,
) -> Result<usize> {
    validate_filter(filter)?;
    let db =
        env.create_database::<Bytes, Bytes>(wtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?;
    let (low, high) = time_bounds(filter, &settings.time_field);
    let meta_hash = meta_equality_hash(filter, settings);
    let meta_filter = meta_only_filter(filter, settings);
    let now = crate::ttl::now_ms() as i64;
    let write_filter = match candidate_ids {
        Some(ids) => WriteFilter::from_ids(&db, &*wtxn, ids)?,
        None => None,
    };
    let mut deleted = 0usize;

    let mut buckets: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    if let Some(filter) = &write_filter {
        for (hash, salt, window, serial) in filter.by_bucket.keys() {
            let key = bucket_key(*hash, *salt, *window, *serial);
            if let Some(payload) = db.get(&*wtxn, &key)? {
                buckets.push((key, payload.to_vec()));
            }
        }
    } else {
        for item in db.iter(&*wtxn)? {
            let (key, payload) = item?;
            if key.first() == Some(&KEY_BUCKET) {
                buckets.push((key.to_vec(), payload.to_vec()));
            }
        }
    }
    for (key, payload) in buckets {
        if !multi && deleted > 0 {
            break;
        }
        if let Some(filter) = &write_filter {
            if !filter.bucket_ok(&key) {
                continue;
            }
        }
        if let Some(expected) = meta_hash {
            if key_parts(&key).map(|parts| parts.0) != Some(expected) {
                continue;
            }
        }
        let (_, start, end, meta) = bucket_header(&payload, settings)?;
        if low.is_some_and(|low| end < low) || high.is_some_and(|high| start > high) {
            continue;
        }
        if let Some(filter) = &meta_filter {
            if !meta_bucket_matches(filter, settings, meta.as_ref()) {
                continue;
            }
        }
        let (mut bucket, selected) = decode_bucket_inner(&payload, settings, None, None)?;
        let (hash, salt, window, serial) = key_parts(&key).unwrap_or((0, 0, start, 0));
        merge_tombstones(wtxn, &db, &mut bucket, hash, salt, window, serial)?;
        let mut live = 0usize;
        let mut removed_tombstones = Vec::new();
        for (row, idx) in bucket.rows.iter().zip(selected.iter()) {
            let seq = *idx as u32;
            if bucket.deleted.contains(&seq) {
                continue;
            }
            live += 1;
            if write_filter
                .as_ref()
                .is_some_and(|filter| !filter.seq_ok(&key, seq))
            {
                continue;
            }
            let mut doc = Value::Object(row.clone());
            if !row.contains_key("_id") {
                set_path(
                    doc.as_object_mut().expect("object"),
                    "_id",
                    Value::String(row_id(hash, salt, window, serial, seq)),
                );
            }
            let hit = !expired(settings, &doc, now) && matcher::matches(&doc, filter);
            if hit && (multi || deleted + removed_tombstones.len() == 0) {
                removed_tombstones.push(seq);
            }
        }
        if removed_tombstones.is_empty() {
            continue;
        }
        let removed = removed_tombstones.len();
        deleted += removed;
        bucket.deleted.extend(removed_tombstones);
        // If every row of the bucket is now deleted, drop the payload instead
        // of keeping an empty sealed bucket.
        if removed >= live {
            delete_bucket_payload(wtxn, &db, &key)?;
            drop_tombstones(wtxn, &db, hash, salt, window, serial)?;
        } else {
            persist_bucket(
                wtxn,
                &db,
                &key,
                &mut bucket,
                settings,
                (hash, salt, window, serial),
            )?;
        }
    }

    if multi || deleted == 0 {
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        if let Some(filter) = &write_filter {
            for ((hash, salt, window, serial), seqs) in &filter.by_bucket {
                for seq in seqs {
                    let key = row_key(*hash, *salt, *window, *serial, *seq);
                    if let Some(payload) = db.get(&*wtxn, &key)? {
                        rows.push((key, payload.to_vec()));
                    }
                }
            }
        } else {
            for item in db.iter(&*wtxn)? {
                let (key, payload) = item?;
                if key.first() == Some(&KEY_ROW) {
                    rows.push((key.to_vec(), payload.to_vec()));
                }
            }
        }
        for (key, payload) in rows {
            if !multi && deleted > 0 {
                break;
            }
            let Some((hash, salt, window, serial, seq)) = row_key_parts(&key) else {
                continue;
            };
            if write_filter
                .as_ref()
                .is_some_and(|filter| !filter.seq_ok(&key, seq))
            {
                continue;
            }
            if let Some(expected) = meta_hash {
                if hash != expected {
                    continue;
                }
            }
            let window_end = window.saturating_add(settings.span_ms()).saturating_sub(1);
            if low.is_some_and(|low| window_end < low) || high.is_some_and(|high| window > high) {
                continue;
            }
            let row: Map<String, Value> = serde_json::from_slice(&payload)?;
            let mut doc = Value::Object(row);
            if !doc
                .as_object()
                .is_some_and(|object| object.contains_key("_id"))
            {
                set_path(
                    doc.as_object_mut().expect("object"),
                    "_id",
                    Value::String(row_id(hash, salt, window, serial, seq)),
                );
            }
            if expired(settings, &doc, now) || !matcher::matches(&doc, filter) {
                continue;
            }
            db.delete(&mut *wtxn, &key)?;
            deleted += 1;
        }
    }

    refresh_pointers(wtxn, &db)?;
    invalidate_rollups(wtxn, env, name)?;
    Ok(deleted)
}

/// Deletes one row by `_id`. Synthetic ids are resolved without a scan.
pub fn delete_by_id(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    id: &str,
) -> Result<bool> {
    let db =
        env.create_database::<Bytes, Bytes>(wtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?;
    if let Some((hash, salt, window, serial, seq)) = parse_row_id(id) {
        if db
            .get(&*wtxn, &row_key(hash, salt, window, serial, seq))?
            .is_some()
        {
            db.delete(&mut *wtxn, &row_key(hash, salt, window, serial, seq))?;
            refresh_pointers(wtxn, &db)?;
            return Ok(true);
        }
        let sealed = bucket_key(hash, salt, window, serial);
        if db.get(&*wtxn, &sealed)?.is_some() {
            // The row exists only if its position is inside the bucket.
            let payload = db
                .get(&*wtxn, &sealed)?
                .ok_or_else(|| corrupt("bucket disappeared during delete"))?
                .to_vec();
            let (mut bucket, selected) = decode_bucket_inner(&payload, settings, None, None)?;
            if !selected.contains(&(seq as usize)) {
                return Ok(false);
            }
            merge_tombstones(wtxn, &db, &mut bucket, hash, salt, window, serial)?;
            if bucket.deleted.contains(&seq) {
                return Ok(false);
            }
            bucket.deleted.insert(seq);
            if bucket.deleted.len() >= bucket.rows.len() {
                delete_bucket_payload(wtxn, &db, &sealed)?;
                drop_tombstones(wtxn, &db, hash, salt, window, serial)?;
            } else {
                persist_bucket(
                    wtxn,
                    &db,
                    &sealed,
                    &mut bucket,
                    settings,
                    (hash, salt, window, serial),
                )?;
            }
            refresh_pointers(wtxn, &db)?;
            return Ok(true);
        }
    }
    let filter = Value::Object(Map::from_iter([(
        "_id".to_string(),
        Value::String(id.to_string()),
    )]));
    Ok(delete(wtxn, env, name, settings, &filter, false)? > 0)
}

/// Verifies every bucket: header, column decode, row count, and key layout.
pub fn verify_in(
    rtxn: &RoTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
) -> Result<(u64, u64)> {
    let Some(db) =
        env.open_database::<Bytes, Bytes>(rtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?
    else {
        return Ok((0, 0));
    };
    let mut rows = 0u64;
    let mut bytes = 0u64;
    for item in db.iter(rtxn)? {
        let (key, payload) = item?;
        match key.first() {
            Some(&KEY_OPEN) => {
                if parse_open(payload).is_none() {
                    return Err(corrupt("malformed open-bucket pointer"));
                }
            }
            Some(&KEY_ROW) => {
                let Some((hash, salt, window, serial, seq)) = row_key_parts(key) else {
                    return Err(corrupt("malformed pending row key"));
                };
                let _ = (hash, salt, window, serial, seq);
                let row: Map<String, Value> = serde_json::from_slice(payload)?;
                if extract_time(&Value::Object(row), &settings.time_field).is_none() {
                    return Err(corrupt("pending row has no time field"));
                }
                rows += 1;
                bytes += payload.len() as u64;
            }
            Some(&KEY_BUCKET) => {
                let (_, start, end, _) = bucket_header(payload, settings)?;
                if end < start {
                    return Err(corrupt("bucket end precedes window start"));
                }
                let (hash, salt, window, serial) = key_parts(key).unwrap_or((0, 0, 0, 0));
                let tombs = tombstones(rtxn, &db, hash, salt, window, serial)?;
                let (bucket, selected) = decode_bucket_inner(payload, settings, None, None)?;
                if bucket.rows.len() != selected.len() {
                    return Err(corrupt("bucket row count mismatch"));
                }
                for seq in &tombs {
                    if !selected.iter().any(|idx| *idx as u32 == *seq) {
                        return Err(corrupt("tombstone points outside its bucket"));
                    }
                }
                for seq in &bucket.deleted {
                    if !selected.iter().any(|idx| *idx as u32 == *seq) {
                        return Err(corrupt("deletion bitmap points outside its bucket"));
                    }
                }
                for (row, idx) in bucket.rows.iter().zip(selected.iter()) {
                    if deleted_at(&bucket, &tombs, *idx as u32) {
                        continue;
                    }
                    if extract_time(&Value::Object(row.clone()), &settings.time_field).is_none() {
                        return Err(corrupt("sealed row has no time field"));
                    }
                    rows += 1;
                }
                bytes += payload.len() as u64;
            }
            Some(&KEY_USER) => {
                if key.len() <= 1 {
                    return Err(corrupt("malformed user-id key"));
                }
            }
            Some(&KEY_TOMB) => {
                if key.len() != 27 {
                    return Err(corrupt("malformed tombstone key"));
                }
                let mut copy = key[..23].to_vec();
                copy[0] = KEY_BUCKET;
                if key_parts(&copy).is_none() {
                    return Err(corrupt("malformed tombstone bucket key"));
                }
            }
            Some(&KEY_INDEX_MARKER) => {
                if key.len() != 1 {
                    return Err(corrupt("malformed bucket-index marker"));
                }
            }
            Some(&KEY_INDEX_SERIES) | Some(&KEY_INDEX_TIME) => {
                if index_key_locator(key).is_none() {
                    return Err(corrupt("malformed bucket-index key"));
                }
            }
            _ => return Err(corrupt("unknown time-series key prefix")),
        }
    }
    Ok((rows, bytes))
}

/// Removes every row older than `before_ms` (exclusive).
///
/// Buckets whose whole time range is older are dropped by key, with no column
/// decode. Only buckets that straddle the cutoff are decoded and re-encoded.
/// Pending rows are inspected individually because they are stored as JSON.
pub fn retain(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    before_ms: i64,
) -> Result<RetainReport> {
    retain_inner(wtxn, env, name, settings, before_ms, false)
}

/// Retention variant that also returns the removed documents so the caller can
/// drop secondary/search/vector index entries.
pub fn retain_with_reaped(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    before_ms: i64,
) -> Result<RetainReport> {
    retain_inner(wtxn, env, name, settings, before_ms, true)
}

fn retain_inner(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    before_ms: i64,
    collect_reaped: bool,
) -> Result<RetainReport> {
    let db =
        env.create_database::<Bytes, Bytes>(wtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?;
    let mut report = RetainReport::default();
    let mut buckets: Vec<(Vec<u8>, i64, i64)> = Vec::new();
    let mut open_rows: Vec<(Vec<u8>, Value)> = Vec::new();

    for item in db.iter(&*wtxn)? {
        let (key, value) = item?;
        match key.first() {
            Some(&KEY_BUCKET) => {
                let (_, start, end, _) = bucket_header(value, settings)?;
                buckets.push((key.to_vec(), start, end));
            }
            Some(&KEY_ROW) => {
                let row: Map<String, Value> = serde_json::from_slice(value)?;
                let doc = Value::Object(row);
                if let Some(ts) = extract_time(&doc, &settings.time_field) {
                    if ts < before_ms {
                        open_rows.push((key.to_vec(), doc));
                    }
                }
            }
            _ => {}
        }
    }

    for (key, start, end) in buckets {
        let Some((hash, salt, window, serial)) = key_parts(&key) else {
            continue;
        };
        if end < before_ms {
            if collect_reaped {
                let payload = db
                    .get(&*wtxn, &key)?
                    .ok_or_else(|| corrupt("bucket disappeared during retention"))?
                    .to_vec();
                let (bucket, selected) = decode_bucket_inner(&payload, settings, None, None)?;
                for (row, idx) in bucket.rows.iter().zip(selected.iter()) {
                    let seq = *idx as u32;
                    if bucket.deleted.contains(&seq) {
                        continue;
                    }
                    let doc = row_doc(row, hash, salt, window, serial, seq);
                    if let Some(id) = doc.get("_id").and_then(Value::as_str) {
                        report.reaped.push((id.to_string(), doc));
                    }
                }
            }
            delete_bucket_payload(wtxn, &db, &key)?;
            drop_tombstones(wtxn, &db, hash, salt, window, serial)?;
            report.dropped_buckets += 1;
            continue;
        }
        if start >= before_ms {
            continue;
        }
        // Boundary bucket: keep the columnar payload immutable (synthetic `_id`s
        // encode row positions) and tombstone expired rows instead.
        let payload = db
            .get(&*wtxn, &key)?
            .ok_or_else(|| corrupt("bucket disappeared during retention"))?
            .to_vec();
        let (mut bucket, selected) = decode_bucket_inner(&payload, settings, None, None)?;
        merge_tombstones(wtxn, &db, &mut bucket, hash, salt, window, serial)?;
        let mut added = 0u64;
        for (row, idx) in bucket.rows.iter().zip(selected.iter()) {
            let seq = *idx as u32;
            if bucket.deleted.contains(&seq) {
                continue;
            }
            let expired = extract_time(&Value::Object(row.clone()), &settings.time_field)
                .is_some_and(|ts| ts < before_ms);
            if expired {
                if collect_reaped {
                    let doc = row_doc(row, hash, salt, window, serial, seq);
                    if let Some(id) = doc.get("_id").and_then(Value::as_str) {
                        report.reaped.push((id.to_string(), doc));
                    }
                }
                bucket.deleted.insert(seq);
                added += 1;
            }
        }
        if added > 0 {
            report.tombstoned_buckets += 1;
        }
        let live_after = bucket.rows.len() - bucket.deleted.len();
        if live_after == 0 {
            delete_bucket_payload(wtxn, &db, &key)?;
            drop_tombstones(wtxn, &db, hash, salt, window, serial)?;
            report.dropped_buckets += 1;
        } else if added > 0 {
            persist_bucket(
                wtxn,
                &db,
                &key,
                &mut bucket,
                settings,
                (hash, salt, window, serial),
            )?;
        }
    }

    for (key, doc) in &open_rows {
        if collect_reaped {
            if let Some(id) = doc.get("_id").and_then(Value::as_str) {
                report.reaped.push((id.to_string(), doc.clone()));
            }
        }
        db.delete(&mut *wtxn, key)?;
        report.dropped_rows += 1;
    }

    refresh_pointers(wtxn, &db)?;
    report.remaining_rows = stats(wtxn, env, name, settings)?.rows;
    invalidate_rollups(wtxn, env, name)?;
    Ok(report)
}

/// Result of a time-series update.
#[derive(Debug, Clone, Default)]
pub struct UpdateReport {
    pub matched: usize,
    pub modified: usize,
    pub upserted_id: Option<String>,
    /// `(old_id, new_id)` for rows whose time or series changed. Those rows are
    /// removed and re-inserted, so their synthetic `_id` changes.
    pub moved: Vec<(String, String)>,
}

fn row_doc(
    row: &Map<String, Value>,
    hash: u64,
    salt: u32,
    window: i64,
    serial: u16,
    seq: u32,
) -> Value {
    let mut doc = Value::Object(row.clone());
    if !row.contains_key("_id") {
        set_path(
            doc.as_object_mut().expect("object"),
            "_id",
            Value::String(row_id(hash, salt, window, serial, seq)),
        );
    }
    doc
}

fn normalize_doc(doc: &mut Value) -> Result<()> {
    crate::date::normalize_document(doc)?;
    crate::numeric::normalize_document(doc)?;
    crate::bson_types::normalize_document(doc)?;
    Ok(())
}

/// Removes a synthetic `_id` that was derived from the row location.
fn strip_derived_id(doc: &mut Value) {
    let derived = doc
        .get("_id")
        .and_then(Value::as_str)
        .is_some_and(|id| parse_row_id(id).is_some());
    if derived {
        if let Some(object) = doc.as_object_mut() {
            object.remove("_id");
        }
    }
}

/// Applies `update` to matching time-series rows.
///
/// Rows whose time and series stay the same are replaced in place (their
/// synthetic `_id` remains valid). Rows whose `timeField` or `metaField`
/// changes are deleted and re-inserted, which changes their `_id`; the returned
/// [`UpdateReport::moved`] carries the id mapping.
#[allow(clippy::too_many_arguments)]
pub fn update(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    filter: &Value,
    update: &Value,
    multi: bool,
    array_filters: &[Value],
    upsert: bool,
) -> Result<UpdateReport> {
    update_inner(
        wtxn,
        env,
        name,
        settings,
        filter,
        update,
        multi,
        array_filters,
        upsert,
        None,
    )
}

/// Updates rows restricted to secondary-index candidate ids.
#[allow(clippy::too_many_arguments)]
pub fn update_with_ids(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    filter: &Value,
    update: &Value,
    multi: bool,
    array_filters: &[Value],
    upsert: bool,
    ids: &std::collections::HashSet<String>,
) -> Result<UpdateReport> {
    update_inner(
        wtxn,
        env,
        name,
        settings,
        filter,
        update,
        multi,
        array_filters,
        upsert,
        Some(ids),
    )
}

#[allow(clippy::too_many_arguments)]
fn update_inner(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    filter: &Value,
    update: &Value,
    multi: bool,
    array_filters: &[Value],
    upsert: bool,
    candidate_ids: Option<&std::collections::HashSet<String>>,
) -> Result<UpdateReport> {
    use crate::query::updater::{apply_upsert_with_options, apply_with_options, seed_from_filter};

    validate_filter(filter)?;
    let db =
        env.create_database::<Bytes, Bytes>(wtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?;
    let (low, high) = time_bounds(filter, &settings.time_field);
    let meta_hash = meta_equality_hash(filter, settings);
    let meta_filter = meta_only_filter(filter, settings);
    let now = crate::ttl::now_ms() as i64;
    let write_filter = match candidate_ids {
        Some(ids) => WriteFilter::from_ids(&db, &*wtxn, ids)?,
        None => None,
    };
    let mut report = UpdateReport::default();
    // A pure `_id` equality only needs the bucket encoded in that id.
    let direct = (!multi)
        .then(|| exact_id_filter(filter))
        .flatten()
        .and_then(parse_row_id);

    let mut buckets: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    if let Some((hash, salt, window, serial, _)) = direct {
        let key = bucket_key(hash, salt, window, serial);
        if let Some(payload) = db.get(&*wtxn, &key)? {
            buckets.push((key, payload.to_vec()));
        }
    } else if let Some(filter) = &write_filter {
        for (hash, salt, window, serial) in filter.by_bucket.keys() {
            let key = bucket_key(*hash, *salt, *window, *serial);
            if let Some(payload) = db.get(&*wtxn, &key)? {
                buckets.push((key, payload.to_vec()));
            }
        }
    } else {
        for item in db.iter(&*wtxn)? {
            let (key, payload) = item?;
            if key.first() == Some(&KEY_BUCKET) {
                buckets.push((key.to_vec(), payload.to_vec()));
            }
        }
    }
    let mut moved_docs: Vec<(String, Value)> = Vec::new();
    for (key, payload) in buckets {
        if !multi && report.matched > 0 {
            break;
        }
        if let Some(filter) = &write_filter {
            if !filter.bucket_ok(&key) {
                continue;
            }
        }
        if let Some(expected) = meta_hash {
            if key_parts(&key).map(|parts| parts.0) != Some(expected) {
                continue;
            }
        }
        let (_, start, end, meta) = bucket_header(&payload, settings)?;
        if low.is_some_and(|low| end < low) || high.is_some_and(|high| start > high) {
            continue;
        }
        if let Some(filter) = &meta_filter {
            if !meta_bucket_matches(filter, settings, meta.as_ref()) {
                continue;
            }
        }
        let (mut bucket, selected) = decode_bucket_inner(&payload, settings, None, None)?;
        let (hash, salt, window, serial) = key_parts(&key).unwrap_or((0, 0, start, 0));
        merge_tombstones(wtxn, &db, &mut bucket, hash, salt, window, serial)?;
        let mut changed = false;
        let mut moved_out = Vec::new();
        for (pos, idx) in selected.iter().enumerate() {
            if !multi && report.matched > 0 {
                break;
            }
            let seq = *idx as u32;
            if write_filter
                .as_ref()
                .is_some_and(|filter| !filter.seq_ok(&key, seq))
            {
                continue;
            }
            if bucket.deleted.contains(&seq) {
                continue;
            }
            let row = &bucket.rows[pos];
            let had_id = row.contains_key("_id");
            let doc = row_doc(row, hash, salt, window, serial, seq);
            if expired(settings, &doc, now) || !matcher::matches(&doc, filter) {
                continue;
            }
            report.matched += 1;
            report.modified += 1;
            let old_time = extract_time(&doc, &settings.time_field);
            let old_meta = series_meta(&doc, settings);
            let mut next = doc.clone();
            apply_with_options(&mut next, update, filter, array_filters)?;
            normalize_doc(&mut next)?;
            let new_time = extract_time(&next, &settings.time_field).ok_or_else(|| {
                Error::Query(format!(
                    "time-series update cannot remove or invalidate `{}`",
                    settings.time_field
                ))
            })?;
            let new_meta = series_meta(&next, settings);
            let same = old_time == Some(new_time) && old_meta == new_meta;
            if same {
                if !had_id {
                    if let Some(object) = next.as_object_mut() {
                        object.remove("_id");
                    }
                }
                bucket.rows[pos] = next.as_object().cloned().unwrap_or_default();
                changed = true;
            } else {
                moved_out.push(seq);
                if !had_id {
                    if let Some(object) = next.as_object_mut() {
                        object.remove("_id");
                    }
                }
                let old_id = row_id(hash, salt, window, serial, seq);
                moved_docs.push((old_id, next));
            }
        }
        bucket.deleted.extend(moved_out.iter().copied());
        if changed || !moved_out.is_empty() {
            if bucket.deleted.len() >= bucket.rows.len() {
                delete_bucket_payload(wtxn, &db, &key)?;
                drop_tombstones(wtxn, &db, hash, salt, window, serial)?;
            } else {
                persist_bucket(
                    wtxn,
                    &db,
                    &key,
                    &mut bucket,
                    settings,
                    (hash, salt, window, serial),
                )?;
            }
        }
    }

    if multi || report.matched == 0 {
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        if let Some(filter) = &write_filter {
            for ((hash, salt, window, serial), seqs) in &filter.by_bucket {
                for seq in seqs {
                    let key = row_key(*hash, *salt, *window, *serial, *seq);
                    if let Some(payload) = db.get(&*wtxn, &key)? {
                        rows.push((key, payload.to_vec()));
                    }
                }
            }
        } else if let Some((hash, salt, window, serial, seq)) = direct {
            let key = row_key(hash, salt, window, serial, seq);
            if let Some(payload) = db.get(&*wtxn, &key)? {
                rows.push((key, payload.to_vec()));
            }
        } else {
            for item in db.iter(&*wtxn)? {
                let (key, payload) = item?;
                if key.first() == Some(&KEY_ROW) {
                    rows.push((key.to_vec(), payload.to_vec()));
                }
            }
        }
        for (key, payload) in rows {
            if !multi && report.matched > 0 {
                break;
            }
            let Some((hash, salt, window, serial, seq)) = row_key_parts(&key) else {
                continue;
            };
            if write_filter
                .as_ref()
                .is_some_and(|filter| !filter.seq_ok(&key, seq))
            {
                continue;
            }
            if let Some((dhash, dsalt, dwindow, dserial, dseq)) = direct {
                if (hash, salt, window, serial, seq) != (dhash, dsalt, dwindow, dserial, dseq) {
                    continue;
                }
            }
            if let Some(expected) = meta_hash {
                if hash != expected {
                    continue;
                }
            }
            let row: Map<String, Value> = serde_json::from_slice(&payload)?;
            let had_id = row.contains_key("_id");
            let doc = row_doc(&row, hash, salt, window, serial, seq);
            if expired(settings, &doc, now) || !matcher::matches(&doc, filter) {
                continue;
            }
            report.matched += 1;
            report.modified += 1;
            let old_time = extract_time(&doc, &settings.time_field);
            let old_meta = series_meta(&doc, settings);
            let mut next = doc.clone();
            apply_with_options(&mut next, update, filter, array_filters)?;
            normalize_doc(&mut next)?;
            let new_time = extract_time(&next, &settings.time_field).ok_or_else(|| {
                Error::Query(format!(
                    "time-series update cannot remove or invalidate `{}`",
                    settings.time_field
                ))
            })?;
            if old_time == Some(new_time) && old_meta == series_meta(&next, settings) {
                if !had_id {
                    if let Some(object) = next.as_object_mut() {
                        object.remove("_id");
                    }
                }
                let bytes = serde_json::to_vec(&next)?;
                db.put(&mut *wtxn, &key, &bytes)?;
            } else {
                db.delete(&mut *wtxn, &key)?;
                if !had_id {
                    if let Some(object) = next.as_object_mut() {
                        object.remove("_id");
                    }
                }
                let old_id = row_id(hash, salt, window, serial, seq);
                moved_docs.push((old_id, next));
            }
        }
    }

    refresh_pointers(wtxn, &db)?;
    for (old_id, next) in moved_docs {
        let new_id = insert(wtxn, env, name, settings, next)?;
        report.moved.push((old_id, new_id));
    }

    if report.matched == 0 && upsert {
        let mut doc = seed_from_filter(filter);
        apply_upsert_with_options(&mut doc, update, filter, array_filters)?;
        normalize_doc(&mut doc)?;
        if extract_time(&doc, &settings.time_field).is_none() {
            return Err(Error::Query(format!(
                "time-series upsert must provide the time field `{}`",
                settings.time_field
            )));
        }
        let id = insert(wtxn, env, name, settings, doc)?;
        report.upserted_id = Some(id);
    }

    invalidate_rollups(wtxn, env, name)?;
    Ok(report)
}

/// Replica-side update: replaces one row by `_id`, or inserts `after` when the
/// primary moved the row to another bucket.
pub fn replace_by_id(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    id: &str,
    after: &Value,
) -> Result<()> {
    let db =
        env.create_database::<Bytes, Bytes>(wtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?;
    let mut next = after.clone();
    strip_derived_id(&mut next);
    normalize_doc(&mut next)?;
    let Some((hash, salt, window, serial, seq)) = parse_row_id(id) else {
        // User-supplied `_id`: fall back to delete+insert semantics.
        if let Some((hash, salt, window, serial, seq)) =
            locator_by_user_id(wtxn, env, name, settings, id)?
        {
            delete_locator(wtxn, &db, settings, (hash, salt, window, serial, seq))?;
        }
        insert(wtxn, env, name, settings, next)?;
        return Ok(());
    };
    let new_time = extract_time(&next, &settings.time_field).ok_or_else(|| {
        Error::Query(format!(
            "replicated time-series row is missing `{}`",
            settings.time_field
        ))
    })?;
    let new_meta = series_meta(&next, settings);

    if let Some(payload) = db.get(&*wtxn, &row_key(hash, salt, window, serial, seq))? {
        let row: Map<String, Value> = serde_json::from_slice(payload)?;
        let old = row_doc(&row, hash, salt, window, serial, seq);
        let same = extract_time(&old, &settings.time_field) == Some(new_time)
            && series_meta(&old, settings) == new_meta;
        if same {
            let bytes = serde_json::to_vec(&next)?;
            db.put(
                &mut *wtxn,
                &row_key(hash, salt, window, serial, seq),
                &bytes,
            )?;
            return Ok(());
        }
        db.delete(&mut *wtxn, &row_key(hash, salt, window, serial, seq))?;
        insert(wtxn, env, name, settings, next)?;
        refresh_pointers(wtxn, &db)?;
        return Ok(());
    }

    let bucket = bucket_key(hash, salt, window, serial);
    if db
        .get(&*wtxn, &tomb_key(hash, salt, window, serial, seq))?
        .is_some()
    {
        return Ok(());
    }
    if let Some(payload) = db.get(&*wtxn, &bucket)?.map(|raw| raw.to_vec()) {
        let (mut decoded, selected) = decode_bucket_inner(&payload, settings, None, None)?;
        merge_tombstones(wtxn, &db, &mut decoded, hash, salt, window, serial)?;
        let Some(pos) = selected.iter().position(|idx| *idx == seq as usize) else {
            insert_replicated(wtxn, env, name, settings, id, &next)?;
            return Ok(());
        };
        if decoded.deleted.contains(&seq) {
            return Ok(());
        }
        let row = &decoded.rows[pos];
        let old = row_doc(row, hash, salt, window, serial, seq);
        let same = extract_time(&old, &settings.time_field) == Some(new_time)
            && series_meta(&old, settings) == new_meta;
        if !same {
            decoded.deleted.insert(seq);
            persist_bucket(
                wtxn,
                &db,
                &bucket,
                &mut decoded,
                settings,
                (hash, salt, window, serial),
            )?;
            insert_replicated(wtxn, env, name, settings, id, &next)?;
            return Ok(());
        }
        let mut row_map = next.as_object().cloned().unwrap_or_default();
        if let Some(field) = &settings.meta_field {
            if let Some(object) = next.as_object() {
                if let Some(meta) = get_path(&Value::Object(object.clone()), field) {
                    row_map.insert(field.clone(), meta.clone());
                }
            }
        }
        decoded.rows[pos] = row_map;
        persist_bucket(
            wtxn,
            &db,
            &bucket,
            &mut decoded,
            settings,
            (hash, salt, window, serial),
        )?;
        return Ok(());
    }

    // The primary moved the row: the old location is gone, insert the payload
    // at the primary's locator so ids stay stable across nodes.
    insert_replicated(wtxn, env, name, settings, id, &next)?;
    Ok(())
}

/// Replica-side insert that honours the primary's synthetic `_id`. Physical
/// bucket layout (serial numbers) may differ between nodes, so the row is
/// written at the exact locator encoded in `id` whenever it is synthetic.
pub fn insert_replicated(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    id: &str,
    doc: &Value,
) -> Result<String> {
    let mut doc = doc.clone();
    strip_derived_id(&mut doc);
    normalize_doc(&mut doc)?;
    let Some((hash, salt, window, serial, seq)) = parse_row_id(id) else {
        return insert(wtxn, env, name, settings, doc);
    };
    if extract_time(&doc, &settings.time_field).is_none() {
        return Err(Error::Query(format!(
            "replicated time-series row is missing `{}`",
            settings.time_field
        )));
    }
    let db = ensure_bucket_db(wtxn, env, name)?;
    let key = bucket_key(hash, salt, window, serial);
    let mut bucket = match db.get(&*wtxn, &key)? {
        Some(payload) => {
            let (mut bucket, _) = decode_bucket_inner(payload, settings, None, None)?;
            merge_tombstones(wtxn, &db, &mut bucket, hash, salt, window, serial)?;
            bucket
        }
        None => Bucket {
            sealed: false,
            window_start: window,
            end: window,
            meta: series_meta(&doc, settings),
            rows: Vec::new(),
            deleted: BTreeSet::new(),
        },
    };
    let row = doc.as_object().cloned().unwrap_or_default();
    let idx = seq as usize;
    if idx < bucket.rows.len() {
        bucket.rows[idx] = row;
    } else if idx == bucket.rows.len() {
        bucket.rows.push(row);
    } else {
        return Err(Error::Query(format!(
            "replicated time-series row `{id}` leaves a gap in its bucket"
        )));
    }
    bucket.deleted.remove(&seq);
    if let Some(time) = extract_time(&doc, &settings.time_field) {
        bucket.end = bucket.end.max(time);
    }
    persist_bucket(
        wtxn,
        &db,
        &key,
        &mut bucket,
        settings,
        (hash, salt, window, serial),
    )?;
    refresh_pointers(wtxn, &db)?;
    Ok(id.to_string())
}

type Locator = (u64, u32, i64, u16, u32);

fn locator_by_user_id(
    wtxn: &RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    id: &str,
) -> Result<Option<Locator>> {
    let Some(db) =
        env.open_database::<Bytes, Bytes>(wtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?
    else {
        return Ok(None);
    };
    for item in db.iter(wtxn)? {
        let (key, payload) = item?;
        if key.first() != Some(&KEY_ROW) {
            continue;
        }
        let row: Map<String, Value> = serde_json::from_slice(payload)?;
        if row.get("_id").and_then(Value::as_str) == Some(id) {
            return Ok(row_key_parts(key));
        }
    }
    for item in db.iter(wtxn)? {
        let (key, payload) = item?;
        if key.first() != Some(&KEY_BUCKET) {
            continue;
        }
        let Some((hash, salt, window, serial)) = key_parts(key) else {
            continue;
        };
        let (bucket, selected) = decode_bucket_inner(payload, settings, None, None)?;
        let tombs = tombstones(wtxn, &db, hash, salt, window, serial)?;
        for (row, idx) in bucket.rows.iter().zip(selected.iter()) {
            if deleted_at(&bucket, &tombs, *idx as u32) {
                continue;
            }
            if row.get("_id").and_then(Value::as_str) == Some(id) {
                return Ok(Some((hash, salt, window, serial, *idx as u32)));
            }
        }
    }
    Ok(None)
}

fn delete_locator(
    wtxn: &mut RwTxn<'_>,
    db: &Database<Bytes, Bytes>,
    settings: &Settings,
    loc: (u64, u32, i64, u16, u32),
) -> Result<()> {
    let (hash, salt, window, serial, seq) = loc;
    if db
        .get(&*wtxn, &row_key(hash, salt, window, serial, seq))?
        .is_some()
    {
        db.delete(&mut *wtxn, &row_key(hash, salt, window, serial, seq))?;
        return refresh_pointers(wtxn, db);
    }
    let key = bucket_key(hash, salt, window, serial);
    if let Some(payload) = db.get(&*wtxn, &key)?.map(|raw| raw.to_vec()) {
        let (mut bucket, _) = decode_bucket_inner(&payload, settings, None, None)?;
        merge_tombstones(wtxn, db, &mut bucket, hash, salt, window, serial)?;
        if bucket.deleted.insert(seq) {
            if bucket.deleted.len() >= bucket.rows.len() {
                delete_bucket_payload(wtxn, db, &key)?;
                drop_tombstones(wtxn, db, hash, salt, window, serial)?;
            } else {
                persist_bucket(
                    wtxn,
                    db,
                    &key,
                    &mut bucket,
                    settings,
                    (hash, salt, window, serial),
                )?;
            }
        }
    }
    refresh_pointers(wtxn, db)
}

/// Fetches one row by `_id` without scanning the collection.
///
/// Synthetic ids encode the physical location, so this is a single LMDB get
/// (pending row) or one get plus one block decode (sealed row); a miss is
/// authoritative. User-supplied ids use the `0x05` locator map. Only ids written
/// before that map existed fall back to a filtered scan.
pub fn find_by_id(
    rtxn: &RoTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    id: &str,
) -> Result<Option<Value>> {
    let Some(db) =
        env.open_database::<Bytes, Bytes>(rtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?
    else {
        return Ok(None);
    };
    let now = crate::ttl::now_ms() as i64;
    if let Some((hash, salt, window, serial, seq)) = parse_row_id(id) {
        if let Some(payload) = db.get(rtxn, &row_key(hash, salt, window, serial, seq))? {
            let row: Map<String, Value> = serde_json::from_slice(payload)?;
            let mut doc = Value::Object(row);
            if !doc
                .as_object()
                .is_some_and(|object| object.contains_key("_id"))
            {
                set_path(
                    doc.as_object_mut().expect("object"),
                    "_id",
                    Value::String(row_id(hash, salt, window, serial, seq)),
                );
            }
            if expired(settings, &doc, now) {
                return Ok(None);
            }
            return Ok(Some(doc));
        }
        if db
            .get(rtxn, &tomb_key(hash, salt, window, serial, seq))?
            .is_some()
        {
            return Ok(None);
        }
        if let Some(payload) = db.get(rtxn, &bucket_key(hash, salt, window, serial))? {
            if let Some(row) = decode_bucket_row(payload, settings, seq as usize)? {
                let mut doc = Value::Object(row);
                if !doc
                    .as_object()
                    .is_some_and(|object| object.contains_key("_id"))
                {
                    set_path(
                        doc.as_object_mut().expect("object"),
                        "_id",
                        Value::String(id.to_string()),
                    );
                }
                if expired(settings, &doc, now) {
                    return Ok(None);
                }
                return Ok(Some(doc));
            }
        }
        // A synthetic id is authoritative: a miss is a stale/deleted id.
        return Ok(None);
    }
    if let Some(value) = db.get(rtxn, &user_key(id))? {
        if value.len() == 27 {
            let seq = u32::from_be_bytes(value[23..27].try_into().expect("4 bytes"));
            let bucket = &value[..23];
            if let Some((hash, salt, window, serial)) = key_parts(bucket) {
                if let Some(row) = db.get(rtxn, &row_key(hash, salt, window, serial, seq))? {
                    let row: Map<String, Value> = serde_json::from_slice(row)?;
                    let doc = Value::Object(row);
                    if !expired(settings, &doc, now) {
                        return Ok(Some(doc));
                    }
                } else if let Some(payload) = db.get(rtxn, bucket)? {
                    if let Some(row) = decode_bucket_row(payload, settings, seq as usize)? {
                        let doc = Value::Object(row);
                        if !expired(settings, &doc, now) {
                            return Ok(Some(doc));
                        }
                    }
                }
                return Ok(None);
            }
        }
    }
    // User-supplied `_id` written before the id map existed: filtered scan.
    let query = FindQuery {
        filter: Value::Object(Map::new()),
        projection: None,
        sort: Vec::new(),
        skip: 0,
        limit: None,
        comment: None,
        max_time_ms: None,
        hint: None,
    };
    let docs = find(rtxn, env, name, settings, &query)?;
    Ok(docs
        .into_iter()
        .find(|doc| doc.get("_id").and_then(Value::as_str) == Some(id)))
}

/// Removes every bucket and series pointer for a collection.
pub fn clear(wtxn: &mut RwTxn<'_>, env: &heed::Env, name: &str) -> Result<()> {
    let Some(db) =
        env.open_database::<Bytes, Bytes>(wtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?
    else {
        return Ok(());
    };
    db.clear(wtxn)?;
    Ok(())
}

/// Computes storage statistics from the stored buckets and pending rows.
pub fn stats(rtxn: &RoTxn<'_>, env: &heed::Env, name: &str, settings: &Settings) -> Result<Stats> {
    let mut stats = Stats::default();
    let Some(db) =
        env.open_database::<Bytes, Bytes>(rtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?
    else {
        return Ok(stats);
    };
    for item in db.iter(rtxn)? {
        let (key, payload) = item?;
        match key.first() {
            Some(&KEY_OPEN) => {
                stats.series += 1;
                if parse_open(payload).is_some_and(|(_, count)| count > 0) {
                    stats.open_buckets += 1;
                }
            }
            Some(&KEY_ROW) => {
                stats.rows += 1;
                stats.open_bytes += payload.len() as u64;
                stats.raw_bytes += payload.len() as u64;
            }
            Some(&KEY_BUCKET) => {
                let (sealed, _, _, _) = bucket_header(payload, settings)?;
                let (hash, salt, window, serial) = key_parts(key).unwrap_or((0, 0, 0, 0));
                let tombs = tombstones(rtxn, &db, hash, salt, window, serial)?;
                let (bucket, selected) = decode_bucket_inner(payload, settings, None, None)?;
                stats.buckets += 1;
                if sealed {
                    stats.sealed_buckets += 1;
                } else {
                    stats.open_buckets += 1;
                }
                stats.encoded_bytes += payload.len() as u64;
                for (row, idx) in bucket.rows.iter().zip(selected.iter()) {
                    if deleted_at(&bucket, &tombs, *idx as u32) {
                        stats.deleted_rows += 1;
                        continue;
                    }
                    stats.rows += 1;
                    stats.raw_bytes += row_json_bytes(&Value::Object(row.clone()));
                }
            }
            _ => {}
        }
    }
    Ok(stats)
}

/// Resolves a row `_id` to its bucket locator and sequence.
pub fn locator_for_id(
    rtxn: &RoTxn<'_>,
    env: &heed::Env,
    name: &str,
    id: &str,
) -> Result<Option<RowLocator>> {
    if let Some(locator) = parse_row_id(id) {
        return Ok(Some(locator));
    }
    let Some(db) =
        env.open_database::<Bytes, Bytes>(rtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?
    else {
        return Ok(None);
    };
    let Some(value) = db.get(rtxn, &user_key(id))? else {
        return Ok(None);
    };
    if value.len() != 27 || value[0] != KEY_BUCKET {
        return Ok(None);
    }
    let Some((hash, salt, window, serial)) = key_parts(&value[..23]) else {
        return Ok(None);
    };
    let seq = u32::from_be_bytes(value[23..27].try_into().expect("4 bytes"));
    Ok(Some((hash, salt, window, serial, seq)))
}

/// Lists every sealed bucket key in a collection.
pub fn bucket_keys(rtxn: &RoTxn<'_>, env: &heed::Env, name: &str) -> Result<Vec<Vec<u8>>> {
    let Some(db) =
        env.open_database::<Bytes, Bytes>(rtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?
    else {
        return Ok(Vec::new());
    };
    let mut keys = Vec::new();
    for item in db.iter(rtxn)? {
        let (key, _) = item?;
        if key.first() == Some(&KEY_BUCKET) {
            keys.push(key.to_vec());
        }
    }
    Ok(keys)
}

/// Re-encodes one legacy bucket with zone maps and Bloom filters.
pub fn reencode_bucket(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    key: &[u8],
    settings: &Settings,
) -> Result<bool> {
    let db = ensure_bucket_db(wtxn, env, name)?;
    let Some(payload) = db.get(&*wtxn, key)? else {
        return Ok(false);
    };
    if payload
        .get(4)
        .is_some_and(|version| *version >= FORMAT_VERSION)
    {
        return Ok(false);
    }
    let Some((hash, salt, window, serial)) = key_parts(key) else {
        return Ok(false);
    };
    let (mut bucket, _) = decode_bucket_inner(payload, settings, None, None)?;
    persist_bucket(
        wtxn,
        &db,
        key,
        &mut bucket,
        settings,
        (hash, salt, window, serial),
    )?;
    Ok(true)
}

/// Prefix of the per-collection rollup sub-database.
pub const ROLLUP_DB_PREFIX: &str = "tsr:";
const ROLLUP_MARKER: u8 = 0x7F;
const ROLLUP_META: u8 = 0x00;
const ROLLUP_HOUR: u8 = 0x01;
const ROLLUP_DAY: u8 = 0x02;
const ROLLUP_HOUR_MS: i64 = 3_600_000;
const ROLLUP_DAY_MS: i64 = 86_400_000;

#[derive(Clone, Copy, Default)]
struct RollupRow {
    min_ts: i64,
    max_ts: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    n: u64,
}

impl RollupRow {
    fn add(&mut self, ts: i64, open: f64, high: f64, low: f64, close: f64, volume: f64) {
        if self.n == 0 {
            self.min_ts = ts;
            self.max_ts = ts;
            self.open = open;
            self.high = high;
            self.low = low;
            self.close = close;
            self.volume = volume;
            self.n = 1;
            return;
        }
        if ts < self.min_ts {
            self.min_ts = ts;
            self.open = open;
        }
        if ts > self.max_ts {
            self.max_ts = ts;
            self.close = close;
        }
        self.high = self.high.max(high);
        self.low = self.low.min(low);
        self.volume += volume;
        self.n += 1;
    }

    fn encode(self) -> [u8; 64] {
        let mut out = [0u8; 64];
        out[0..8].copy_from_slice(&self.min_ts.to_be_bytes());
        out[8..16].copy_from_slice(&self.max_ts.to_be_bytes());
        for (slot, value) in [self.open, self.high, self.low, self.close, self.volume]
            .into_iter()
            .enumerate()
        {
            out[16 + slot * 8..24 + slot * 8].copy_from_slice(&value.to_bits().to_be_bytes());
        }
        out[56..64].copy_from_slice(&self.n.to_be_bytes());
        out
    }

    fn decode(raw: &[u8]) -> Option<Self> {
        if raw.len() != 64 {
            return None;
        }
        let value = |slot: usize| -> Option<f64> {
            Some(f64::from_bits(u64::from_be_bytes(
                raw[16 + slot * 8..24 + slot * 8].try_into().ok()?,
            )))
        };
        Some(Self {
            min_ts: i64::from_be_bytes(raw[0..8].try_into().ok()?),
            max_ts: i64::from_be_bytes(raw[8..16].try_into().ok()?),
            open: value(0)?,
            high: value(1)?,
            low: value(2)?,
            close: value(3)?,
            volume: value(4)?,
            n: u64::from_be_bytes(raw[56..64].try_into().ok()?),
        })
    }
}

fn rollup_key(unit: u8, hash: u64, window: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(17);
    key.push(unit);
    key.extend_from_slice(&hash.to_be_bytes());
    key.extend_from_slice(&window.to_be_bytes());
    key
}

fn rollup_meta_key(hash: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(ROLLUP_META);
    key.extend_from_slice(&hash.to_be_bytes());
    key
}

fn rollup_ohlc(doc: &Value) -> Option<(f64, f64, f64, f64, f64)> {
    let field = |name: &str| get_path(doc, name).and_then(Value::as_f64);
    Some((
        field("open")?,
        field("high")?,
        field("low")?,
        field("close")?,
        field("volume")?,
    ))
}

pub(crate) fn extract_time_public(doc: &Value, field: &str) -> Option<i64> {
    extract_time(doc, field)
}

/// Builds hourly and daily OHLC/volume rollups for `[low, high]` and marks the
/// covered range. Existing rollups in the collection are replaced.
pub fn build_rollups(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    low: i64,
    high: i64,
) -> Result<usize> {
    let db_name = format!("{ROLLUP_DB_PREFIX}{name}");
    let db = env.create_database::<Bytes, Bytes>(wtxn, Some(&db_name))?;
    db.clear(&mut *wtxn)?;
    let filter = serde_json::json!({
        "ts": {
            "$gte": {"$date": {"$numberLong": low.to_string()}},
            "$lte": {"$date": {"$numberLong": high.to_string()}},
        }
    });
    let mut rows: std::collections::HashMap<(u8, u64, i64), RollupRow> =
        std::collections::HashMap::new();
    let mut metas: std::collections::HashMap<u64, Value> = std::collections::HashMap::new();
    scan_batches(wtxn, env, name, settings, &filter, 4096, |batch| {
        for doc in batch {
            let Some(ts) = extract_time(&doc, &settings.time_field) else {
                continue;
            };
            let Some((open, high, low_price, close, volume)) = rollup_ohlc(&doc) else {
                continue;
            };
            let meta = series_meta(&doc, settings);
            let hash = series_hash(settings, meta.as_ref());
            if let Some(meta) = meta {
                metas.insert(hash, meta);
            }
            let hour = ts.div_euclid(ROLLUP_HOUR_MS) * ROLLUP_HOUR_MS;
            let day = ts.div_euclid(ROLLUP_DAY_MS) * ROLLUP_DAY_MS;
            rows.entry((ROLLUP_HOUR, hash, hour))
                .or_default()
                .add(ts, open, high, low_price, close, volume);
            rows.entry((ROLLUP_DAY, hash, day))
                .or_default()
                .add(ts, open, high, low_price, close, volume);
        }
        Ok(true)
    })?;
    for (hash, meta) in metas {
        db.put(
            &mut *wtxn,
            &rollup_meta_key(hash),
            &serde_json::to_vec(&meta)?,
        )?;
    }
    let mut keys: Vec<_> = rows.keys().copied().collect();
    keys.sort_unstable();
    for key in &keys {
        let row = rows[key];
        db.put(&mut *wtxn, &rollup_key(key.0, key.1, key.2), &row.encode())?;
    }
    let mut marker = Vec::with_capacity(16);
    marker.extend_from_slice(&low.to_be_bytes());
    marker.extend_from_slice(&high.to_be_bytes());
    for unit in [ROLLUP_HOUR, ROLLUP_DAY] {
        db.put(&mut *wtxn, &[ROLLUP_MARKER, unit], &marker)?;
    }
    Ok(keys.len())
}

/// Loads rollup documents for a unit. Returns `None` when the covered range
/// does not fully contain the query or when a boundary rollup would include
/// rows outside the query.
#[allow(clippy::type_complexity)]
pub fn rollup_docs(
    rtxn: &RoTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    filter: &Value,
    unit: &str,
) -> Result<Option<(Vec<Value>, Vec<(i64, i64)>)>> {
    let (unit_byte, window_size) = match unit {
        "hour" => (ROLLUP_HOUR, ROLLUP_HOUR_MS),
        "day" => (ROLLUP_DAY, ROLLUP_DAY_MS),
        _ => return Ok(None),
    };
    let Some(db) =
        env.open_database::<Bytes, Bytes>(rtxn, Some(&format!("{ROLLUP_DB_PREFIX}{name}")))?
    else {
        return Ok(None);
    };
    let marker = db.get(rtxn, &[ROLLUP_MARKER, unit_byte])?;
    let Some(marker) = marker else {
        return Ok(None);
    };
    if marker.len() != 16 {
        return Ok(None);
    }
    let covered_low = i64::from_be_bytes(marker[0..8].try_into().expect("8 bytes"));
    let covered_high = i64::from_be_bytes(marker[8..16].try_into().expect("8 bytes"));
    let (low, high) = time_bounds(filter, &settings.time_field);
    let (Some(low), Some(high)) = (low, high) else {
        return Ok(None);
    };
    if low < covered_low || high > covered_high {
        return Ok(None);
    }
    let meta_hash = meta_equality_hash(filter, settings);
    let mut out = Vec::new();
    let mut partial = Vec::new();
    for item in db.iter(rtxn)? {
        let (key, raw) = item?;
        if key.len() != 17 || key[0] != unit_byte {
            continue;
        }
        let hash = u64::from_be_bytes(key[1..9].try_into().expect("8 bytes"));
        if meta_hash.is_some_and(|expected| expected != hash) {
            continue;
        }
        let window = i64::from_be_bytes(key[9..17].try_into().expect("8 bytes"));
        let Some(row) = RollupRow::decode(raw) else {
            continue;
        };
        // A rollup may only cover rows outside the requested interval.
        if row.max_ts < low || row.min_ts > high {
            continue;
        }
        // Partial boundary windows fall back to raw rows; full windows use the
        // materialized rollup.
        if row.min_ts < low || row.max_ts > high {
            partial.push((window, window.saturating_add(window_size).saturating_sub(1)));
            continue;
        }
        let meta = db
            .get(rtxn, &rollup_meta_key(hash))?
            .and_then(|raw| serde_json::from_slice::<Value>(raw).ok());
        let mut doc = Map::new();
        if let Some(field) = &settings.meta_field {
            if let Some(meta) = meta {
                doc.insert(field.clone(), meta);
            }
        }
        doc.insert(
            settings.time_field.clone(),
            serde_json::json!({"$date": {"$numberLong": window.to_string()}}),
        );
        doc.insert("open".into(), serde_json::json!(row.open));
        doc.insert("high".into(), serde_json::json!(row.high));
        doc.insert("low".into(), serde_json::json!(row.low));
        doc.insert("close".into(), serde_json::json!(row.close));
        doc.insert("volume".into(), serde_json::json!(row.volume));
        doc.insert("n".into(), serde_json::json!(row.n));
        out.push(Value::Object(doc));
    }
    out.sort_by(|a, b| {
        let ta = extract_time(a, &settings.time_field).unwrap_or(0);
        let tb = extract_time(b, &settings.time_field).unwrap_or(0);
        ta.cmp(&tb)
    });
    partial.sort_unstable();
    partial.dedup();
    Ok(Some((out, partial)))
}

/// Keeps an already-built rollup current for an inserted row. If the row falls
/// outside the covered range, the marker is removed so the next query rebuilds.
fn update_rollups_for_doc(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    doc: &Value,
) -> Result<()> {
    let Some(db) =
        env.open_database::<Bytes, Bytes>(&*wtxn, Some(&format!("{ROLLUP_DB_PREFIX}{name}")))?
    else {
        return Ok(());
    };
    let mut covered = Vec::new();
    for unit in [ROLLUP_HOUR, ROLLUP_DAY] {
        let Some(marker) = db.get(&*wtxn, &[ROLLUP_MARKER, unit])? else {
            continue;
        };
        if marker.len() != 16 {
            continue;
        }
        covered.push((
            unit,
            i64::from_be_bytes(marker[0..8].try_into().expect("8 bytes")),
            i64::from_be_bytes(marker[8..16].try_into().expect("8 bytes")),
        ));
    }
    if covered.is_empty() {
        return Ok(());
    }
    let Some(ts) = extract_time(doc, &settings.time_field) else {
        return Ok(());
    };
    let Some((open, high, low, close, volume)) = rollup_ohlc(doc) else {
        return Ok(());
    };
    let meta = series_meta(doc, settings);
    let hash = series_hash(settings, meta.as_ref());
    if let Some(meta) = meta {
        db.put(
            &mut *wtxn,
            &rollup_meta_key(hash),
            &serde_json::to_vec(&meta)?,
        )?;
    }
    for (unit, covered_low, covered_high) in covered {
        if ts < covered_low || ts > covered_high {
            invalidate_rollups(wtxn, env, name)?;
            return Ok(());
        }
        let window_size = if unit == ROLLUP_HOUR {
            ROLLUP_HOUR_MS
        } else {
            ROLLUP_DAY_MS
        };
        let window = ts.div_euclid(window_size) * window_size;
        let key = rollup_key(unit, hash, window);
        let mut row = db
            .get(&*wtxn, &key)?
            .and_then(RollupRow::decode)
            .unwrap_or_default();
        row.add(ts, open, high, low, close, volume);
        db.put(&mut *wtxn, &key, &row.encode())?;
    }
    Ok(())
}

/// Invalidates rollup coverage after a write/retention.
pub fn invalidate_rollups(wtxn: &mut RwTxn<'_>, env: &heed::Env, name: &str) -> Result<()> {
    let Some(db) =
        env.open_database::<Bytes, Bytes>(&*wtxn, Some(&format!("{ROLLUP_DB_PREFIX}{name}")))?
    else {
        return Ok(());
    };
    for unit in [ROLLUP_HOUR, ROLLUP_DAY] {
        db.delete(&mut *wtxn, &[ROLLUP_MARKER, unit])?;
    }
    Ok(())
}

/// Parses settings from stored JSON.
pub fn parse_settings(raw: &[u8]) -> Result<Settings> {
    let settings: Settings = serde_json::from_slice(raw)?;
    settings.validate()?;
    Ok(settings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn roundtrip(values: &[Option<Value>], compression: Compression) -> Column {
        let column = Column {
            field: "v".into(),
            values: values.to_vec(),
        };
        let bytes = encode_column(&column, compression).unwrap();
        let mut pos = 0;
        let decoded = decode_column(&bytes, &mut pos).unwrap();
        assert_eq!(pos, bytes.len());
        decoded
    }

    #[test]
    fn integer_columns_roundtrip() {
        for compression in [Compression::Fast, Compression::Balanced, Compression::Max] {
            let values: Vec<Option<Value>> = (0..100)
                .map(|idx| Some(Value::from(1_700_000_000_000i64 + idx * 1000)))
                .collect();
            assert_eq!(roundtrip(&values, compression).values, values);
            let sparse: Vec<Option<Value>> = (0..50)
                .map(|idx| {
                    if idx % 5 == 0 {
                        None
                    } else {
                        Some(Value::from(idx as i64 * 7 - 3))
                    }
                })
                .collect();
            assert_eq!(roundtrip(&sparse, compression).values, sparse);
        }
    }

    #[test]
    fn float_columns_roundtrip() {
        let values: Vec<Option<Value>> = (0..80)
            .map(|idx| Some(json!(21.5 + f64::from(idx) * 0.125)))
            .collect();
        for compression in [Compression::Fast, Compression::Max] {
            assert_eq!(roundtrip(&values, compression).values, values);
        }
        let zeros: Vec<Option<Value>> = (0..80).map(|_| Some(json!(0.0))).collect();
        assert_eq!(roundtrip(&zeros, Compression::Balanced).values, zeros);
    }

    #[test]
    fn string_and_bool_columns_roundtrip() {
        let strings: Vec<Option<Value>> = (0..60)
            .map(|idx| Some(Value::from(if idx % 2 == 0 { "on" } else { "off" })))
            .collect();
        assert_eq!(roundtrip(&strings, Compression::Balanced).values, strings);
        let bools: Vec<Option<Value>> =
            (0..33).map(|idx| Some(Value::Bool(idx % 3 == 0))).collect();
        assert_eq!(roundtrip(&bools, Compression::Balanced).values, bools);
        let mixed: Vec<Option<Value>> = vec![
            Some(json!({"a": 1})),
            None,
            Some(json!([1, 2, 3])),
            Some(json!("text")),
        ];
        assert_eq!(roundtrip(&mixed, Compression::Balanced).values, mixed);
    }

    #[test]
    fn bucket_roundtrip_with_meta() {
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Settings::default()
        };
        let mut bucket = Bucket {
            sealed: false,
            window_start: 3_600_000,
            end: 3_600_000,
            meta: None,
            rows: Vec::new(),
            deleted: BTreeSet::new(),
        };
        bucket.meta = Some(json!({"id": "sensor-1"}));
        for idx in 0..10i64 {
            let mut row = Map::new();
            row.insert(
                "ts".into(),
                json!({"$date": {"$numberLong": (3_600_000 + idx).to_string()}}),
            );
            row.insert("device".into(), json!({"id": "sensor-1"}));
            row.insert("temp".into(), json!(20.0 + idx as f64));
            bucket.rows.push(row);
        }
        bucket.end = 3_600_009;
        let encoded = encode_bucket(&bucket, &settings).unwrap();
        let (sealed, start, end) = bucket_bounds(&encoded).unwrap();
        assert!(!sealed);
        assert_eq!(start, 3_600_000);
        assert_eq!(end, 3_600_009);
        let decoded = decode_bucket(&encoded, &settings).unwrap();
        assert_eq!(decoded.rows.len(), 10);
        assert_eq!(decoded.rows[3]["device"], json!({"id": "sensor-1"}));
        assert_eq!(decoded.rows[3]["temp"], json!(23.0));
    }

    #[test]
    fn end_to_end_insert_count_and_find() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::store::Store::open(dir.path()).unwrap();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            granularity: Granularity::Seconds,
            ..Settings::default()
        };
        let collection = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        for idx in 0..250i64 {
            let ts = 1_700_000_000_000i64 + idx * 1000;
            collection
                .insert_one(json!({
                    "ts": {"$date": {"$numberLong": ts.to_string()}},
                    "device": {"id": if idx % 2 == 0 { "a" } else { "b" }},
                    "temp": 20.0 + f64::from(idx as i32) * 0.1,
                    "level": idx % 4,
                }))
                .unwrap();
        }
        assert_eq!(collection.count(&json!({})).unwrap(), 250);

        let query = FindQuery {
            filter: json!({
                "ts": {
                    "$gte": {"$date": {"$numberLong": "1700000050000"}},
                    "$lt": {"$date": {"$numberLong": "1700000060000"}}
                },
                "device": {"id": "a"}
            }),
            projection: Some(json!({"temp": 1})),
            sort: vec![("ts".into(), -1)],
            skip: 0,
            limit: Some(5),
            comment: None,
            max_time_ms: None,
            hint: None,
        };
        let docs = collection.find(&query).unwrap();
        assert!(!docs.is_empty() && docs.len() <= 5);
        assert!(docs.iter().all(|doc| doc.get("_id").is_some()));
        assert!(docs.iter().all(|doc| doc.get("device").is_none()));

        let by_id = docs[0]["_id"].as_str().unwrap().to_string();
        let found = collection.find_by_key(&by_id).unwrap();
        assert!(found.is_some());

        assert_eq!(store.seal_timeseries("metrics").unwrap(), 2);
        let stats = collection.timeseries_stats().unwrap().unwrap();
        assert_eq!(stats.rows, 250);
        assert_eq!(stats.open_bytes, 0);
        assert!(
            stats.compression_ratio() > 2.0,
            "ratio {}",
            stats.compression_ratio()
        );

        let updated = collection
            .update(&json!({}), &json!({"$set": {"x": 1}}), false)
            .unwrap();
        assert_eq!(updated, 1);
        assert!(collection.create_index("level", false).is_ok());
        assert!(collection.drop_index("level").unwrap());
        assert_eq!(collection.delete(&json!({}), false).unwrap(), 1);
        assert_eq!(collection.count(&json!({})).unwrap(), 249);

        store.drop_collection("metrics").unwrap();
        assert!(store.collection_profile("metrics").unwrap().is_none());
    }

    #[test]
    fn retention_drops_old_buckets_and_filters_the_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::store::Store::open(dir.path()).unwrap();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            granularity: Granularity::Seconds,
            ..Settings::default()
        };
        let collection = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let hour = 3_600_000i64;
        let base = 1_700_000_000_000i64;
        // Three hourly windows: one sample every two minutes.
        for idx in 0..90i64 {
            let ts = base + idx * 120_000;
            collection
                .insert_one(json!({
                    "ts": {"$date": {"$numberLong": ts.to_string()}},
                    "device": {"id": "a"},
                    "value": idx,
                }))
                .unwrap();
        }
        store.seal_timeseries("metrics").unwrap();
        assert_eq!(collection.count(&json!({})).unwrap(), 90);

        // Drop the first window entirely, keep the second half of the middle
        // window, and leave the third window untouched.
        let cutoff = base + hour + 30 * 60_000;
        let report = store.retain_timeseries("metrics", cutoff).unwrap();
        assert_eq!(report.dropped_buckets, 1);
        assert_eq!(report.tombstoned_buckets, 1);
        let remaining = collection.count(&json!({})).unwrap();
        assert_eq!(remaining, 15 + 30);
        assert_eq!(report.remaining_rows, remaining as u64);

        let docs = collection
            .find(&FindQuery {
                filter: json!({"ts": {"$lt": {"$date": {"$numberLong": cutoff.to_string()}}}}),
                projection: None,
                sort: Vec::new(),
                skip: 0,
                limit: None,
                comment: None,
                max_time_ms: None,
                hint: None,
            })
            .unwrap();
        assert!(docs.is_empty());
    }

    #[test]
    fn aggregation_groups_time_series_documents() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::store::Store::open(dir.path()).unwrap();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Settings::default()
        };
        let collection = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        for idx in 0..100i64 {
            collection
                .insert_one(json!({
                    "ts": {"$date": {"$numberLong": (1_700_000_000_000i64 + idx * 1000).to_string()}},
                    "device": {"id": "a"},
                    "value": idx,
                }))
                .unwrap();
        }
        let grouped = collection
            .aggregate(&[
                json!({"$match": {"device.id": "a"}}),
                json!({"$group": {"_id": "$device.id", "count": {"$sum": 1}, "total": {"$sum": "$value"}}}),
            ])
            .unwrap();
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[0]["count"], 100);
        assert_eq!(grouped[0]["total"], 4950);
    }

    #[test]
    fn replication_applies_buckets_settings_and_retention() {
        use crate::replication::{ReplicationChange, ReplicationOp, ReplicationTxn};
        use crate::vault::CollectionProfile;

        let primary_dir = tempfile::tempdir().unwrap();
        let primary = crate::store::Store::open(primary_dir.path()).unwrap();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Settings::default()
        };
        let source = primary
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let mut inserted = Vec::new();
        for idx in 0..40i64 {
            let doc = json!({
                "ts": {"$date": {"$numberLong": (1_700_000_000_000i64 + idx * 1000).to_string()}},
                "device": {"id": "a"},
                "value": idx,
            });
            let id = source.insert_one(doc.clone()).unwrap();
            inserted.push((id, doc));
        }
        primary.seal_timeseries("metrics").unwrap();

        let mut changes = Vec::new();
        let mut ops = vec![ReplicationOp::CollectionCreate {
            name: "metrics".into(),
            profile: "timeseries".into(),
            ttl_seconds: None,
            ttl_field: None,
            ttl_purge_history: None,
            timeseries: Some(serde_json::to_string(&settings).unwrap()),
        }];
        for (id, doc) in &inserted {
            let change = changes.len();
            changes.push(ReplicationChange {
                collection: "metrics".into(),
                op: "insert".into(),
                id: id.clone(),
                before: None,
                after: Some(doc.clone()),
                vault: false,
                ttl_at: None,
            });
            ops.push(ReplicationOp::Document { change });
        }

        let replica_dir = tempfile::tempdir().unwrap();
        let replica = crate::store::Store::open(replica_dir.path()).unwrap();
        let txn = ReplicationTxn::new_with_ops(1, "primary", 0, changes, ops);
        assert!(replica.apply_replication_txn(&txn).unwrap());

        let target = replica.collection("metrics").unwrap();
        assert_eq!(target.profile(), CollectionProfile::TimeSeries);
        assert_eq!(
            target
                .timeseries_settings_in(&replica.read_txn().unwrap())
                .unwrap(),
            Some(settings.clone())
        );
        assert_eq!(target.count(&json!({})).unwrap(), 40);
        let all = |collection: &crate::collection::Collection<'_>| {
            collection
                .find(&FindQuery {
                    filter: json!({}),
                    projection: None,
                    sort: vec![("ts".into(), 1)],
                    skip: 0,
                    limit: None,
                    comment: None,
                    max_time_ms: None,
                    hint: None,
                })
                .unwrap()
        };
        let primary_docs = all(&source);
        let replica_docs = all(&target);
        assert_eq!(
            primary_docs, replica_docs,
            "replica rows must match primary"
        );

        // Delete replicates: remove one row by id on the primary and replay it.
        let doomed = source
            .find(&FindQuery {
                filter: json!({}),
                projection: None,
                sort: vec![("ts".into(), 1)],
                skip: 0,
                limit: Some(1),
                comment: None,
                max_time_ms: None,
                hint: None,
            })
            .unwrap()
            .remove(0);
        let doomed_id = doomed["_id"].as_str().unwrap().to_string();
        assert_eq!(source.delete(&json!({"_id": doomed_id}), false).unwrap(), 1);
        let delete_txn = ReplicationTxn::new_with_ops(
            2,
            "primary",
            0,
            vec![ReplicationChange {
                collection: "metrics".into(),
                op: "delete".into(),
                id: doomed_id.clone(),
                before: Some(doomed),
                after: None,
                vault: false,
                ttl_at: None,
            }],
            vec![ReplicationOp::Document { change: 0 }],
        );
        assert!(replica.apply_replication_txn(&delete_txn).unwrap());
        assert!(replica
            .collection("metrics")
            .unwrap()
            .find_by_key(&doomed_id)
            .unwrap()
            .is_none());
        assert_eq!(target.count(&json!({})).unwrap(), 39);

        // Retention replicates too: same cutoff, same remaining rows.
        let cutoff = 1_700_000_000_000i64 + 20_000;
        let report = primary.retain_timeseries("metrics", cutoff).unwrap();
        let retain = ReplicationTxn::new_with_ops(
            3,
            "primary",
            0,
            Vec::new(),
            vec![ReplicationOp::TimeSeriesRetain {
                coll: "metrics".into(),
                before_ms: cutoff,
            }],
        );
        assert!(replica.apply_replication_txn(&retain).unwrap());
        assert_eq!(
            target.count(&json!({})).unwrap(),
            report.remaining_rows as usize
        );
        assert_eq!(target.count(&json!({})).unwrap(), 20);
    }

    #[test]
    fn delete_by_id_by_filter_and_verification() {
        use crate::collection::FindAndModifyOptions;
        let dir = tempfile::tempdir().unwrap();
        let store = crate::store::Store::open(dir.path()).unwrap();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Settings::default()
        };
        let collection = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        for idx in 0..100i64 {
            collection
                .insert_one(json!({
                    "ts": {"$date": {"$numberLong": (1_700_000_000_000i64 + idx * 1000).to_string()}},
                    "device": {"id": "a"},
                    "level": idx % 4,
                    "value": idx,
                }))
                .unwrap();
        }
        store.seal_timeseries("metrics").unwrap();
        let first = collection
            .find(&FindQuery {
                filter: json!({}),
                projection: None,
                sort: Vec::new(),
                skip: 0,
                limit: Some(1),
                comment: None,
                max_time_ms: None,
                hint: None,
            })
            .unwrap()
            .remove(0);
        let first_id = first["_id"].as_str().unwrap().to_string();
        assert!(collection.find_by_key(&first_id).unwrap().is_some());
        assert_eq!(
            collection.delete(&json!({"_id": first_id}), false).unwrap(),
            1
        );
        assert!(collection.find_by_key(&first_id).unwrap().is_none());
        assert_eq!(collection.count(&json!({})).unwrap(), 99);

        // Single delete by filter removes exactly one row.
        assert_eq!(collection.delete(&json!({"level": 1}), false).unwrap(), 1);
        assert_eq!(collection.count(&json!({"level": 1})).unwrap(), 24);
        // Multi delete removes the rest of that level.
        assert_eq!(collection.delete(&json!({"level": 1}), true).unwrap(), 24);
        assert_eq!(collection.count(&json!({"level": 1})).unwrap(), 0);
        assert_eq!(collection.count(&json!({})).unwrap(), 74);

        // findAndModify remove works, update does not.
        let removed = collection
            .find_and_modify(&FindAndModifyOptions {
                filter: &json!({"level": 2}),
                update: None,
                sort: &[],
                new: false,
                remove: true,
                upsert: false,
                array_filters: &[],
                projection: None,
            })
            .unwrap();
        assert!(removed.value.is_some());
        assert!(collection
            .find_and_modify(&FindAndModifyOptions {
                filter: &json!({"level": 2}),
                update: Some(&json!({"$set": {"x": 1}})),
                sort: &[],
                new: false,
                remove: false,
                upsert: false,
                array_filters: &[],
                projection: None,
            })
            .is_ok());

        let expected = collection.count(&json!({})).unwrap() as u64;
        let rtxn = store.read_txn().unwrap();
        let report = collection.verify_in_options(&rtxn, false).unwrap();
        rtxn.commit().unwrap();
        assert_eq!(report.documents, expected);
        assert!(report.indexes.is_empty());
    }

    #[test]
    fn numeric_columns_compress_well() {
        let settings = Settings::default();
        let mut bucket = Bucket {
            sealed: false,
            window_start: 0,
            end: 0,
            meta: None,
            rows: Vec::new(),
            deleted: BTreeSet::new(),
        };
        for idx in 0..512i64 {
            let mut row = Map::new();
            row.insert(
                "ts".into(),
                json!({"$date": {"$numberLong": (1_700_000_000_000i64 + idx * 1000).to_string()}}),
            );
            row.insert("value".into(), json!(1234.5 + (idx % 7) as f64 * 0.25));
            row.insert("level".into(), json!(idx % 4));
            bucket.rows.push(row);
        }
        let raw: usize = bucket
            .rows
            .iter()
            .map(|row| {
                serde_json::to_vec(&Value::Object(row.clone()))
                    .unwrap()
                    .len()
            })
            .sum();
        let encoded = encode_bucket(&bucket, &settings).unwrap();
        assert!(
            encoded.len() * 4 < raw,
            "expected at least 4x compression, got {} -> {}",
            raw,
            encoded.len()
        );
    }

    fn ts_store() -> (tempfile::TempDir, crate::store::Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::store::Store::open(dir.path()).unwrap();
        (dir, store)
    }

    fn ts_insert_range(c: &crate::collection::Collection<'_>, base: i64, rows: i64) -> Vec<String> {
        (0..rows)
            .map(|idx| {
                c.insert_one(json!({
                    "ts": {"$date": {"$numberLong": (base + idx * 1000).to_string()}},
                    "device": {"id": "a"},
                    "value": idx,
                    "counter": 2,
                    "legacy": true,
                }))
                .unwrap()
            })
            .collect()
    }

    fn ts_find_all(c: &crate::collection::Collection<'_>) -> Vec<Value> {
        c.find(&crate::collection::FindQuery {
            filter: json!({}),
            projection: None,
            sort: vec![("ts".into(), 1)],
            skip: 0,
            limit: None,
            comment: None,
            max_time_ms: None,
            hint: None,
        })
        .unwrap()
    }

    #[test]
    fn update_keeps_synthetic_ids_and_moves_on_time_or_meta_change() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let base = 1_700_000_000_000i64;
        let ids = ts_insert_range(&c, base, 6);
        store.seal_timeseries("metrics").unwrap();
        let target = ids[0].clone();

        let out = c
            .update_opts(
                &json!({"_id": target}),
                &json!({
                    "$set": {"value": 100},
                    "$inc": {"counter": 3},
                    "$rename": {"legacy": "renamed"},
                }),
                false,
                false,
            )
            .unwrap();
        assert_eq!((out.matched, out.modified), (1, 1));
        assert!(out.moved.is_empty());
        let doc = c.find_by_key(&target).unwrap().unwrap();
        assert_eq!(doc["_id"], json!(target));
        assert_eq!(doc["value"], json!(100));
        assert_eq!(doc["counter"], json!(5));
        assert_eq!(doc["renamed"], json!(true));
        assert!(doc.get("legacy").is_none());

        c.update_opts(
            &json!({"_id": target}),
            &json!({"$unset": {"renamed": ""}}),
            false,
            false,
        )
        .unwrap();
        c.update_opts(
            &json!({"_id": target}),
            &json!({"$mul": {"value": 2}}),
            false,
            false,
        )
        .unwrap();
        c.update_opts(
            &json!({"_id": target}),
            &json!([{"$set": {"piped": true}}]),
            false,
            false,
        )
        .unwrap();
        let doc = c.find_by_key(&target).unwrap().unwrap();
        assert_eq!(doc["value"], json!(200));
        assert_eq!(doc["piped"], json!(true));
        assert!(doc.get("renamed").is_none());

        // A multi pipeline update keeps every synthetic id stable.
        let out = c
            .update_opts(&json!({}), &json!([{"$set": {"tag": "x"}}]), true, false)
            .unwrap();
        assert_eq!(out.matched, 6);
        for id in &ids {
            let doc = c.find_by_key(id).unwrap().expect("id must survive");
            assert_eq!(doc["tag"], json!("x"));
        }
        assert_eq!(c.count(&json!({})).unwrap(), 6);

        // Changing the time field deletes + re-inserts the row.
        let moved_ts = base + 3_600_000;
        let out = c
            .update_opts(
                &json!({"_id": target}),
                &json!({"$set": {"ts": {"$date": {"$numberLong": moved_ts.to_string()}}}}),
                false,
                false,
            )
            .unwrap();
        assert_eq!(out.moved.len(), 1);
        let new_id = out.moved[0].1.clone();
        assert_ne!(new_id, target);
        assert!(c.find_by_key(&target).unwrap().is_none());
        let moved = c.find_by_key(&new_id).unwrap().unwrap();
        assert_eq!(moved["value"], json!(200));
        assert_eq!(c.count(&json!({})).unwrap(), 6);

        // Changing the meta field also moves the row.
        let out = c
            .update_opts(
                &json!({"_id": new_id}),
                &json!({"$set": {"device": {"id": "b"}}}),
                false,
                false,
            )
            .unwrap();
        assert_eq!(out.moved.len(), 1);
        assert!(c
            .find(&crate::collection::FindQuery {
                filter: json!({"device.id": "b"}),
                projection: None,
                sort: vec![],
                skip: 0,
                limit: None,
                comment: None,
                max_time_ms: None,
                hint: None,
            })
            .unwrap()
            .iter()
            .any(|doc| doc["_id"] == json!(out.moved[0].1)));
        assert_eq!(c.count(&json!({})).unwrap(), 6);
    }

    #[test]
    fn update_pending_rows_and_upsert() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let base = 1_700_000_000_000i64;
        let ids = ts_insert_range(&c, base, 3);
        let out = c
            .update_opts(
                &json!({"_id": ids[1]}),
                &json!({"$set": {"value": 77}}),
                false,
                false,
            )
            .unwrap();
        assert!(out.moved.is_empty());
        assert_eq!(c.find_by_key(&ids[1]).unwrap().unwrap()["value"], json!(77));

        let out = c
            .update_opts(
                &json!({"device.id": "zzz"}),
                &json!({
                    "$set": {"value": 1},
                    "$setOnInsert": {"ts": {"$date": {"$numberLong": (base + 60_000).to_string()}}},
                }),
                false,
                true,
            )
            .unwrap();
        let upserted = out.upserted_id.expect("upserted id");
        assert_eq!(c.count(&json!({})).unwrap(), 4);
        let doc = c.find_by_key(&upserted).unwrap().unwrap();
        assert_eq!(doc["value"], json!(1));
        assert_eq!(doc["device"]["id"], json!("zzz"));

        // Moving a pending row to another time window removes the old key.
        let out = c
            .update_opts(
                &json!({"_id": ids[0]}),
                &json!({"$set": {"ts": {"$date": {"$numberLong": (base + 86_400_000).to_string()}}}}),
                false,
                false,
            )
            .unwrap();
        assert_eq!(out.moved.len(), 1);
        assert!(
            c.find_by_key(&ids[0]).unwrap().is_none(),
            "{:?}",
            c.find_by_key(&ids[0]).unwrap()
        );
        assert!(c.find_by_key(&out.moved[0].1).unwrap().is_some());
        // Sealing the old bucket preserved the sequence gaps, so the surviving
        // pending ids did not shift.
        assert!(c.find_by_key(&ids[1]).unwrap().is_some());
        assert!(c.find_by_key(&ids[2]).unwrap().is_some());

        // Removing the time field from a row is rejected.
        let err = c
            .update_opts(
                &json!({"_id": ids[2]}),
                &json!({"$unset": {"ts": ""}}),
                false,
                false,
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("cannot remove or invalidate"),
            "{err}"
        );
    }

    #[test]
    fn find_and_modify_timeseries_new_remove_and_upsert() {
        use crate::collection::FindAndModifyOptions;
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let base = 1_700_000_000_000i64;
        let ids = ts_insert_range(&c, base, 3);
        store.seal_timeseries("metrics").unwrap();

        let update = json!({"$set": {"value": 42}});
        let old = c
            .find_and_modify(&FindAndModifyOptions {
                filter: &json!({"_id": ids[1]}),
                update: Some(&update),
                sort: &[],
                new: false,
                upsert: false,
                remove: false,
                array_filters: &[],
                projection: None,
            })
            .unwrap();
        assert_eq!(old.value.as_ref().unwrap()["_id"], json!(ids[1]));
        assert_eq!(old.value.as_ref().unwrap()["value"], json!(1));
        assert_eq!(old.new_id.as_deref(), Some(ids[1].as_str()));

        let new = c
            .find_and_modify(&FindAndModifyOptions {
                filter: &json!({"_id": ids[1]}),
                update: Some(&json!({"$set": {"value": 43}})),
                sort: &[],
                new: true,
                upsert: false,
                remove: false,
                array_filters: &[],
                projection: None,
            })
            .unwrap();
        assert_eq!(new.value.as_ref().unwrap()["value"], json!(43));
        assert_eq!(new.value.as_ref().unwrap()["_id"], json!(ids[1]));

        // A time change moves the row and returns the new id.
        let moved_ts = base + 7_200_000;
        let move_ts_doc = json!({"$set": {"ts": {"$date": {"$numberLong": moved_ts.to_string()}}}});
        let moved = c
            .find_and_modify(&FindAndModifyOptions {
                filter: &json!({"_id": ids[1]}),
                update: Some(&move_ts_doc),
                sort: &[],
                new: true,
                upsert: false,
                remove: false,
                array_filters: &[],
                projection: None,
            })
            .unwrap();
        let moved_id = moved.new_id.clone().unwrap();
        assert_ne!(moved_id, ids[1]);
        assert_eq!(moved.value.as_ref().unwrap()["value"], json!(43));
        assert!(c.find_by_key(&ids[1]).unwrap().is_none());

        let removed = c
            .find_and_modify(&FindAndModifyOptions {
                filter: &json!({"_id": moved_id}),
                update: None,
                sort: &[],
                new: false,
                upsert: false,
                remove: true,
                array_filters: &[],
                projection: None,
            })
            .unwrap();
        assert_eq!(removed.matched, 1);
        assert_eq!(removed.value.as_ref().unwrap()["value"], json!(43));
        assert!(c.find_by_key(&moved_id).unwrap().is_none());
        assert_eq!(c.count(&json!({})).unwrap(), 2);

        let upsert = json!({
            "$set": {"value": 9},
            "$setOnInsert": {"ts": {"$date": {"$numberLong": (base + 120_000).to_string()}}},
        });
        let created = c
            .find_and_modify(&FindAndModifyOptions {
                filter: &json!({"device.id": "u"}),
                update: Some(&upsert),
                sort: &[],
                new: true,
                upsert: true,
                remove: false,
                array_filters: &[],
                projection: None,
            })
            .unwrap();
        assert!(created.upserted_id.is_some());
        assert_eq!(created.value.as_ref().unwrap()["value"], json!(9));
        assert_eq!(c.count(&json!({})).unwrap(), 3);
    }

    #[test]
    fn secondary_index_lifecycle_and_planner() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let base = 1_700_000_000_000i64;
        for idx in 0..1_200i64 {
            c.insert_one(json!({
                "ts": {"$date": {"$numberLong": (base + idx * 1000).to_string()}},
                "device": {"id": format!("d{}", idx % 4)},
                "level": idx % 10,
                "value": idx,
            }))
            .unwrap();
        }
        c.create_index("level", false).unwrap();
        assert!(c
            .list_indexes()
            .unwrap()
            .iter()
            .any(|(name, unique)| name == "level" && !unique));

        let query = |hint: Option<Value>| crate::collection::FindQuery {
            filter: json!({"level": 7}),
            projection: None,
            sort: vec![("ts".into(), 1)],
            skip: 0,
            limit: None,
            comment: None,
            max_time_ms: None,
            hint,
        };
        let find_level = |level: i64| {
            c.find(&crate::collection::FindQuery {
                filter: json!({"level": level}),
                projection: None,
                sort: vec![("ts".into(), 1)],
                skip: 0,
                limit: None,
                comment: None,
                max_time_ms: None,
                hint: None,
            })
            .unwrap()
        };
        let indexed = c.find(&query(None)).unwrap();
        let scanned = c.find(&query(Some(json!({"$natural": 1})))).unwrap();
        assert_eq!(indexed, scanned);
        assert_eq!(indexed.len(), 120);

        // An unsorted indexed query stops after skip + limit rows.
        let limited = c
            .find(&crate::collection::FindQuery {
                filter: json!({"level": 7}),
                projection: None,
                sort: Vec::new(),
                skip: 5,
                limit: Some(7),
                comment: None,
                max_time_ms: None,
                hint: None,
            })
            .unwrap();
        assert_eq!(limited.len(), 7);
        assert!(limited.iter().all(|doc| doc["level"] == json!(7)));
        assert!(limited.iter().all(|doc| indexed
            .iter()
            .any(|candidate| candidate["_id"] == doc["_id"])));

        let plan = c.explain(&query(None)).unwrap();
        assert_eq!(plan["index_used"], json!(true), "{plan}");
        assert_eq!(plan["access"], json!("index_scan"));
        let natural = c.explain(&query(Some(json!({"$natural": 1})))).unwrap();
        assert_eq!(natural["index_used"], json!(false));

        // Updates move index entries.
        let target = indexed[0]["_id"].as_str().unwrap().to_string();
        c.update_opts(
            &json!({"_id": target}),
            &json!({"$set": {"level": 3}}),
            false,
            false,
        )
        .unwrap();
        assert_eq!(find_level(7).len(), 119);
        assert!(find_level(3).iter().any(|doc| doc["_id"] == json!(target)));

        // Deletes remove index entries.
        assert_eq!(c.delete(&json!({"_id": target}), false).unwrap(), 1);
        assert!(!find_level(3).iter().any(|doc| doc["_id"] == json!(target)));

        // Unique indexes are supported and tracked in metadata.
        c.create_index("value", true).unwrap();
        assert!(c
            .list_indexes()
            .unwrap()
            .iter()
            .any(|(name, unique)| name == "value" && *unique));
        c.drop_index("value").unwrap();

        // Reindex rebuilds the same index and keeps the planner happy.
        let report = c.reindex().unwrap();
        assert!(report.secondary.iter().any(|name| name == "level"));
        assert_eq!(
            c.find(&query(None)).unwrap(),
            c.find(&query(Some(json!({"$natural": 1})))).unwrap()
        );

        assert!(c.drop_index("level").unwrap());
        assert!(!c.drop_index("level").unwrap());
        assert_eq!(c.explain(&query(None)).unwrap()["index_used"], json!(false));
    }

    #[test]
    fn bucket_locator_indexes_follow_insert_delete_and_retention() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            bucket_max_docs: 8,
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let base = 1_700_000_000_000i64;
        let day = 86_400_000i64;
        for d in 0..5i64 {
            for device in 0..2i64 {
                for idx in 0..4i64 {
                    c.insert_one(json!({
                        "ts": {"$date": {"$numberLong": (base + d * day + idx * 60_000).to_string()}},
                        "device": format!("d{device}"),
                        "value": idx,
                    }))
                    .unwrap();
                }
            }
        }
        {
            let mut wtxn = store.write_txn().unwrap();
            seal_all(&mut wtxn, store.env(), "metrics", &settings).unwrap();
            wtxn.commit().unwrap();
        }
        let range = |from: i64, to: i64| {
            json!({
                "$gte": {"$date": {"$numberLong": from.to_string()}},
                "$lte": {"$date": {"$numberLong": to.to_string()}},
            })
        };
        let filter =
            json!({"device": "d0", "ts": range(base + 2 * day, base + 4 * day - 3_600_000)});
        let hash = series_hash(&settings, Some(&json!("d0")));
        {
            let rtxn = store.read_txn().unwrap();
            let locators =
                lookup_indexed_buckets(&rtxn, store.env(), "metrics", &filter, &settings)
                    .unwrap()
                    .expect("bucket index");
            assert!(!locators.is_empty());
            assert!(locators.iter().all(|loc| loc.hash == hash));
            let span = settings.span_ms();
            assert!(locators.iter().all(|loc| {
                loc.window.saturating_add(span).saturating_sub(1) >= base + 2 * day
                    && loc.window <= base + 4 * day - 3_600_000
            }));
            rtxn.commit().unwrap();
        }
        assert_eq!(c.count(&filter).unwrap(), 8);
        assert_eq!(
            c.find(&FindQuery {
                filter: filter.clone(),
                ..Default::default()
            })
            .unwrap()
            .len(),
            8
        );

        // Emptying a day removes its locator entries.
        assert_eq!(c.delete(&filter, true).unwrap(), 8);
        {
            let rtxn = store.read_txn().unwrap();
            let locators =
                lookup_indexed_buckets(&rtxn, store.env(), "metrics", &filter, &settings)
                    .unwrap()
                    .expect("bucket index");
            assert!(locators.is_empty());
            rtxn.commit().unwrap();
        }

        // Dropping old buckets by retention removes their locator entries too.
        let report = store.retain_timeseries("metrics", base + 2 * day).unwrap();
        assert!(report.dropped_buckets > 0);
        let old = json!({"device": "d0", "ts": range(base, base + day)});
        {
            let rtxn = store.read_txn().unwrap();
            let locators = lookup_indexed_buckets(&rtxn, store.env(), "metrics", &old, &settings)
                .unwrap()
                .expect("bucket index");
            assert!(locators.is_empty());
            assert!(crate::timeseries::verify_in(&rtxn, store.env(), "metrics", &settings).is_ok());
            rtxn.commit().unwrap();
        }
    }

    #[test]
    fn rollups_match_raw_aggregations_and_invalidate_on_write() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            granularity: Granularity::Minutes,
            bucket_max_docs: 256,
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("rolled", &settings)
            .unwrap();
        let base = 1_700_000_000_000i64;
        let day = 86_400_000i64;
        for d in 0..3i64 {
            for device in 0..2i64 {
                for h in 0..6i64 {
                    let ts = base + d * day + h * 3_600_000;
                    c.insert_one(json!({
                        "ts": {"$date": {"$numberLong": ts.to_string()}},
                        "device": format!("d{device}"),
                        "open": 10 + h,
                        "high": 12 + h,
                        "low": 9 + h,
                        "close": 11 + h,
                        "volume": 1.5,
                    }))
                    .unwrap();
                }
            }
        }
        let range = |unit: &str| {
            let trunc = |ms: i64, span: i64| ms.div_euclid(span) * span;
            let span = if unit == "hour" { 3_600_000 } else { day };
            json!({
                "$gte": {"$date": {"$numberLong": trunc(base, span).to_string()}},
                "$lte": {"$date": {"$numberLong": (trunc(base + 3 * day, span) - 1).to_string()}}
            })
        };
        let pipeline = |unit: &str| {
            vec![
                json!({"$match": {"ts": range(unit)}}),
                json!({"$sort": {"ts": 1}}),
                json!({"$group": {
                    "_id": {"device": "$device", "bucket": {"$dateTrunc": {"date": "$ts", "unit": unit}}},
                    "open": {"$first": "$open"},
                    "high": {"$max": "$high"},
                    "low": {"$min": "$low"},
                    "close": {"$last": "$close"},
                    "volume": {"$sum": "$volume"},
                    "n": {"$sum": 1}
                }}),
                json!({"$sort": {"_id": 1}}),
            ]
        };
        let raw_hour = c.aggregate(&pipeline("hour")).unwrap();
        let raw_day = c.aggregate(&pipeline("day")).unwrap();
        {
            let mut wtxn = store.write_txn().unwrap();
            build_rollups(
                &mut wtxn,
                store.env(),
                "rolled",
                &settings,
                base,
                base + 3 * day,
            )
            .unwrap();
            wtxn.commit().unwrap();
        }
        assert_eq!(c.aggregate(&pipeline("hour")).unwrap(), raw_hour);
        assert_eq!(c.aggregate(&pipeline("day")).unwrap(), raw_day);

        // Inserts keep the rollup current; a later delete invalidates it.
        let added = c
            .insert_one(json!({
                "ts": {"$date": {"$numberLong": (base + day + 90_000_000).to_string()}},
                "device": "d0", "open": 1, "high": 2, "low": 0, "close": 1, "volume": 1.0,
            }))
            .unwrap();
        let raw_after = c.aggregate(&pipeline("hour")).unwrap();
        assert_ne!(raw_after, raw_hour);
        {
            let mut wtxn = store.write_txn().unwrap();
            build_rollups(
                &mut wtxn,
                store.env(),
                "rolled",
                &settings,
                base,
                base + 3 * day,
            )
            .unwrap();
            wtxn.commit().unwrap();
        }
        assert_eq!(c.aggregate(&pipeline("hour")).unwrap(), raw_after);
        c.delete(&json!({"_id": added}), false).unwrap();
        assert_eq!(c.aggregate(&pipeline("hour")).unwrap(), raw_hour);
    }

    #[test]
    fn zone_maps_prune_without_decoding() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            granularity: Granularity::Minutes,
            bucket_max_docs: 64,
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("zones", &settings)
            .unwrap();
        let base = 1_700_000_000_000i64;
        for idx in 0..20i64 {
            c.insert_one(json!({
                "ts": {"$date": {"$numberLong": (base + idx * 60_000).to_string()}},
                "device": "d0",
                "source": if idx % 2 == 0 { "alpha" } else { "beta" },
                "value": 10 + idx,
            }))
            .unwrap();
        }
        {
            let mut wtxn = store.write_txn().unwrap();
            seal_all(&mut wtxn, store.env(), "zones", &settings).unwrap();
            wtxn.commit().unwrap();
        }
        {
            let rtxn = store.read_txn().unwrap();
            let db = store
                .env()
                .open_database::<Bytes, Bytes>(&rtxn, Some("tsb:zones"))
                .unwrap()
                .unwrap();
            let key = db
                .iter(&rtxn)
                .unwrap()
                .find_map(|item| {
                    let (key, value) = item.unwrap();
                    (key.first() == Some(&KEY_BUCKET)).then(|| (key.to_vec(), value.to_vec()))
                })
                .unwrap();
            let filter = json!({"device": "d0", "source": "gamma"});
            assert!(!bucket_zone_allows(&key.1, &settings, &filter).unwrap());
            assert_eq!(key.1[4], FORMAT_VERSION);
        }
        let check = |filter: Value| {
            let rtxn = store.read_txn().unwrap();
            let locators = lookup_indexed_buckets(&rtxn, store.env(), "zones", &filter, &settings)
                .unwrap()
                .expect("bucket index");
            rtxn.commit().unwrap();
            locators
        };
        // Bloom rejects a string that is absent from the bucket.
        assert!(check(json!({"device": "d0", "source": "gamma"})).is_empty());
        // Zone min/max rejects a numeric range entirely above the bucket.
        assert!(check(json!({"device": "d0", "value": {"$gt": 1000}})).is_empty());
        // Inside/boundary ranges survive and return the correct rows.
        assert!(!check(json!({"device": "d0", "value": 10})).is_empty());
        assert!(!check(json!({"device": "d0", "value": 29})).is_empty());
        assert_eq!(
            c.count(&json!({"device": "d0", "source": "alpha"}))
                .unwrap(),
            10
        );
        assert_eq!(
            c.count(&json!({"device": "d0", "source": "gamma"}))
                .unwrap(),
            0
        );
        assert_eq!(
            c.count(&json!({"device": "d0", "value": {"$gte": 20, "$lt": 25}}))
                .unwrap(),
            5
        );
        let row = json!({"device": "d0", "value": 15});
        assert_eq!(
            c.find(&FindQuery {
                filter: row.clone(),
                ..Default::default()
            })
            .unwrap()
            .len(),
            1
        );
        assert_eq!(c.count(&row).unwrap(), 1);
    }

    #[test]
    fn indexed_count_and_ordered_find_match_natural_scan() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            granularity: Granularity::Minutes,
            bucket_max_docs: 64,
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("fast", &settings)
            .unwrap();
        let base = 1_700_000_000_000i64;
        let day = 86_400_000i64;
        for d in 0..3i64 {
            for device in 0..2i64 {
                for idx in 0..12i64 {
                    c.insert_one(json!({
                        "ts": {"$date": {"$numberLong": (base + d * day + idx * 60_000).to_string()}},
                        "device": format!("d{device}"),
                        "value": idx,
                    }))
                    .unwrap();
                }
            }
        }
        {
            let mut wtxn = store.write_txn().unwrap();
            seal_all(&mut wtxn, store.env(), "fast", &settings).unwrap();
            wtxn.commit().unwrap();
        }
        let range = |from: i64, to: i64| {
            json!({
                "$gte": {"$date": {"$numberLong": from.to_string()}},
                "$lte": {"$date": {"$numberLong": to.to_string()}},
            })
        };
        let partial = json!({"device": "d0", "ts": range(base + day, base + 2 * day - 1)});
        assert_eq!(c.count(&partial).unwrap(), 12);
        c.delete(
            &json!({"device": "d0", "ts": range(base + day, base + day + 5 * 60_000)}),
            true,
        )
        .unwrap();
        assert_eq!(c.count(&partial).unwrap(), 6);
        assert_eq!(
            c.count(&json!({"ts": range(base + day, base + 2 * day - 1)}))
                .unwrap(),
            18
        );
        assert_eq!(c.count(&json!({"device": "d1"})).unwrap(), 36);
        assert_eq!(
            c.count(&json!({"$and": [{"device": "d1"}, {"ts": range(base, base + day - 1)}]}))
                .unwrap(),
            12
        );

        let query = FindQuery {
            filter: json!({"device": "d0", "ts": range(base, base + 3 * day - 1)}),
            sort: vec![("ts".into(), -1)],
            limit: Some(10),
            ..Default::default()
        };
        let indexed = c.find(&query).unwrap();
        assert_eq!(indexed.len(), 10);
        let times: Vec<i64> = indexed
            .iter()
            .map(|doc| extract_time(doc, "ts").unwrap())
            .collect();
        assert!(times.windows(2).all(|pair| pair[0] >= pair[1]));
        let explain = c.explain(&query).unwrap();
        assert_eq!(explain["access"], "index_scan");
        assert_eq!(explain["index"], "bucket_locator");
        assert_eq!(explain["bucket_index_used"], true);

        // Remove the marker so the same calls take the natural full-scan path.
        {
            let mut wtxn = store.write_txn().unwrap();
            let db = ensure_bucket_db(&mut wtxn, store.env(), "fast").unwrap();
            db.delete(&mut wtxn, &[KEY_INDEX_MARKER]).unwrap();
            wtxn.commit().unwrap();
        }
        assert_eq!(c.count(&partial).unwrap(), 6);
        assert_eq!(
            c.count(&json!({"ts": range(base + day, base + 2 * day - 1)}))
                .unwrap(),
            18
        );
        assert_eq!(c.count(&json!({"device": "d1"})).unwrap(), 36);
        assert_eq!(
            c.count(&json!({"$and": [{"device": "d1"}, {"ts": range(base, base + day - 1)}]}))
                .unwrap(),
            12
        );
        let natural = c.find(&query).unwrap();
        assert_eq!(indexed, natural);
    }

    #[test]
    fn window_scan_matches_find_for_time_ranges() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            granularity: Granularity::Minutes,
            bucket_max_docs: 64,
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("window_scan", &settings)
            .unwrap();
        let base = 1_700_000_000_000i64;
        let day = 86_400_000i64;
        for d in 0..3i64 {
            for device in 0..2i64 {
                for idx in 0..12i64 {
                    c.insert_one(json!({
                        "ts": {"$date": {"$numberLong": (base + d * day + idx * 60_000).to_string()}},
                        "device": format!("d{device}"),
                        "value": idx,
                    }))
                    .unwrap();
                }
            }
        }
        {
            let mut wtxn = store.write_txn().unwrap();
            seal_all(&mut wtxn, store.env(), "window_scan", &settings).unwrap();
            wtxn.commit().unwrap();
        }
        let range = |from: i64, to: i64| {
            json!({
                "$gte": {"$date": {"$numberLong": from.to_string()}},
                "$lte": {"$date": {"$numberLong": to.to_string()}},
            })
        };
        let query = FindQuery {
            filter: json!({"ts": range(base + day, base + 2 * day - 1)}),
            ..Default::default()
        };
        let rtxn = store.read_txn().unwrap();
        let mut expected = find(&rtxn, store.env(), "window_scan", &settings, &query).unwrap();
        let mut actual =
            find_window_docs(&rtxn, store.env(), "window_scan", &settings, &query).unwrap();
        let id = |doc: &Value| {
            doc.get("_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };
        expected.sort_by_key(id);
        actual.sort_by_key(id);
        assert_eq!(actual, expected);
    }

    #[test]
    fn ensure_bucket_index_backfills_legacy_collections() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("legacy", &settings)
            .unwrap();
        for idx in 0..20 {
            c.insert_one(json!({
                "ts": {"$date": {"$numberLong": (1_700_000_000_000i64 + idx * 60_000).to_string()}},
                "device": format!("d{}", idx % 2),
                "value": idx,
            }))
            .unwrap();
        }
        {
            let mut wtxn = store.write_txn().unwrap();
            seal_all(&mut wtxn, store.env(), "legacy", &settings).unwrap();
            wtxn.commit().unwrap();
        }
        {
            let mut wtxn = store.write_txn().unwrap();
            let db = ensure_bucket_db(&mut wtxn, store.env(), "legacy").unwrap();
            let keys: Vec<Vec<u8>> = db
                .iter(&wtxn)
                .unwrap()
                .filter_map(|item| {
                    let (key, _) = item.ok()?;
                    matches!(key.first(), Some(&KEY_INDEX_SERIES) | Some(&KEY_INDEX_TIME))
                        .then(|| key.to_vec())
                })
                .collect();
            for key in keys {
                db.delete(&mut wtxn, &key).unwrap();
            }
            db.delete(&mut wtxn, &[KEY_INDEX_MARKER]).unwrap();
            wtxn.commit().unwrap();
        }
        {
            let rtxn = store.read_txn().unwrap();
            assert!(!bucket_index_stats(&rtxn, store.env(), "legacy").unwrap().0);
            rtxn.commit().unwrap();
        }
        let built = {
            let mut wtxn = store.write_txn().unwrap();
            let built = ensure_bucket_index(&mut wtxn, store.env(), "legacy").unwrap();
            wtxn.commit().unwrap();
            built
        };
        assert!(built);
        let rtxn = store.read_txn().unwrap();
        let (ready, entries, bytes) = bucket_index_stats(&rtxn, store.env(), "legacy").unwrap();
        assert!(ready);
        assert!(entries > 0);
        assert!(bytes > 0);
        let locators = lookup_indexed_buckets(
            &rtxn,
            store.env(),
            "legacy",
            &json!({"device": "d0"}),
            &settings,
        )
        .unwrap()
        .expect("index");
        assert_eq!(locators.len() as u64, entries / 4);
        rtxn.commit().unwrap();
    }

    #[test]
    fn index_backfill_streams_across_many_buckets() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            bucket_max_docs: 7,
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let base = 1_700_000_000_000i64;
        for idx in 0..500i64 {
            c.insert_one(json!({
                "ts": {"$date": {"$numberLong": (base + idx * 3_600_000).to_string()}},
                "device": format!("d{}", idx % 5),
                "level": idx % 10,
                "value": idx,
            }))
            .unwrap();
        }
        assert!(c.timeseries_stats().unwrap().unwrap().buckets > 100);
        c.create_index("level", false).unwrap();
        let query = |hint: Option<Value>| crate::collection::FindQuery {
            filter: json!({"level": 7}),
            projection: None,
            sort: vec![("ts".into(), 1)],
            skip: 0,
            limit: None,
            comment: None,
            max_time_ms: None,
            hint,
        };
        let indexed = c.find(&query(None)).unwrap();
        let natural = c.find(&query(Some(json!({"$natural": 1})))).unwrap();
        assert_eq!(indexed, natural);
        assert_eq!(indexed.len(), 50);
        c.reindex().unwrap();
        assert_eq!(c.find(&query(None)).unwrap(), natural);
    }

    #[test]
    fn insert_many_txn_batches_series_and_windows() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let base = 1_700_000_000_000i64;
        let docs: Vec<Value> = (0..200i64)
            .map(|idx| {
                json!({
                    "ts": {"$date": {"$numberLong": (base + idx * 3_600_000).to_string()}},
                    "device": {"id": format!("d{}", idx % 3)},
                    "value": idx,
                })
            })
            .collect();
        let mut wtxn = store.write_txn().unwrap();
        let ids = c.insert_many_txn(&mut wtxn, &docs).unwrap();
        wtxn.commit().unwrap();
        assert_eq!(ids.len(), docs.len());
        assert_eq!(c.count(&json!({})).unwrap(), docs.len());
        for (idx, id) in ids.iter().enumerate() {
            let doc = c.find_by_key(id).unwrap().expect("inserted id");
            assert_eq!(doc["value"], json!(idx));
        }
    }

    #[test]
    fn write_candidates_reach_sealed_buckets() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let ids: Vec<String> = (0..20i64)
            .map(|idx| {
                c.insert_one(json!({"ts": idx * 60_000, "value": idx, "flag": false}))
                    .unwrap()
            })
            .collect();
        let mut wtxn = store.write_txn().unwrap();
        seal_all(&mut wtxn, store.env(), "metrics", &settings).unwrap();
        wtxn.commit().unwrap();
        let one: std::collections::HashSet<String> = [ids[3].clone()].into();
        let mut wtxn = store.write_txn().unwrap();
        let report = update_with_ids(
            &mut wtxn,
            store.env(),
            "metrics",
            &settings,
            &json!({}),
            &json!({"$set": {"flag": true}}),
            true,
            &[],
            false,
            &one,
        )
        .unwrap();
        assert_eq!(report.matched, 1);
        wtxn.commit().unwrap();
        assert_eq!(c.count(&json!({"flag": true})).unwrap(), 1);
        let mut wtxn = store.write_txn().unwrap();
        let two: std::collections::HashSet<String> = [ids[4].clone(), ids[5].clone()].into();
        assert_eq!(
            delete_with_ids(
                &mut wtxn,
                store.env(),
                "metrics",
                &settings,
                &json!({}),
                true,
                &two
            )
            .unwrap(),
            2
        );
        wtxn.commit().unwrap();
        assert_eq!(c.count(&json!({})).unwrap(), 18);
        assert_eq!(c.count(&json!({"flag": true})).unwrap(), 1);
    }

    #[test]
    fn unique_indexes_are_enforced_on_timeseries() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        c.create_index("seq", true).unwrap();
        let first = c.insert_one(json!({"ts": 1, "seq": 1})).unwrap();
        assert!(c.insert_one(json!({"ts": 2, "seq": 1})).is_err());
        assert_eq!(c.count(&json!({"seq": 1})).unwrap(), 1);
        let second = c.insert_one(json!({"ts": 3, "seq": 2})).unwrap();
        assert!(c
            .update_opts(
                &json!({"_id": second}),
                &json!({"$set": {"seq": 1}}),
                false,
                false,
            )
            .is_err());
        assert_eq!(c.count(&json!({"seq": 2})).unwrap(), 1);
        assert_eq!(c.delete(&json!({"_id": first}), false).unwrap(), 1);
        c.update_opts(
            &json!({"_id": second}),
            &json!({"$set": {"seq": 1}}),
            false,
            false,
        )
        .unwrap();
        assert!(c.insert_one(json!({"ts": 4, "seq": 1})).is_err());

        let mut wtxn = store.write_txn().unwrap();
        assert!(c
            .insert_many_txn(
                &mut wtxn,
                &[json!({"ts": 10, "seq": 9}), json!({"ts": 11, "seq": 9})],
            )
            .is_err());
        drop(wtxn);
        assert_eq!(c.count(&json!({"seq": 9})).unwrap(), 0);

        let d = store
            .create_timeseries_collection("dups", &settings)
            .unwrap();
        d.insert_one(json!({"ts": 1, "seq": 1})).unwrap();
        d.insert_one(json!({"ts": 2, "seq": 1})).unwrap();
        assert!(d.create_index("seq", true).is_err());
        assert!(d.create_index("seq", false).is_ok());
    }

    #[test]
    fn user_supplied_ids_use_the_point_id_map() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            bucket_max_docs: 64,
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        c.insert_one(json!({"_id": "user-1", "ts": 1_000, "v": 1}))
            .unwrap();
        for idx in 0..2_000i64 {
            c.insert_one(json!({"ts": 2_000 + idx * 1000, "v": idx}))
                .unwrap();
        }
        let mut wtxn = store.write_txn().unwrap();
        seal_all(&mut wtxn, store.env(), "metrics", &settings).unwrap();
        let db = store
            .env()
            .create_database::<heed::types::Bytes, heed::types::Bytes>(
                &mut wtxn,
                Some("tsb:metrics"),
            )
            .unwrap();
        let locator = db
            .get(&wtxn, &user_key("user-1"))
            .unwrap()
            .unwrap()
            .to_vec();
        let wanted = &locator[..23];
        let keys: Vec<Vec<u8>> = db
            .iter(&wtxn)
            .unwrap()
            .map(|item| item.unwrap().0.to_vec())
            .filter(|key| key.first() == Some(&KEY_BUCKET) && key.as_slice() != wanted)
            .collect();
        assert!(!keys.is_empty());
        let stale = row_id(0, 0, 0, 0, u32::MAX);
        for key in keys {
            db.put(&mut wtxn, &key, b"corrupt").unwrap();
        }
        wtxn.commit().unwrap();
        // Only the mapped bucket is decoded; corrupt siblings are never read.
        let doc = c.find_by_key("user-1").unwrap().unwrap();
        assert_eq!(doc["v"], json!(1));
        // A stale synthetic id is authoritative and returns None without a scan.
        assert!(c.find_by_key(&stale).unwrap().is_none());
    }

    #[test]
    fn multi_update_moves_sealed_and_pending_rows_once() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            bucket_max_docs: 16,
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        c.create_index("k", true).unwrap();
        for idx in 0..20i64 {
            c.insert_one(json!({"ts": idx * 1000, "k": idx, "v": 0}))
                .unwrap();
        }
        let report = c
            .update_opts(
                &json!({}),
                &json!([{"$set": {
                    "ts": {"$add": ["$ts", 7_200_000]},
                    "v": {"$add": ["$v", 1]}
                }}]),
                true,
                false,
            )
            .unwrap();
        assert_eq!(report.matched, 20);
        assert_eq!(report.modified, 20);
        assert_eq!(c.count(&json!({})).unwrap(), 20);
        assert_eq!(c.count(&json!({"v": 0})).unwrap(), 0);
        assert_eq!(c.count(&json!({"v": 1})).unwrap(), 20);
        assert_eq!(c.count(&json!({"ts": {"$lt": 7_200_000}})).unwrap(), 0);
        for idx in 0..20i64 {
            assert_eq!(c.count(&json!({"k": idx})).unwrap(), 1);
        }
    }

    #[test]
    fn multi_update_collisions_are_rejected_before_writing() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        c.create_index("k", true).unwrap();
        c.insert_one(json!({"ts": 1, "k": 1})).unwrap();
        c.insert_one(json!({"ts": 2, "k": 2})).unwrap();
        assert!(c
            .update(&json!({}), &json!({"$set": {"k": 5}}), true)
            .is_err());
        assert_eq!(c.count(&json!({"k": 1})).unwrap(), 1);
        assert_eq!(c.count(&json!({"k": 2})).unwrap(), 1);
        assert_eq!(c.count(&json!({"k": 5})).unwrap(), 0);
        assert_eq!(
            c.find(&crate::collection::FindQuery {
                filter: json!({"k": 5}),
                projection: None,
                sort: Vec::new(),
                skip: 0,
                limit: None,
                comment: None,
                max_time_ms: None,
                hint: Some(json!({"$natural": 1})),
            })
            .unwrap()
            .len(),
            0
        );
    }

    #[test]
    fn time_range_delete_keeps_out_of_range_rows_in_the_bucket() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        for idx in 0..30i64 {
            c.insert_one(json!({"ts": idx * 1000, "v": idx})).unwrap();
        }
        let mut wtxn = store.write_txn().unwrap();
        seal_all(&mut wtxn, store.env(), "metrics", &settings).unwrap();
        wtxn.commit().unwrap();
        assert_eq!(
            c.delete(&json!({"ts": {"$gte": 5_000, "$lte": 14_000}}), true)
                .unwrap(),
            10
        );
        assert_eq!(c.count(&json!({})).unwrap(), 20);
        assert_eq!(c.count(&json!({"ts": {"$lt": 5_000}})).unwrap(), 5);
        assert_eq!(c.count(&json!({"ts": {"$gt": 14_000}})).unwrap(), 15);
    }

    #[test]
    fn indexed_writes_resolve_user_supplied_ids() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            bucket_max_docs: 32,
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        c.create_index("grp", false).unwrap();
        for idx in 0..200i64 {
            c.insert_one(json!({
                "_id": format!("u{idx}"),
                "ts": idx * 1000,
                "grp": if idx < 10 { "target" } else { "other" },
            }))
            .unwrap();
        }
        let mut wtxn = store.write_txn().unwrap();
        seal_all(&mut wtxn, store.env(), "metrics", &settings).unwrap();
        let db = store
            .env()
            .create_database::<heed::types::Bytes, heed::types::Bytes>(
                &mut wtxn,
                Some("tsb:metrics"),
            )
            .unwrap();
        let locator = db.get(&wtxn, &user_key("u0")).unwrap().unwrap().to_vec();
        let wanted = &locator[..23];
        let keys: Vec<Vec<u8>> = db
            .iter(&wtxn)
            .unwrap()
            .map(|item| item.unwrap().0.to_vec())
            .filter(|key| key.first() == Some(&KEY_BUCKET) && key.as_slice() != wanted)
            .collect();
        assert!(!keys.is_empty());
        for key in keys {
            db.put(&mut wtxn, &key, b"corrupt").unwrap();
        }
        wtxn.commit().unwrap();

        assert_eq!(
            c.update(
                &json!({"grp": "target"}),
                &json!({"$set": {"hit": true}}),
                true
            )
            .unwrap(),
            10
        );
        assert_eq!(c.find_by_key("u0").unwrap().unwrap()["hit"], json!(true));
        assert_eq!(c.delete(&json!({"grp": "target"}), true).unwrap(), 10);
        assert!(c.find_by_key("u0").unwrap().is_none());
    }

    #[test]
    fn retention_removes_index_entries_for_reaped_rows() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        c.create_index("seq", true).unwrap();
        c.insert_one(json!({"ts": 1_000, "seq": 1, "msg": "old"}))
            .unwrap();
        c.insert_one(json!({"ts": 2_000, "seq": 2, "msg": "new"}))
            .unwrap();
        c.insert_one(json!({"ts": 3_000, "seq": 3, "msg": "new"}))
            .unwrap();
        c.create_text_index("msg").unwrap();
        let mut wtxn = store.write_txn().unwrap();
        seal_all(&mut wtxn, store.env(), "metrics", &settings).unwrap();
        wtxn.commit().unwrap();

        let report = store.retain_timeseries("metrics", 1_500).unwrap();
        assert_eq!(report.tombstoned_buckets, 1);
        assert!(report.reaped.iter().any(|(_, doc)| doc["seq"] == json!(1)));
        // The unique entry for the reaped row is gone, so the value is reusable.
        c.insert_one(json!({"ts": 4_000, "seq": 1, "msg": "reused"}))
            .unwrap();
        assert_eq!(c.count(&json!({"seq": 1})).unwrap(), 1);
        assert_eq!(
            c.search("msg", "old", &json!({}), 0, None).unwrap().len(),
            0
        );
    }

    #[test]
    fn deleting_rows_reclaims_bucket_payload_and_keeps_ids() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let ids: Vec<String> = (0..20_000i64)
            .map(|idx| {
                c.insert_one(json!({"ts": idx * 1000, "value": idx}))
                    .unwrap()
            })
            .collect();
        let mut wtxn = store.write_txn().unwrap();
        seal_all(&mut wtxn, store.env(), "metrics", &settings).unwrap();
        wtxn.commit().unwrap();
        let before = store
            .timeseries_stats("metrics")
            .unwrap()
            .unwrap()
            .encoded_bytes;

        assert_eq!(
            c.delete(&json!({"value": {"$lt": 10_000}}), true).unwrap(),
            10_000
        );
        let after = store.timeseries_stats("metrics").unwrap().unwrap();
        assert_eq!(after.rows, 10_000);
        assert!(
            after.encoded_bytes < before,
            "payload did not shrink: {before} -> {}",
            after.encoded_bytes
        );
        assert_eq!(
            c.find_by_key(&ids[15_000]).unwrap().unwrap()["value"],
            json!(15_000)
        );
        assert!(c.find_by_key(&ids[5_000]).unwrap().is_none());

        let mut wtxn = store.write_txn().unwrap();
        let report =
            compact_deleted_buckets(&mut wtxn, store.env(), "metrics", &settings, 1).unwrap();
        wtxn.commit().unwrap();
        assert_eq!(report.reclaimed_bytes(), 0);
    }

    #[test]
    fn aggregation_streams_batches_and_matches_fallback() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        for idx in 0..5_000i64 {
            c.insert_one(json!({
                "ts": 1_700_000_000_000i64 + idx * 1000,
                "grp": if idx % 2 == 0 { "a" } else { "b" },
                "v": idx,
            }))
            .unwrap();
        }
        let streamed = c
            .aggregate(&[
                json!({"$match": {"grp": "a"}}),
                json!({"$project": {"v": 1, "_id": 0}}),
            ])
            .unwrap();
        assert_eq!(streamed.len(), 2_500);
        let grouped = c
            .aggregate(&[
                json!({"$group": {"_id": "$grp", "n": {"$sum": 1}}}),
                json!({"$sort": {"_id": 1}}),
            ])
            .unwrap();
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[0]["n"], json!(2_500));
        assert_eq!(grouped[1]["n"], json!(2_500));
        let averaged = c
            .aggregate(&[
                json!({"$match": {"grp": "a"}}),
                json!({"$group": {"_id": null, "avg": {"$avg": "$v"}}}),
                json!({"$limit": 1}),
            ])
            .unwrap();
        assert_eq!(averaged.len(), 1);
        assert_eq!(averaged[0]["avg"], json!(2_499));
        let limited = c
            .aggregate(&[
                json!({"$match": {"grp": "b"}}),
                json!({"$sort": {"v": 1}}),
                json!({"$limit": 7}),
            ])
            .unwrap();
        assert_eq!(limited.len(), 7);
        assert_eq!(limited[0]["v"], json!(1));

        let mut seen = 0usize;
        let mut batches = 0usize;
        scan_batches(
            &store.read_txn().unwrap(),
            store.env(),
            "metrics",
            &settings,
            &json!({}),
            17,
            |batch| {
                batches += 1;
                seen += batch.len();
                Ok(true)
            },
        )
        .unwrap();
        assert_eq!(seen, 5_000);
        assert_eq!(batches, 295);
    }

    #[test]
    fn indexed_filter_update_and_delete_on_timeseries() {
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let docs: Vec<Value> = (0..12i64)
            .map(|idx| json!({"ts": idx * 60_000, "status": if idx % 3 == 0 { "bad" } else { "ok" }}))
            .collect();
        for doc in docs {
            c.insert_one(doc).unwrap();
        }
        c.create_index("status", false).unwrap();
        assert_eq!(
            c.update(
                &json!({"status": "bad"}),
                &json!({"$set": {"fixed": true}}),
                true
            )
            .unwrap(),
            4
        );
        assert_eq!(c.delete(&json!({"status": "ok"}), true).unwrap(), 8);
        assert_eq!(c.count(&json!({"fixed": true})).unwrap(), 4);
        assert_eq!(c.count(&json!({})).unwrap(), 4);
    }

    #[test]
    fn find_and_modify_keeps_timeseries_indexes_current() {
        use crate::collection::FindAndModifyOptions;

        fn opts<'a>(
            filter: &'a Value,
            update: &'a Value,
            upsert: bool,
        ) -> FindAndModifyOptions<'a> {
            FindAndModifyOptions {
                filter,
                update: Some(update),
                sort: &[],
                new: true,
                upsert,
                remove: false,
                array_filters: &[],
                projection: None,
            }
        }

        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let base = 1_700_000_000_000i64;
        for idx in 0..50i64 {
            c.insert_one(json!({
                "ts": {"$date": {"$numberLong": (base + idx * 1000).to_string()}},
                "device": {"id": "a"},
                "level": idx % 5,
                "value": idx,
            }))
            .unwrap();
        }
        c.create_index("level", false).unwrap();
        assert_eq!(c.count(&json!({"level": 1})).unwrap(), 10);

        let filter = json!({"level": 1});
        let update = json!({"$set": {"level": 3}});
        let changed = c.find_and_modify(&opts(&filter, &update, false)).unwrap();
        let changed_id = changed.value.as_ref().unwrap()["_id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(c.count(&json!({"level": 1})).unwrap(), 9);
        assert_eq!(c.count(&json!({"level": 3})).unwrap(), 11);
        assert_eq!(c.count(&json!({"level": 3, "_id": changed_id})).unwrap(), 1);

        // A time/meta move removes the old index entry and inserts the new one.
        let filter = json!({"_id": changed_id});
        let update = json!({"$set": {
            "ts": {"$date": {"$numberLong": (base + 86_400_000).to_string()}},
            "level": 4
        }});
        let moved_before = c.find_by_key(&changed_id).unwrap().unwrap();
        let moved = c.find_and_modify(&opts(&filter, &update, false)).unwrap();
        let moved_id = moved.new_id.as_ref().unwrap();
        assert_ne!(moved_id, &changed_id);
        assert!(
            c.find_by_key(&changed_id).unwrap().is_none(),
            "{moved_before}"
        );
        assert_eq!(
            c.find_by_key(moved_id).unwrap().unwrap()["value"],
            moved_before["value"]
        );
        assert_eq!(c.count(&json!({"level": 3})).unwrap(), 10);
        assert_eq!(c.count(&json!({"level": 4})).unwrap(), 11);
        assert_eq!(c.count(&json!({"_id": moved_id, "level": 4})).unwrap(), 1);

        // Upserts add index entries too.
        let filter = json!({"_id": "custom-ts-id", "level": 9});
        let update = json!({"$set": {
            "ts": {"$date": {"$numberLong": (base + 1_000).to_string()}},
            "device": {"id": "a"}
        }});
        let upserted = c.find_and_modify(&opts(&filter, &update, true)).unwrap();
        assert_eq!(upserted.upserted_id.as_deref(), Some("custom-ts-id"));
        assert_eq!(c.count(&json!({"level": 9})).unwrap(), 1);
    }

    #[test]
    fn text_fuzzy_geo_and_vector_indexes_work_on_timeseries() {
        use crate::vector::VectorMetric;

        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let base = 1_700_000_000_000i64;
        let rows = [
            (
                "rust database engine",
                "color",
                [1.0f32, 0.0, 0.0],
                [19.94, 50.06],
            ),
            (
                "python web framework",
                "colour",
                [0.0, 1.0, 0.0],
                [21.01, 52.23],
            ),
            (
                "rust web framework",
                "colourful",
                [0.9, 0.1, 0.0],
                [19.941, 50.06],
            ),
        ];
        for (idx, (title, code, emb, loc)) in rows.iter().enumerate() {
            c.insert_one(json!({
                "ts": {"$date": {"$numberLong": (base + idx as i64 * 1000).to_string()}},
                "device": {"id": "a"},
                "title": title,
                "code": code,
                "emb": emb,
                "loc": {"type": "Point", "coordinates": loc},
            }))
            .unwrap();
        }

        c.create_text_index("title").unwrap();
        c.create_fuzzy_index("code").unwrap();
        c.create_geo_index("loc").unwrap();
        c.create_vector_index("emb", 3, VectorMetric::Cosine)
            .unwrap();

        // Text search and the `$search` aggregation stage.
        let hits = c.search("title", "rust", &json!({}), 0, Some(10)).unwrap();
        assert_eq!(hits.len(), 2);
        let staged = c
            .aggregate(&[
                json!({"$search": {"text": {"query": "rust", "path": "title"}}}),
                json!({"$project": {"_id": 1, "title": 1}}),
            ])
            .unwrap();
        assert_eq!(staged.len(), 2);

        // Fuzzy search by distance.
        let fuzzy = c
            .fuzzy_search("code", "color", 1, &json!({}), Some(10))
            .unwrap();
        assert!(fuzzy.iter().any(|doc| doc["code"] == json!("color")));
        assert!(fuzzy.iter().any(|doc| doc["code"] == json!("colour")));

        // Vector search returns the closest rows with `_distance`.
        let vector = c
            .vector_search("emb", &[1.0, 0.0, 0.0], &json!({}), 0, Some(2))
            .unwrap();
        assert_eq!(vector.len(), 2);
        assert!(
            vector[0]["_distance"].as_f64().unwrap() <= vector[1]["_distance"].as_f64().unwrap()
        );

        // Geo `$near` sorts by distance and `$geoWithin` filters by box.
        let near = crate::collection::FindQuery {
            filter: json!({"loc": {"$near": {
                "$geometry": {"type": "Point", "coordinates": [19.94, 50.06]},
                "$maxDistance": 500
            }}}),
            ..Default::default()
        };
        let found = c.find(&near).unwrap();
        assert_eq!(found.len(), 2);
        assert_eq!(found[0]["title"], json!("rust database engine"));
        assert_eq!(
            c.count(&json!({"loc": {"$geoWithin": {"$box": [[19.0, 50.0], [20.0, 51.0]]}}}))
                .unwrap(),
            2
        );

        // Insert/update/delete keep every index in sync.
        let id = c
            .insert_one(json!({
                "ts": {"$date": {"$numberLong": (base + 500_000).to_string()}},
                "device": {"id": "a"},
                "title": "rust stream processor",
                "code": "colorr",
                "emb": [1.0, 0.0, 0.0],
                "loc": {"type": "Point", "coordinates": [19.941, 50.06]},
            }))
            .unwrap();
        assert!(c
            .search("title", "stream", &json!({}), 0, Some(10))
            .unwrap()
            .iter()
            .any(|doc| doc["_id"] == json!(id)));

        c.update_opts(
            &json!({"_id": id}),
            &json!({"$set": {"title": "velvet stream", "code": "velvet", "loc": {"type": "Point", "coordinates": [10.0, 40.0]}}}),
            false,
            false,
        )
        .unwrap();
        assert!(c
            .search("title", "rust", &json!({}), 0, Some(10))
            .unwrap()
            .iter()
            .all(|doc| doc["_id"] != json!(id)));
        assert!(c
            .search("title", "velvet", &json!({}), 0, Some(10))
            .unwrap()
            .iter()
            .any(|doc| doc["_id"] == json!(id)));
        assert_eq!(
            c.count(&json!({"loc": {"$geoWithin": {"$box": [[19.0, 50.0], [20.0, 51.0]]}}}))
                .unwrap(),
            2
        );

        assert_eq!(c.delete(&json!({"_id": id}), false).unwrap(), 1);
        assert!(c
            .search("title", "velvet", &json!({}), 0, Some(10))
            .unwrap()
            .is_empty());

        // Reindex rebuilds all four index types.
        let report = c.reindex().unwrap();
        assert!(report.text.iter().any(|name| name == "title"));
        assert!(report.fuzzy.iter().any(|name| name == "code"));
        assert!(report.vector.iter().any(|name| name == "emb"));
        assert_eq!(
            c.search("title", "rust", &json!({}), 0, Some(10))
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn sql_select_insert_update_delete_on_timeseries() {
        use crate::sql;
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let insert = sql::PreparedSql::parse(
            "INSERT INTO metrics (ts, device, value) VALUES \
             (1700000000000, 'a', 1), (1700000001000, 'a', 2), \
             (1700000002000, 'a', 3), (1700000003000, 'b', 2), \
             (1700000004000, 'b', 4)",
        )
        .unwrap();
        let mut wtxn = store.write_txn().unwrap();
        let out = insert.execute_write(&c, &mut wtxn, &[]).unwrap();
        wtxn.commit().unwrap();
        assert_eq!(out["inserted"], 5);
        assert_eq!(c.count(&json!({})).unwrap(), 5);

        // SELECT uses `find`: filter, sort, limit/offset.
        let rows = sql::execute(
            &c,
            "SELECT device, value FROM metrics WHERE value >= 2 ORDER BY value DESC LIMIT 2 OFFSET 1",
            &[],
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["value"], json!(3));
        assert_eq!(rows[1]["value"], json!(2));

        // GROUP BY matches the equivalent native aggregation.
        let grouped = sql::execute(
            &c,
            "SELECT device, COUNT(*) AS n, SUM(value) AS total FROM metrics GROUP BY device ORDER BY device",
            &[],
        )
        .unwrap();
        let native = c
            .aggregate(&[
                json!({"$group": {"_id": "$device", "n": {"$sum": 1}, "total": {"$sum": "$value"}}}),
                json!({"$sort": {"_id": 1}}),
            ])
            .unwrap();
        assert_eq!(grouped.len(), native.len());
        for (sql_row, native_row) in grouped.iter().zip(native.iter()) {
            assert_eq!(sql_row["device"], native_row["_id"]);
            assert_eq!(sql_row["n"], native_row["n"]);
            assert_eq!(sql_row["total"], native_row["total"]);
        }

        // SQL explain goes through the collection planner.
        c.create_index("value", false).unwrap();
        let explained = sql::explain(&c, "SELECT * FROM metrics WHERE value = 3", &[]).unwrap();
        assert_eq!(explained["index_used"], json!(true), "{explained}");

        // UPDATE rewrites rows; the time field stays intact and ids are stable.
        let update =
            sql::PreparedSql::parse("UPDATE metrics SET value = 100 WHERE device = 'a'").unwrap();
        let mut wtxn = store.write_txn().unwrap();
        let out = update.execute_write(&c, &mut wtxn, &[]).unwrap();
        wtxn.commit().unwrap();
        assert_eq!(out["matched"], 3);
        assert_eq!(c.count(&json!({"value": 100})).unwrap(), 3);

        // DELETE removes matching rows.
        let delete = sql::PreparedSql::parse("DELETE FROM metrics WHERE value = 100").unwrap();
        let mut wtxn = store.write_txn().unwrap();
        let out = delete.execute_write(&c, &mut wtxn, &[]).unwrap();
        wtxn.commit().unwrap();
        assert_eq!(out["deleted"], 3);
        assert_eq!(c.count(&json!({})).unwrap(), 2);
    }

    #[test]
    fn lookup_and_union_with_work_on_timeseries() {
        use crate::vault::CollectionProfile;
        let (_dir, store) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            ..Settings::default()
        };
        let ts = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let std_metrics = store
            .create_collection("metrics_std", CollectionProfile::Standard)
            .unwrap();
        let devices = store
            .create_collection("devices", CollectionProfile::Standard)
            .unwrap();
        let base = 1_700_000_000_000i64;
        for idx in 0..4i64 {
            let doc = json!({
                "ts": base + idx * 1000,
                "device": if idx < 2 { "a" } else { "b" },
                "value": idx,
            });
            ts.insert_one(doc.clone()).unwrap();
            std_metrics.insert_one(doc).unwrap();
        }
        devices
            .insert_one(json!({"device_id": "a", "label": "Alpha"}))
            .unwrap();
        devices
            .insert_one(json!({"device_id": "b", "label": "Beta"}))
            .unwrap();

        let lookup = vec![
            json!({"$match": {"value": {"$gte": 1}}}),
            json!({"$lookup": {
                "from": "devices",
                "localField": "device",
                "foreignField": "device_id",
                "as": "info"
            }}),
            json!({"$project": {
                "_id": 0,
                "device": 1,
                "value": 1,
                "info": 1
            }}),
            json!({"$sort": {"value": 1}}),
        ];
        let ts_out = ts.aggregate(&lookup).unwrap();
        let std_out = std_metrics.aggregate(&lookup).unwrap();
        assert_eq!(ts_out, std_out, "local/foreign $lookup must match");
        assert_eq!(ts_out.len(), 3);
        assert_eq!(ts_out[0]["info"][0]["label"], json!("Alpha"));

        // Pipeline `$lookup` with `let` and `$expr`.
        let pipelined = vec![
            json!({"$lookup": {
                "from": "devices",
                "let": {"d": "$device"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$device_id", "$$d"]}}},
                    {"$project": {"_id": 0, "label": 1}}
                ],
                "as": "info"
            }}),
            json!({"$project": {"_id": 0, "device": 1, "info": 1}}),
            json!({"$sort": {"device": 1}}),
        ];
        assert_eq!(
            ts.aggregate(&pipelined).unwrap(),
            std_metrics.aggregate(&pipelined).unwrap(),
            "pipeline $lookup must match"
        );

        // `$unionWith`, including a pipeline on the joined collection.
        let union = vec![
            json!({"$match": {"device": "a"}}),
            json!({"$project": {"_id": 0, "device": 1, "value": 1}}),
            json!({"$unionWith": {
                "coll": "metrics_std",
                "pipeline": [
                    {"$match": {"device": "b"}},
                    {"$project": {"_id": 0, "device": 1, "value": 1}}
                ]
            }}),
            json!({"$sort": {"value": 1}}),
        ];
        let union_out = ts.aggregate(&union).unwrap();
        assert_eq!(union_out.len(), 4);
        assert_eq!(union_out[0]["device"], json!("a"));
        assert_eq!(union_out[3]["device"], json!("b"));

        // A TS-to-TS `$lookup` also works.
        let self_lookup = vec![
            json!({"$match": {"device": "a"}}),
            json!({"$lookup": {
                "from": "metrics",
                "localField": "value",
                "foreignField": "value",
                "as": "same"
            }}),
            json!({"$project": {"_id": 0, "value": 1, "n": {"$size": "$same"}}}),
            json!({"$sort": {"value": 1}}),
        ];
        let rows = ts.aggregate(&self_lookup).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row["n"] == json!(1)));
    }

    #[test]
    fn settings_roundtrip_validation_and_replication() {
        use crate::replication::{ReplicationOp, ReplicationTxn};
        use crate::vault::CollectionProfile;

        let (_primary_dir, primary) = ts_store();
        let initial = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            compression: Compression::Fast,
            ..Settings::default()
        };
        let source = primary
            .create_timeseries_collection("metrics", &initial)
            .unwrap();
        assert_eq!(
            primary.timeseries_settings("metrics").unwrap(),
            Some(initial.clone())
        );
        let changed = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            compression: Compression::Max,
            expire_after_seconds: Some(3_600),
            ..Settings::default()
        };
        source.set_timeseries_settings(&changed).unwrap();
        assert_eq!(
            primary.timeseries_settings("metrics").unwrap(),
            Some(changed.clone())
        );

        // Invalid settings are rejected by validation.
        let invalid = Settings {
            time_field: String::new(),
            ..Settings::default()
        };
        assert!(source.set_timeseries_settings(&invalid).is_err());
        assert_eq!(
            primary.timeseries_settings("metrics").unwrap(),
            Some(changed.clone())
        );

        // Replicating the settings update leaves both nodes identical.
        let (_replica_dir, replica) = ts_store();
        let txn = ReplicationTxn::new_with_ops(
            1,
            "primary",
            0,
            Vec::new(),
            vec![ReplicationOp::CollectionCreate {
                name: "metrics".into(),
                profile: "timeseries".into(),
                ttl_seconds: None,
                ttl_field: None,
                ttl_purge_history: None,
                timeseries: Some(serde_json::to_string(&initial).unwrap()),
            }],
        );
        assert!(replica.apply_replication_txn(&txn).unwrap());
        let txn = ReplicationTxn::new_with_ops(
            2,
            "primary",
            0,
            Vec::new(),
            vec![ReplicationOp::TimeSeriesSettings {
                coll: "metrics".into(),
                settings: changed.clone(),
            }],
        );
        assert!(replica.apply_replication_txn(&txn).unwrap());
        assert_eq!(
            replica.timeseries_settings("metrics").unwrap(),
            primary.timeseries_settings("metrics").unwrap()
        );
        assert_eq!(
            replica.collection("metrics").unwrap().profile(),
            CollectionProfile::TimeSeries
        );

        // Document collections report no time-series settings.
        primary
            .create_collection("docs", CollectionProfile::Standard)
            .unwrap();
        assert_eq!(primary.timeseries_settings("docs").unwrap(), None);
    }

    #[test]
    fn replication_applies_timeseries_update_and_move() {
        use crate::replication::{ReplicationChange, ReplicationOp, ReplicationTxn};

        let (primary_dir, primary) = ts_store();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Settings::default()
        };
        let source = primary
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let base = 1_700_000_000_000i64;
        let ids = ts_insert_range(&source, base, 12);
        primary.seal_timeseries("metrics").unwrap();

        let mut changes = Vec::new();
        let mut ops = vec![ReplicationOp::CollectionCreate {
            name: "metrics".into(),
            profile: "timeseries".into(),
            ttl_seconds: None,
            ttl_field: None,
            ttl_purge_history: None,
            timeseries: Some(serde_json::to_string(&settings).unwrap()),
        }];
        for id in &ids {
            let doc = source.find_by_key(id).unwrap().unwrap();
            changes.push(ReplicationChange {
                collection: "metrics".into(),
                op: "insert".into(),
                id: id.clone(),
                before: None,
                after: Some(doc),
                vault: false,
                ttl_at: None,
            });
            ops.push(ReplicationOp::Document {
                change: changes.len() - 1,
            });
        }
        let (_replica_dir, replica) = ts_store();
        let txn = ReplicationTxn::new_with_ops(1, "primary", 0, changes, ops);
        assert!(replica.apply_replication_txn(&txn).unwrap());
        assert_eq!(
            ts_find_all(&source),
            ts_find_all(&replica.collection("metrics").unwrap())
        );

        // In-place update on a sealed bucket replicates without changing ids.
        let target = ids[2].clone();
        let before = source.find_by_key(&target).unwrap().unwrap();
        let update = json!({"$set": {"value": 500}, "$inc": {"counter": 1}});
        source
            .update_opts(&json!({"_id": target}), &update, false, false)
            .unwrap();
        let after = source.find_by_key(&target).unwrap().unwrap();
        let txn = ReplicationTxn::new_with_ops(
            2,
            "primary",
            0,
            vec![ReplicationChange {
                collection: "metrics".into(),
                op: "update".into(),
                id: target.clone(),
                before: Some(before),
                after: Some(after),
                vault: false,
                ttl_at: None,
            }],
            vec![ReplicationOp::Document { change: 0 }],
        );
        assert!(replica.apply_replication_txn(&txn).unwrap());
        let replica_coll = replica.collection("metrics").unwrap();
        assert_eq!(
            replica_coll.find_by_key(&target).unwrap().unwrap()["value"],
            json!(500)
        );
        assert_eq!(ts_find_all(&source), ts_find_all(&replica_coll));

        // Move: primary captures delete(old) + insert(new).
        let moved_ts = base + 10 * 3_600_000;
        let out = source
            .update_opts(
                &json!({"_id": target}),
                &json!({"$set": {"ts": {"$date": {"$numberLong": moved_ts.to_string()}}}}),
                false,
                false,
            )
            .unwrap();
        let new_id = out.moved[0].1.clone();
        let after = source.find_by_key(&new_id).unwrap().unwrap();
        let txn = ReplicationTxn::new_with_ops(
            3,
            "primary",
            0,
            vec![
                ReplicationChange {
                    collection: "metrics".into(),
                    op: "delete".into(),
                    id: target.clone(),
                    before: None,
                    after: None,
                    vault: false,
                    ttl_at: None,
                },
                ReplicationChange {
                    collection: "metrics".into(),
                    op: "insert".into(),
                    id: new_id.clone(),
                    before: None,
                    after: Some(after),
                    vault: false,
                    ttl_at: None,
                },
            ],
            vec![
                ReplicationOp::Document { change: 0 },
                ReplicationOp::Document { change: 1 },
            ],
        );
        assert!(replica.apply_replication_txn(&txn).unwrap());
        assert_eq!(
            source.count(&json!({})).unwrap(),
            replica_coll.count(&json!({})).unwrap()
        );
        assert_eq!(ts_find_all(&source), ts_find_all(&replica_coll));
        let _ = primary_dir;
    }

    #[test]
    fn deletion_bitmap_reads_both_formats_and_migrates_legacy_keys() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::store::Store::open(dir.path()).unwrap();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        for idx in 0..10i64 {
            c.insert_one(json!({
                "ts": {"$date": {"$numberLong": (1_700_000_000_000i64 + idx * 1000).to_string()}},
                "device": {"id": "a"},
                "value": idx,
            }))
            .unwrap();
        }
        store.seal_timeseries("metrics").unwrap();
        let docs = ts_find_all(&c);
        assert_eq!(docs.len(), 10);
        let ids: Vec<(String, Locator)> = docs
            .iter()
            .map(|doc| {
                let id = doc["_id"].as_str().unwrap().to_string();
                (id.clone(), parse_row_id(&id).unwrap())
            })
            .collect();
        let (_, loc0) = ids[0].clone();
        let (hash, salt, window, serial, _) = loc0;
        let key = bucket_key(hash, salt, window, serial);

        // Rewrite the payload as the pre-bitmap NTB1 format.
        let env = store.env();
        let mut wtxn = env.write_txn().unwrap();
        let db = env
            .create_database::<Bytes, Bytes>(&mut wtxn, Some("tsb:metrics"))
            .unwrap();
        // Legacy 0x03 tombstone for the second row.
        let (_, _, _, _, seq1) = ids[1].1;
        db.put(&mut wtxn, &tomb_key(hash, salt, window, serial, seq1), &[])
            .unwrap();
        wtxn.commit().unwrap();

        assert_eq!(c.count(&json!({})).unwrap(), 9);
        assert_eq!(
            store
                .timeseries_stats("metrics")
                .unwrap()
                .unwrap()
                .deleted_rows,
            1
        );

        // A new delete migrates the old tombstone into the bitmap and drops the key.
        assert_eq!(c.delete(&json!({"value": 2}), false).unwrap(), 1);
        assert_eq!(c.count(&json!({})).unwrap(), 8);
        let stats = store.timeseries_stats("metrics").unwrap().unwrap();
        assert_eq!(stats.deleted_rows, 2);
        let rtxn = env.read_txn().unwrap();
        let db = env
            .open_database::<Bytes, Bytes>(&rtxn, Some("tsb:metrics"))
            .unwrap()
            .unwrap();
        assert!(db
            .get(&rtxn, &tomb_key(hash, salt, window, serial, seq1))
            .unwrap()
            .is_none());
        let payload = db.get(&rtxn, &key).unwrap().unwrap();
        assert_eq!(&payload[..4], MAGIC);
        drop(rtxn);
        assert!(c.find_by_key(&ids[1].0).unwrap().is_none());
        assert!(c.find_by_key(&ids[2].0).unwrap().is_none());
    }

    fn measure_deletes(legacy: bool) -> (u64, Vec<u8>) {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::store::Store::open(dir.path()).unwrap();
        let settings = Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Settings::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        for device in 0..10i64 {
            for idx in 0..1001i64 {
                c.insert_one(json!({
                    "ts": {"$date": {"$numberLong": (1_700_000_000_000i64 + idx * 1000).to_string()}},
                    "device": {"id": format!("dev-{device}")},
                    "value": idx,
                }))
                .unwrap();
            }
        }
        store.seal_timeseries("metrics").unwrap();
        if legacy {
            let docs = ts_find_all(&c);
            let env = store.env();
            let mut wtxn = env.write_txn().unwrap();
            let db = env
                .create_database::<Bytes, Bytes>(&mut wtxn, Some("tsb:metrics"))
                .unwrap();
            for doc in docs {
                if doc["value"].as_i64().unwrap_or(i64::MAX) >= 1000 {
                    continue;
                }
                let id = doc["_id"].as_str().unwrap();
                let (hash, salt, window, serial, seq) = parse_row_id(id).unwrap();
                db.put(&mut wtxn, &tomb_key(hash, salt, window, serial, seq), &[])
                    .unwrap();
            }
            wtxn.commit().unwrap();
        } else {
            assert_eq!(
                c.delete(&json!({"value": {"$lt": 1000}}), true).unwrap(),
                10_000
            );
        }
        assert_eq!(c.count(&json!({})).unwrap(), 10);
        let stats = store.timeseries_stats("metrics").unwrap().unwrap();
        assert_eq!(stats.deleted_rows, 10_000);
        let size = std::fs::metadata(dir.path().join("data.mdb"))
            .unwrap()
            .len();
        let mut payload = Vec::new();
        {
            let env = store.env();
            let rtxn = env.read_txn().unwrap();
            let db = env
                .open_database::<Bytes, Bytes>(&rtxn, Some("tsb:metrics"))
                .unwrap()
                .unwrap();
            for item in db.iter(&rtxn).unwrap() {
                let (key, value) = item.unwrap();
                if key.first() == Some(&KEY_BUCKET) {
                    payload = value.to_vec();
                    break;
                }
            }
        }
        drop(store);
        (size, payload)
    }

    #[test]
    fn deletion_bitmap_is_smaller_than_legacy_tombstones() {
        let (bitmap_size, payload) = measure_deletes(false);
        let (legacy_size, legacy_payload) = measure_deletes(true);
        assert_eq!(&payload[..4], MAGIC);
        assert_eq!(&legacy_payload[..4], MAGIC); // legacy simulation keeps the payload untouched
        assert!(payload.len() < legacy_payload.len() + 10_000 * 27);
        assert!(
            bitmap_size < legacy_size,
            "bitmap {bitmap_size} should be smaller than legacy {legacy_size}"
        );
    }
}
