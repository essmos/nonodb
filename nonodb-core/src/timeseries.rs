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

use heed::types::Bytes;
use heed::{Database, RoTxn, RwTxn};
use serde::{Deserialize, Serialize};
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

const MAGIC: &[u8; 4] = b"NTB1";
const FORMAT_VERSION: u8 = 1;
const KEY_OPEN: u8 = 0x00;
const KEY_BUCKET: u8 = 0x01;
const KEY_ROW: u8 = 0x02;
const KEY_TOMB: u8 = 0x03;
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
    pub time_field: String,
    /// Optional field that groups rows into series. Stored once per bucket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta_field: Option<String>,
    #[serde(default)]
    pub granularity: Granularity,
    #[serde(default = "default_bucket_max_docs")]
    pub bucket_max_docs: usize,
    #[serde(default = "default_bucket_max_bytes")]
    pub bucket_max_bytes: usize,
    #[serde(default)]
    pub compression: Compression,
    /// Optional read-time expiry in seconds, relative to the event time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    put_uvarint(&mut out, encoded_columns.len() as u64);
    for (field, values) in encoded_columns {
        let column = Column { field, values };
        out.extend_from_slice(&encode_column(&column, settings.compression)?);
    }
    Ok(out)
}

#[cfg(test)]
fn bucket_bounds(data: &[u8]) -> Result<(bool, i64, i64)> {
    if data.len() < 6 || &data[..4] != MAGIC {
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
    if data.len() < 6 || &data[..4] != MAGIC {
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
    if data.len() < 6 || &data[..4] != MAGIC {
        return Err(corrupt("bad magic"));
    }
    let version = data[4];
    if version != FORMAT_VERSION {
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
    let columns = get_uvarint(data, &mut pos)? as usize;
    if columns > 4096 {
        return Err(corrupt("column count out of range"));
    }
    let mut decoded = Vec::with_capacity(columns);
    for _ in 0..columns {
        decoded.push(decode_column(data, &mut pos)?);
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
    if data.len() < 6 || &data[..4] != MAGIC {
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
    let columns = get_uvarint(data, &mut pos)? as usize;
    if index >= n {
        return Ok(None);
    }
    let mut row = Map::new();
    for _ in 0..columns {
        let column = decode_column(data, &mut pos)?;
        if let Some(Some(value)) = column.values.into_iter().nth(index) {
            set_path(&mut row, &column.field, value);
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

fn extract_time(doc: &Value, field: &str) -> Option<i64> {
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
fn row_id(hash: u64, salt: u32, window_start: i64, serial: u16, seq: u32) -> String {
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
) -> Result<std::collections::HashSet<u32>> {
    let prefix = tomb_base(hash, salt, window, serial);
    let mut out = std::collections::HashSet::new();
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
    let mut rows = Vec::new();
    let mut keys = Vec::new();
    for item in db.prefix_iter(&*wtxn, &prefix)? {
        let (key, value) = item?;
        keys.push(key.to_vec());
        rows.push(serde_json::from_slice::<Map<String, Value>>(value)?);
    }
    if rows.is_empty() {
        return Ok(0);
    }
    let mut end = window;
    let mut meta = None;
    for row in &rows {
        let doc = Value::Object(row.clone());
        if let Some(ts) = extract_time(&doc, &settings.time_field) {
            end = end.max(ts);
        }
        if meta.is_none() {
            meta = series_meta(&doc, settings);
        }
    }
    let bucket = Bucket {
        sealed: true,
        window_start: window,
        end,
        meta,
        rows,
    };
    let payload = encode_bucket(&bucket, settings)?;
    db.put(&mut *wtxn, bucket_key_bytes, &payload)?;
    for key in keys {
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

    let id = match object.get("_id") {
        Some(Value::String(id)) => id.clone(),
        Some(other) => crate::id::key_from_value(other).ok_or(Error::InvalidId)?,
        None => row_id(hash, salt, window, serial, count),
    };
    Ok(id)
}

fn time_bounds(filter: &Value, field: &str) -> (Option<i64>, Option<i64>) {
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
    let field = settings.meta_field.as_deref()?;
    let object = filter.as_object()?;
    if let Some(value) = object.get(field) {
        if value.is_object() && !value.as_object()?.contains_key("$eq") {
            return None;
        }
        let operand = value
            .as_object()
            .and_then(|object| object.get("$eq"))
            .unwrap_or(value);
        return Some(series_hash(settings, Some(operand)));
    }
    None
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

    let mut out: Vec<Value> = Vec::new();
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
        for ((hash, salt, window, serial), seqs) in by_bucket {
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
                    if tombs.contains(&seq) || !wanted.contains(&seq) {
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
                    }
                }
            }
        }
        for id in user_ids {
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
                        if tombs.contains(&(*idx as u32)) {
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

/// Returns every live row with its `_id`, decoded from buckets. Used to build
/// and rebuild secondary indexes.
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
        match key.first() {
            Some(&KEY_BUCKET) => {
                let Some((hash, salt, window, serial)) = key_parts(key) else {
                    continue;
                };
                let (bucket, selected) = decode_bucket_inner(payload, settings, None, None)?;
                let tombs = tombstones(rtxn, &db, hash, salt, window, serial)?;
                for (row, idx) in bucket.rows.iter().zip(selected.iter()) {
                    if tombs.contains(&(*idx as u32)) {
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
                let id = doc
                    .get("_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                out.push((id, doc));
            }
            _ => {}
        }
    }
    Ok(out)
}

/// Counts matching documents.
pub fn count(
    rtxn: &RoTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    filter: &Value,
) -> Result<usize> {
    validate_filter(filter)?;
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
pub fn delete(
    wtxn: &mut RwTxn<'_>,
    env: &heed::Env,
    name: &str,
    settings: &Settings,
    filter: &Value,
    multi: bool,
) -> Result<usize> {
    validate_filter(filter)?;
    let db =
        env.create_database::<Bytes, Bytes>(wtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?;
    let (low, high) = time_bounds(filter, &settings.time_field);
    let meta_hash = meta_equality_hash(filter, settings);
    let meta_filter = meta_only_filter(filter, settings);
    let now = crate::ttl::now_ms() as i64;
    let mut deleted = 0usize;

    let mut buckets: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for item in db.iter(&*wtxn)? {
        let (key, payload) = item?;
        if key.first() == Some(&KEY_BUCKET) {
            buckets.push((key.to_vec(), payload.to_vec()));
        }
    }
    for (key, payload) in buckets {
        if !multi && deleted > 0 {
            break;
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
        let (bucket, selected) = decode_bucket_inner(&payload, settings, low, high)?;
        let (hash, salt, window, serial) = key_parts(&key).unwrap_or((0, 0, start, 0));
        let tombs = tombstones(&*wtxn, &db, hash, salt, window, serial)?;
        let mut live = 0usize;
        let mut removed_tombstones = Vec::new();
        for (row, idx) in bucket.rows.iter().zip(selected.iter()) {
            let seq = *idx as u32;
            if tombs.contains(&seq) {
                continue;
            }
            live += 1;
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
        for seq in removed_tombstones {
            db.put(&mut *wtxn, &tomb_key(hash, salt, window, serial, seq), &[])?;
        }
        // If every live row of the bucket is now deleted, drop the payload and
        // its tombstones instead of keeping an empty sealed bucket.
        if removed == live {
            db.delete(&mut *wtxn, &key)?;
            drop_tombstones(wtxn, &db, hash, salt, window, serial)?;
        }
    }

    if multi || deleted == 0 {
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for item in db.iter(&*wtxn)? {
            let (key, payload) = item?;
            if key.first() == Some(&KEY_ROW) {
                rows.push((key.to_vec(), payload.to_vec()));
            }
        }
        for (key, payload) in rows {
            if !multi && deleted > 0 {
                break;
            }
            let Some((hash, salt, window, serial, seq)) = row_key_parts(&key) else {
                continue;
            };
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
            let tomb = tomb_key(hash, salt, window, serial, seq);
            if db.get(&*wtxn, &tomb)?.is_some() {
                return Ok(false);
            }
            // The row exists only if its position is inside the bucket.
            let payload = db
                .get(&*wtxn, &sealed)?
                .ok_or_else(|| corrupt("bucket disappeared during delete"))?
                .to_vec();
            let (bucket, selected) = decode_bucket_inner(&payload, settings, None, None)?;
            if !selected.contains(&(seq as usize)) {
                return Ok(false);
            }
            let tombs = tombstones(&*wtxn, &db, hash, salt, window, serial)?;
            if tombs.len() + 1 >= bucket.rows.len() {
                db.delete(&mut *wtxn, &sealed)?;
                drop_tombstones(wtxn, &db, hash, salt, window, serial)?;
            } else {
                db.put(&mut *wtxn, &tomb, &[])?;
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
                for (row, idx) in bucket.rows.iter().zip(selected.iter()) {
                    if tombs.contains(&(*idx as u32)) {
                        continue;
                    }
                    if extract_time(&Value::Object(row.clone()), &settings.time_field).is_none() {
                        return Err(corrupt("sealed row has no time field"));
                    }
                    rows += 1;
                }
                bytes += payload.len() as u64;
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
    let db =
        env.create_database::<Bytes, Bytes>(wtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?;
    let mut report = RetainReport::default();
    let mut buckets: Vec<(Vec<u8>, i64, i64)> = Vec::new();
    let mut open_rows: Vec<Vec<u8>> = Vec::new();

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
                        open_rows.push(key.to_vec());
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
            db.delete(&mut *wtxn, &key)?;
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
        let (bucket, selected) = decode_bucket_inner(&payload, settings, None, None)?;
        let tombs = tombstones(&*wtxn, &db, hash, salt, window, serial)?;
        let mut added = 0u64;
        for (row, idx) in bucket.rows.iter().zip(selected.iter()) {
            let seq = *idx as u32;
            if tombs.contains(&seq) {
                continue;
            }
            let expired = extract_time(&Value::Object(row.clone()), &settings.time_field)
                .is_some_and(|ts| ts < before_ms);
            if expired {
                db.put(&mut *wtxn, &tomb_key(hash, salt, window, serial, seq), &[])?;
                added += 1;
            }
        }
        if added > 0 {
            report.tombstoned_buckets += 1;
        }
        let live_after =
            bucket.rows.len() - tombstones(&*wtxn, &db, hash, salt, window, serial)?.len();
        if live_after == 0 {
            db.delete(&mut *wtxn, &key)?;
            drop_tombstones(wtxn, &db, hash, salt, window, serial)?;
            report.dropped_buckets += 1;
        }
    }

    for key in &open_rows {
        db.delete(&mut *wtxn, key)?;
        report.dropped_rows += 1;
    }

    refresh_pointers(wtxn, &db)?;
    report.remaining_rows = stats(wtxn, env, name, settings)?.rows;
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
pub fn strip_derived_id_pub(doc: &mut Value) {
    strip_derived_id(doc)
}

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
    use crate::query::updater::{apply_upsert_with_options, apply_with_options, seed_from_filter};

    validate_filter(filter)?;
    let db =
        env.create_database::<Bytes, Bytes>(wtxn, Some(&format!("{BUCKET_DB_PREFIX}{name}")))?;
    let (low, high) = time_bounds(filter, &settings.time_field);
    let meta_hash = meta_equality_hash(filter, settings);
    let meta_filter = meta_only_filter(filter, settings);
    let now = crate::ttl::now_ms() as i64;
    let mut report = UpdateReport::default();

    let mut buckets: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for item in db.iter(&*wtxn)? {
        let (key, payload) = item?;
        if key.first() == Some(&KEY_BUCKET) {
            buckets.push((key.to_vec(), payload.to_vec()));
        }
    }
    for (key, payload) in buckets {
        if !multi && report.matched > 0 {
            break;
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
        let tombs = tombstones(&*wtxn, &db, hash, salt, window, serial)?;
        let mut changed = false;
        let mut moved_out = Vec::new();
        for (pos, idx) in selected.iter().enumerate() {
            if !multi && report.matched > 0 {
                break;
            }
            let seq = *idx as u32;
            if tombs.contains(&seq) {
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
                let new_id = insert(wtxn, env, name, settings, next)?;
                report.moved.push((old_id, new_id));
            }
        }
        for seq in &moved_out {
            db.put(&mut *wtxn, &tomb_key(hash, salt, window, serial, *seq), &[])?;
        }
        if changed {
            let payload = encode_bucket(&bucket, settings)?;
            db.put(&mut *wtxn, &key, &payload)?;
        }
        if !moved_out.is_empty() {
            let live =
                bucket.rows.len() - tombstones(&*wtxn, &db, hash, salt, window, serial)?.len();
            if live == 0 {
                db.delete(&mut *wtxn, &key)?;
                drop_tombstones(wtxn, &db, hash, salt, window, serial)?;
            }
        }
    }

    if multi || report.matched == 0 {
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for item in db.iter(&*wtxn)? {
            let (key, payload) = item?;
            if key.first() == Some(&KEY_ROW) {
                rows.push((key.to_vec(), payload.to_vec()));
            }
        }
        for (key, payload) in rows {
            if !multi && report.matched > 0 {
                break;
            }
            let Some((hash, salt, window, serial, seq)) = row_key_parts(&key) else {
                continue;
            };
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
                let new_id = insert(wtxn, env, name, settings, next)?;
                report.moved.push((old_id, new_id));
            }
        }
        refresh_pointers(wtxn, &db)?;
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
            delete_locator(wtxn, &db, hash, salt, window, serial, seq)?;
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
        let Some(pos) = selected.iter().position(|idx| *idx == seq as usize) else {
            insert(wtxn, env, name, settings, next)?;
            return Ok(());
        };
        let row = &decoded.rows[pos];
        let old = row_doc(row, hash, salt, window, serial, seq);
        let same = extract_time(&old, &settings.time_field) == Some(new_time)
            && series_meta(&old, settings) == new_meta;
        if !same {
            db.put(&mut *wtxn, &tomb_key(hash, salt, window, serial, seq), &[])?;
            insert(wtxn, env, name, settings, next)?;
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
        let payload = encode_bucket(&decoded, settings)?;
        db.put(&mut *wtxn, &bucket, &payload)?;
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
        Some(payload) => decode_bucket_inner(payload, settings, None, None)?.0,
        None => Bucket {
            sealed: false,
            window_start: window,
            end: window,
            meta: series_meta(&doc, settings),
            rows: Vec::new(),
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
    if let Some(time) = extract_time(&doc, &settings.time_field) {
        bucket.end = bucket.end.max(time);
    }
    let payload = encode_bucket(&bucket, settings)?;
    db.put(&mut *wtxn, &key, &payload)?;
    db.delete(&mut *wtxn, &tomb_key(hash, salt, window, serial, seq))?;
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
            if tombs.contains(&(*idx as u32)) {
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
    hash: u64,
    salt: u32,
    window: i64,
    serial: u16,
    seq: u32,
) -> Result<()> {
    if db
        .get(&*wtxn, &row_key(hash, salt, window, serial, seq))?
        .is_some()
    {
        db.delete(&mut *wtxn, &row_key(hash, salt, window, serial, seq))?;
    } else {
        db.put(&mut *wtxn, &tomb_key(hash, salt, window, serial, seq), &[])?;
    }
    refresh_pointers(wtxn, db)
}

/// Fetches one row by `_id` without scanning the collection.
///
/// Synthetic ids encode the physical location, so this is a single LMDB get
/// (pending row) or one get plus one bucket decode (sealed row). User-supplied
/// `_id` values fall back to a filtered scan.
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
    }
    // User-supplied `_id` (or a stale synthetic id): filtered scan.
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
                    if tombs.contains(&(*idx as u32)) {
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

        // Removing the time field from a row is rejected.
        let err = c
            .update_opts(
                &json!({"_id": ids[0]}),
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

        // Unique indexes are rejected with a clear message.
        let err = c.create_index("value", true).unwrap_err();
        assert!(err.to_string().contains("unique"), "{err}");

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
}
