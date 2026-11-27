//! Shared bucket column codec and byte primitives.
//!
//! This module is the format layer shared by time-series buckets and the
//! proposed general-purpose columnar profile. It owns the column kinds,
//! codec-selection rules, varint/bit primitives, and the exact byte layout of a
//! column block. Callers own bucket keys, schemas, ids, and query semantics.

use std::collections::BTreeMap;
use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};

/// Hard ceiling for rows in one column block. The bucket layer applies the
/// same ceiling to its own row counts.
pub(crate) const HARD_MAX_COLUMN_ROWS: usize = 16_384;

// Column kinds.
pub(crate) const KIND_I64: u8 = 0;
pub(crate) const KIND_F64: u8 = 1;
pub(crate) const KIND_BOOL: u8 = 2;
pub(crate) const KIND_STR: u8 = 3;
pub(crate) const KIND_JSON: u8 = 4;

// Column codecs (base encoding).
pub(crate) const CODEC_RAW: u8 = 0;
pub(crate) const CODEC_SHUFFLE: u8 = 1;
pub(crate) const CODEC_DELTA: u8 = 2;
pub(crate) const CODEC_DELTA_DELTA: u8 = 3;
pub(crate) const CODEC_XOR: u8 = 4;
pub(crate) const CODEC_DICT: u8 = 5;
pub(crate) const CODEC_BOOL_BITS: u8 = 6;
pub(crate) const CODEC_FOR: u8 = 7;
pub(crate) const CODEC_DEFLATE: u8 = 0x80;

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

fn corrupt(what: &str) -> Error {
    Error::Query(format!("corrupt column codec: {what}"))
}

pub(crate) fn put_uvarint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub(crate) fn get_uvarint(data: &[u8], pos: &mut usize) -> Result<u64> {
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

pub(crate) fn put_ivarint(out: &mut Vec<u8>, value: i64) {
    put_uvarint(out, zigzag(value));
}

pub(crate) fn get_ivarint(data: &[u8], pos: &mut usize) -> Result<i64> {
    Ok(unzigzag(get_uvarint(data, pos)?))
}

pub(crate) fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_uvarint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

pub(crate) fn get_bytes<'a>(data: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
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

pub(crate) fn bits_needed(max: u64) -> u8 {
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
// Columns
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Column encoders
// ---------------------------------------------------------------------------

pub(crate) fn encode_shuffle(values: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 8);
    for byte in 0..8 {
        for value in values {
            out.push((value >> (byte * 8)) as u8);
        }
    }
    out
}

pub(crate) fn decode_shuffle(data: &[u8], n: usize) -> Result<Vec<u64>> {
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

pub(crate) fn encode_delta(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 2);
    let mut prev = 0i64;
    for value in values {
        put_ivarint(&mut out, value.wrapping_sub(prev));
        prev = *value;
    }
    out
}

pub(crate) fn decode_delta(data: &[u8], n: usize) -> Result<Vec<i64>> {
    let mut out = Vec::with_capacity(n);
    let mut pos = 0;
    let mut prev = 0i64;
    for _ in 0..n {
        prev = prev.wrapping_add(get_ivarint(data, &mut pos)?);
        out.push(prev);
    }
    Ok(out)
}

pub(crate) fn encode_delta_delta(values: &[i64]) -> Vec<u8> {
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

pub(crate) fn decode_delta_delta(data: &[u8], n: usize) -> Result<Vec<i64>> {
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

pub(crate) fn encode_for(values: &[i64]) -> Vec<u8> {
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

pub(crate) fn decode_for(data: &[u8], n: usize) -> Result<Vec<i64>> {
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

pub(crate) fn encode_xor(values: &[f64]) -> Vec<u8> {
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

pub(crate) fn decode_xor(data: &[u8], n: usize) -> Result<Vec<f64>> {
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

pub(crate) fn encode_dict(values: &[String]) -> Vec<u8> {
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

pub(crate) fn decode_dict(data: &[u8], n: usize) -> Result<Vec<String>> {
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

pub(crate) struct Column {
    pub(crate) field: String,
    pub(crate) values: Vec<Option<Value>>,
}

pub(crate) fn classify(values: &[Option<Value>]) -> u8 {
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

pub(crate) fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_u64().map(|v| v as i64)),
        _ => None,
    }
}

pub(crate) fn encode_column(column: &Column, compression: Compression) -> Result<Vec<u8>> {
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

pub(crate) fn decode_column(data: &[u8], pos: &mut usize) -> Result<Column> {
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
    if n > HARD_MAX_COLUMN_ROWS {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn encoded_bytes_match_the_pre_extraction_format() {
        let column = Column {
            field: "v".into(),
            values: vec![Some(json!(1)), None, Some(json!(3))],
        };
        let encoded = encode_column(&column, Compression::Fast).expect("encode");
        assert_eq!(
            encoded,
            vec![1, b'v', KIND_I64, CODEC_DELTA, 1, 3, 0xa0, 2, 2, 4,]
        );
    }

    #[test]
    fn typed_columns_roundtrip_with_nulls() {
        let columns = [
            Column {
                field: "count".into(),
                values: vec![Some(json!(1)), None, Some(json!(3)), Some(json!(4))],
            },
            Column {
                field: "ratio".into(),
                values: vec![Some(json!(1.5)), Some(json!(2.25)), None, Some(json!(4.0))],
            },
            Column {
                field: "ok".into(),
                values: vec![
                    Some(json!(true)),
                    None,
                    Some(json!(false)),
                    Some(json!(true)),
                ],
            },
            Column {
                field: "status".into(),
                values: vec![
                    Some(json!("ok")),
                    Some(json!("warn")),
                    Some(json!("ok")),
                    None,
                ],
            },
            Column {
                field: "payload".into(),
                values: vec![Some(json!({"a": [1, 2]})), None, Some(json!([true, null]))],
            },
        ];

        for compression in [Compression::Fast, Compression::Balanced, Compression::Max] {
            for column in &columns {
                let encoded = encode_column(column, compression).expect("encode");
                let mut pos = 0;
                let decoded = decode_column(&encoded, &mut pos).expect("decode");
                assert_eq!(pos, encoded.len());
                assert_eq!(decoded.field, column.field);
                assert_eq!(decoded.values, column.values);
            }
        }
    }
}
