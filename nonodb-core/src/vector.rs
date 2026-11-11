//! Exact vector search support.
//!
//! Vectors are stored in a dedicated LMDB sub-database per indexed field:
//! `vec:<coll>:<field>` maps `_id` to `[norm f32 LE][component f32 LE...]`.
//! Queries scan the whole index and keep a bounded top-K heap. This is
//! deliberately simple and exact: there is no graph, training, or approximate
//! recall. The cost is linear in the number of indexed vectors.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Maximum accepted vector dimension.
pub const MAX_VECTOR_DIMS: usize = 65_536;

/// Distance metric for a vector index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum VectorMetric {
    /// Cosine distance: `1 - cosine_similarity`.
    #[default]
    Cosine,
    /// Negative dot product.
    Dot,
    /// Squared Euclidean distance.
    #[serde(alias = "euclidean")]
    L2,
}

impl VectorMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cosine => "cosine",
            Self::Dot => "dot",
            Self::L2 => "l2",
        }
    }
}

/// Vector index definition stored in `meta` under `vecdef:<coll>:<field>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorDef {
    pub field: String,
    pub dims: usize,
    #[serde(default)]
    pub metric: VectorMetric,
    #[serde(default = "default_format")]
    pub format: u8,
}

fn default_format() -> u8 {
    1
}

impl VectorDef {
    pub fn new(field: &str, dims: usize, metric: VectorMetric) -> Self {
        Self {
            field: field.to_string(),
            dims,
            metric,
            format: default_format(),
        }
    }
}

pub fn db_name(coll: &str, field: &str) -> String {
    format!("vec:{coll}:{field}")
}

pub fn def_key(coll: &str, field: &str) -> String {
    format!("vecdef:{coll}:{field}")
}

/// Parses a JSON array of finite numbers with exactly `dims` elements.
pub fn parse_vector(value: &Value, dims: usize) -> Option<Vec<f32>> {
    let values = value.as_array()?;
    if values.len() != dims {
        return None;
    }
    let mut out = Vec::with_capacity(dims);
    for value in values {
        let number = value.as_f64()?;
        if !number.is_finite() {
            return None;
        }
        let component = number as f32;
        if !component.is_finite() {
            return None;
        }
        out.push(component);
    }
    Some(out)
}

pub fn norm(vector: &[f32]) -> f32 {
    vector.iter().map(|v| v * v).sum::<f32>().sqrt()
}

/// Encodes a norm followed by the vector components as little-endian `f32`.
pub fn encode_vector(vector: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + vector.len() * 4);
    out.extend_from_slice(&norm(vector).to_le_bytes());
    for component in vector {
        out.extend_from_slice(&component.to_le_bytes());
    }
    out
}

/// Computes the metric distance directly from the stored value bytes.
///
/// Lower is always better. Returns `None` for malformed values so an index
/// written by an incompatible version is ignored rather than panicking.
pub fn distance_from_bytes(
    metric: VectorMetric,
    query: &[f32],
    query_norm: f32,
    bytes: &[u8],
    dims: usize,
) -> Option<f32> {
    if bytes.len() != 4 + dims * 4 {
        return None;
    }
    let stored_norm = f32::from_le_bytes(bytes[..4].try_into().ok()?);
    let mut dot = 0.0f32;
    let mut squared = 0.0f32;
    for (index, query_component) in query.iter().enumerate() {
        let start = 4 + index * 4;
        let stored = f32::from_le_bytes(bytes[start..start + 4].try_into().ok()?);
        dot += query_component * stored;
        let delta = query_component - stored;
        squared += delta * delta;
    }

    let distance = match metric {
        VectorMetric::Cosine => {
            if query_norm <= f32::EPSILON || stored_norm <= f32::EPSILON {
                1.0
            } else {
                1.0 - (dot / (query_norm * stored_norm)).clamp(-1.0, 1.0)
            }
        }
        VectorMetric::Dot => -dot,
        VectorMetric::L2 => squared,
    };
    distance.is_finite().then_some(distance)
}

/// Candidate ordered by ascending distance, then `_id` for stable results.
pub struct Candidate {
    pub distance: f32,
    pub id: String,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // Larger = worse, so the heap keeps the worst candidate at the top.
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.id.cmp(&other.id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_vectors_and_rejects_bad_values() {
        assert_eq!(
            parse_vector(&json!([1, 2.5, -3]), 3),
            Some(vec![1.0, 2.5, -3.0])
        );
        assert_eq!(parse_vector(&json!([1, 2]), 3), None);
        assert_eq!(parse_vector(&json!([1, "x", 3]), 3), None);
        assert_eq!(parse_vector(&json!({"x": 1}), 3), None);
    }

    #[test]
    fn cosine_distance_is_zero_for_identical_vectors() {
        let query = vec![1.0, 2.0, 3.0];
        let encoded = encode_vector(&query);
        let distance = distance_from_bytes(
            VectorMetric::Cosine,
            &query,
            norm(&query),
            &encoded,
            query.len(),
        )
        .unwrap();
        assert!(distance.abs() < 1e-6);
    }

    #[test]
    fn l2_and_dot_distances_order_vectors() {
        let query = [1.0f32, 0.0];
        let close = encode_vector(&[0.9, 0.1]);
        let far = encode_vector(&[-2.0, 0.0]);
        let qnorm = norm(&query);
        let close_l2 = distance_from_bytes(VectorMetric::L2, &query, qnorm, &close, 2).unwrap();
        let far_l2 = distance_from_bytes(VectorMetric::L2, &query, qnorm, &far, 2).unwrap();
        assert!(close_l2 < far_l2);

        let close_dot = distance_from_bytes(VectorMetric::Dot, &query, qnorm, &close, 2).unwrap();
        let far_dot = distance_from_bytes(VectorMetric::Dot, &query, qnorm, &far, 2).unwrap();
        assert!(close_dot < far_dot);
    }

    #[test]
    fn rejects_malformed_values() {
        assert!(distance_from_bytes(VectorMetric::L2, &[1.0], 1.0, &[0; 4], 1).is_none());
    }
}
