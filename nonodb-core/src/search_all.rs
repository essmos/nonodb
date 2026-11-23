//! Unified read-side search over BM25, fuzzy, and vector indexes.
//!
//! The existing `search`, `fuzzy`, and `vector_search` operations remain
//! separate compatibility APIs. `search_all` adds a single request shape with
//! multiple fields/modes and reciprocal-rank fusion for hybrid search.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::{Collection, Error, Result};

/// Search strategy used by [`SearchAllRequest`].
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SearchMode {
    #[default]
    Lexical,
    Fuzzy,
    Vector,
    Hybrid,
}

/// One unified search request.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SearchAllRequest {
    pub coll: String,
    #[serde(default)]
    pub mode: SearchMode,
    #[serde(default)]
    pub query: Option<String>,
    /// Text or fuzzy index fields. Empty means all indexes of the selected kind.
    #[serde(default)]
    pub fields: Vec<String>,
    #[serde(default)]
    pub vector: Option<Vec<f32>>,
    #[serde(default)]
    pub vector_field: Option<String>,
    #[serde(default)]
    pub filter: Value,
    #[serde(default)]
    pub skip: usize,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub max_distance: Option<usize>,
    #[serde(default)]
    pub rrf_k: Option<usize>,
}

/// Result metadata for a unified search.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SearchAllResponse {
    pub results: Vec<Value>,
    pub mode: SearchMode,
    pub rrf_k: usize,
    pub candidate_count: usize,
    pub sources: Vec<String>,
}

#[derive(Clone, Copy)]
enum SourceKind {
    Lexical,
    Fuzzy,
    Vector,
}

struct Source {
    label: String,
    kind: SourceKind,
    docs: Vec<Value>,
}

struct Fused {
    doc: Value,
    score: f64,
    sources: BTreeMap<String, Value>,
}

impl Collection<'_> {
    /// Runs a unified search inside an existing read transaction.
    pub fn search_all_in(
        &self,
        rtxn: &crate::heed::RoTxn<'_>,
        request: &SearchAllRequest,
    ) -> Result<SearchAllResponse> {
        let limit = request.limit.unwrap_or(10).min(1000);
        let rrf_k = request.rrf_k.unwrap_or(60).clamp(1, 10_000);
        let want = request.skip.saturating_add(limit).min(1000);
        let fields = self.search_all_fields_in(rtxn, request)?;
        let mut sources = Vec::new();

        if limit == 0 || want == 0 {
            return Ok(SearchAllResponse {
                results: Vec::new(),
                mode: request.mode,
                rrf_k,
                candidate_count: 0,
                sources: fields.source_labels(),
            });
        }

        match request.mode {
            SearchMode::Lexical => {
                let query = request.query.as_deref().ok_or_else(|| {
                    Error::Query("search_all lexical mode requires `query`".into())
                })?;
                if query.trim().is_empty() {
                    return Err(Error::Query("search_all `query` must not be empty".into()));
                }
                for field in fields.text {
                    sources.push(Source {
                        label: format!("lexical:{field}"),
                        kind: SourceKind::Lexical,
                        docs: self.search_in(
                            rtxn,
                            &field,
                            query,
                            &request.filter,
                            0,
                            Some(want),
                        )?,
                    });
                }
            }
            SearchMode::Fuzzy => {
                let query = request
                    .query
                    .as_deref()
                    .ok_or_else(|| Error::Query("search_all fuzzy mode requires `query`".into()))?;
                if query.trim().is_empty() {
                    return Err(Error::Query("search_all `query` must not be empty".into()));
                }
                let distance = request.max_distance.unwrap_or(2).min(10);
                for field in fields.fuzzy {
                    sources.push(Source {
                        label: format!("fuzzy:{field}"),
                        kind: SourceKind::Fuzzy,
                        docs: self.fuzzy_search_in(
                            rtxn,
                            &field,
                            query,
                            distance,
                            &request.filter,
                            Some(want),
                        )?,
                    });
                }
            }
            SearchMode::Vector => {
                let (field, vector) = self.search_all_vector_in(rtxn, request)?;
                sources.push(Source {
                    label: format!("vector:{field}"),
                    kind: SourceKind::Vector,
                    docs: self.vector_search_in(
                        rtxn,
                        &field,
                        &vector,
                        &request.filter,
                        0,
                        Some(want),
                    )?,
                });
            }
            SearchMode::Hybrid => {
                let query = request.query.as_deref().ok_or_else(|| {
                    Error::Query("search_all hybrid mode requires `query`".into())
                })?;
                if query.trim().is_empty() {
                    return Err(Error::Query("search_all `query` must not be empty".into()));
                }
                if fields.text.is_empty() {
                    return Err(Error::Query(
                        "search_all hybrid mode requires at least one text index".into(),
                    ));
                }
                for field in fields.text {
                    sources.push(Source {
                        label: format!("lexical:{field}"),
                        kind: SourceKind::Lexical,
                        docs: self.search_in(
                            rtxn,
                            &field,
                            query,
                            &request.filter,
                            0,
                            Some(want),
                        )?,
                    });
                }
                let (field, vector) = self.search_all_vector_in(rtxn, request)?;
                sources.push(Source {
                    label: format!("vector:{field}"),
                    kind: SourceKind::Vector,
                    docs: self.vector_search_in(
                        rtxn,
                        &field,
                        &vector,
                        &request.filter,
                        0,
                        Some(want),
                    )?,
                });
            }
        }

        let source_labels: Vec<String> =
            sources.iter().map(|source| source.label.clone()).collect();
        let mut fused: BTreeMap<String, Fused> = BTreeMap::new();
        for source in sources {
            for (index, doc) in source.docs.into_iter().enumerate() {
                let Some(id) = doc.get("_id").and_then(Value::as_str).map(str::to_string) else {
                    continue;
                };
                let rank = index + 1;
                let score = 1.0 / (rrf_k as f64 + rank as f64);
                let info = match source.kind {
                    SourceKind::Lexical => json!({
                        "rank": rank,
                        "score": doc.get("_score").cloned().unwrap_or(Value::Null),
                    }),
                    SourceKind::Fuzzy | SourceKind::Vector => json!({
                        "rank": rank,
                        "distance": doc.get("_distance").cloned().unwrap_or(Value::Null),
                    }),
                };
                let entry = fused.entry(id).or_insert_with(|| Fused {
                    doc,
                    score: 0.0,
                    sources: BTreeMap::new(),
                });
                entry.score += score;
                entry.sources.insert(source.label.clone(), info);
            }
        }

        let candidate_count = fused.len();
        let mut ranked: Vec<(String, Fused)> = fused.into_iter().collect();
        ranked.sort_by(|(id_a, a), (id_b, b)| {
            b.score.total_cmp(&a.score).then_with(|| id_a.cmp(id_b))
        });

        let results = ranked
            .into_iter()
            .skip(request.skip)
            .take(limit)
            .map(|(_id, mut entry)| {
                if let Some(object) = entry.doc.as_object_mut() {
                    object.remove("_score");
                    object.remove("_distance");
                    object.insert("_score".to_string(), json!(entry.score));
                    object.insert(
                        "_sources".to_string(),
                        Value::Object(Map::from_iter(entry.sources)),
                    );
                }
                entry.doc
            })
            .collect();

        Ok(SearchAllResponse {
            results,
            mode: request.mode,
            rrf_k,
            candidate_count,
            sources: source_labels,
        })
    }

    /// Convenience wrapper that opens its own read transaction.
    pub fn search_all(&self, request: &SearchAllRequest) -> Result<SearchAllResponse> {
        let rtxn = self.read_txn()?;
        let out = self.search_all_in(&rtxn, request);
        rtxn.commit()?;
        out
    }

    fn search_all_fields_in(
        &self,
        rtxn: &crate::heed::RoTxn<'_>,
        request: &SearchAllRequest,
    ) -> Result<SearchFields> {
        let mut text = Vec::new();
        let mut fuzzy = Vec::new();
        match request.mode {
            SearchMode::Lexical | SearchMode::Hybrid => {
                text = if request.fields.is_empty() {
                    self.list_text_indexes_in(rtxn)?
                } else {
                    request.fields.clone()
                };
                text.sort();
                text.dedup();
                if text.is_empty() {
                    return Err(Error::Query(format!(
                        "collection `{}` has no text index",
                        self.name()
                    )));
                }
            }
            SearchMode::Fuzzy => {
                fuzzy = if request.fields.is_empty() {
                    self.list_fuzzy_indexes_in(rtxn)?
                } else {
                    request.fields.clone()
                };
                fuzzy.sort();
                fuzzy.dedup();
                if fuzzy.is_empty() {
                    return Err(Error::Query(format!(
                        "collection `{}` has no fuzzy index",
                        self.name()
                    )));
                }
            }
            SearchMode::Vector => {}
        }
        Ok(SearchFields { text, fuzzy })
    }

    fn search_all_vector_in(
        &self,
        rtxn: &crate::heed::RoTxn<'_>,
        request: &SearchAllRequest,
    ) -> Result<(String, Vec<f32>)> {
        let vector = request
            .vector
            .clone()
            .ok_or_else(|| Error::Query("search_all vector mode requires `vector`".into()))?;
        let field = if let Some(field) = request.vector_field.as_deref() {
            field.to_string()
        } else {
            let defs = self.list_vector_indexes_in(rtxn)?;
            match defs.as_slice() {
                [def] => def.field.clone(),
                [] => {
                    return Err(Error::Query(format!(
                        "collection `{}` has no vector index",
                        self.name()
                    )))
                }
                _ => {
                    return Err(Error::Query(
                        "search_all requires `vector_field` when multiple vector indexes exist"
                            .into(),
                    ))
                }
            }
        };
        Ok((field, vector))
    }
}

struct SearchFields {
    text: Vec<String>,
    fuzzy: Vec<String>,
}

impl SearchFields {
    fn source_labels(&self) -> Vec<String> {
        self.text
            .iter()
            .map(|field| format!("lexical:{field}"))
            .chain(self.fuzzy.iter().map(|field| format!("fuzzy:{field}")))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;

    fn setup() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        (dir, store)
    }

    #[test]
    fn hybrid_uses_reciprocal_rank_fusion() {
        let (_dir, store) = setup();
        let collection = store.collection("docs").unwrap();
        collection
            .insert_one(json!({"_id": "a", "title": "coffee beans", "v": [1.0, 0.0]}))
            .unwrap();
        collection
            .insert_one(json!({"_id": "b", "title": "coffee grinder", "v": [0.8, 0.2]}))
            .unwrap();
        collection
            .insert_one(json!({"_id": "c", "title": "tea", "v": [0.0, 1.0]}))
            .unwrap();
        collection.create_text_index("title").unwrap();
        collection
            .create_vector_index("v", 2, crate::VectorMetric::Cosine)
            .unwrap();

        let out = collection
            .search_all(&SearchAllRequest {
                coll: "docs".into(),
                mode: SearchMode::Hybrid,
                query: Some("coffee".into()),
                vector: Some(vec![1.0, 0.0]),
                vector_field: Some("v".into()),
                limit: Some(10),
                ..SearchAllRequest {
                    coll: "docs".into(),
                    mode: SearchMode::Lexical,
                    query: None,
                    fields: Vec::new(),
                    vector: None,
                    vector_field: None,
                    filter: Value::Null,
                    skip: 0,
                    limit: None,
                    max_distance: None,
                    rrf_k: None,
                }
            })
            .unwrap();

        assert_eq!(out.mode, SearchMode::Hybrid);
        assert!(out.results.len() >= 2);
        assert!(out
            .results
            .iter()
            .all(|doc| doc.get("_sources").is_some() && doc.get("_score").is_some()));
        assert!(out.results.iter().any(|doc| doc["_id"] == "a"));
    }
}
