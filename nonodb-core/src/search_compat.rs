//! Compatibility subset for Atlas-style `$search`, `$searchMeta`, and
//! `$vectorSearch` aggregation stages.
//!
//! This is intentionally a small translation layer over the native search
//! primitives, not an Atlas Search implementation. Unsupported operators fail
//! loudly instead of silently changing semantics.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Map, Value};

use crate::{Collection, Error, FindQuery, Result, SearchAllRequest, SearchMode};

const DEFAULT_LIMIT: usize = 10;
const MAX_LIMIT: usize = 1000;
const RRF_K: usize = 60;

pub(crate) fn search_stage_in(
    collection: &Collection<'_>,
    rtxn: &crate::heed::RoTxn<'_>,
    spec: &Value,
    limit: usize,
) -> Result<Vec<Value>> {
    let want = limit.clamp(1, MAX_LIMIT);
    let mut docs = eval_search(collection, rtxn, spec, &Value::Null, want)?;
    docs.sort_by(|a, b| {
        score_of(b)
            .total_cmp(&score_of(a))
            .then_with(|| id_of(a).cmp(&id_of(b)))
    });
    docs.truncate(want);
    Ok(docs)
}

pub(crate) fn search_meta_stage_in(
    collection: &Collection<'_>,
    rtxn: &crate::heed::RoTxn<'_>,
    spec: &Value,
) -> Result<Value> {
    let spec = spec
        .as_object()
        .ok_or_else(|| Error::Query("$searchMeta requires an object".into()))?;
    let search = spec
        .get("search")
        .or_else(|| spec.get("$search"))
        .ok_or_else(|| Error::Query("$searchMeta requires a `search` object".into()))?;
    let docs = eval_search(collection, rtxn, search, &Value::Null, MAX_LIMIT)?;
    let total = docs.len();
    Ok(json!({
        "count": {"total": total},
        "meta": {
            "count": {"total": total},
            "capped": total >= MAX_LIMIT
        }
    }))
}

pub(crate) fn vector_search_stage_in(
    collection: &Collection<'_>,
    rtxn: &crate::heed::RoTxn<'_>,
    spec: &Value,
) -> Result<Vec<Value>> {
    let spec = spec
        .as_object()
        .ok_or_else(|| Error::Query("$vectorSearch requires an object".into()))?;
    let path = spec
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Query("$vectorSearch requires `path`".into()))?;
    let vector = spec
        .get("queryVector")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::Query("$vectorSearch requires `queryVector`".into()))?
        .iter()
        .map(|value| {
            crate::numeric::as_f64(value)
                .map(|number| number as f32)
                .ok_or_else(|| Error::Query("$vectorSearch queryVector must be numeric".into()))
        })
        .collect::<Result<Vec<_>>>()?;
    let limit = spec
        .get("limit")
        .and_then(as_usize)
        .unwrap_or(DEFAULT_LIMIT)
        .min(MAX_LIMIT);
    let num_candidates = spec
        .get("numCandidates")
        .and_then(as_usize)
        .unwrap_or(limit);
    if num_candidates < limit {
        return Err(Error::Query(
            "$vectorSearch numCandidates must be >= limit".into(),
        ));
    }
    let filter = spec.get("filter").cloned().unwrap_or(Value::Null);
    let mut docs = collection.vector_search_in(rtxn, path, &vector, &filter, 0, Some(limit))?;
    for doc in &mut docs {
        let distance = doc.get("_distance").and_then(Value::as_f64).unwrap_or(0.0);
        let score = 1.0 / (1.0 + distance.abs());
        if let Some(object) = doc.as_object_mut() {
            object.insert("vectorSearchScore".to_string(), json!(score));
        }
    }
    Ok(docs)
}

fn eval_search(
    collection: &Collection<'_>,
    rtxn: &crate::heed::RoTxn<'_>,
    spec: &Value,
    filter: &Value,
    limit: usize,
) -> Result<Vec<Value>> {
    let object = spec
        .as_object()
        .ok_or_else(|| Error::Query("$search requires an object".into()))?;
    let mut operators: Vec<(&String, &Value)> = object
        .iter()
        .filter(|(name, _)| name.as_str() != "index")
        .collect();
    if operators.is_empty() {
        return Err(Error::Query("$search has no operator".into()));
    }
    if operators.len() > 1 {
        return Err(Error::Query(
            "$search supports one top-level operator; use compound".into(),
        ));
    }
    let (name, arg) = operators.pop().expect("one operator");
    match name.as_str() {
        "text" => text_search(collection, rtxn, arg, filter, limit, false),
        "phrase" => text_search(collection, rtxn, arg, filter, limit, true),
        "equals" => field_filter(collection, rtxn, arg, filter, limit, false),
        "range" => field_filter(collection, rtxn, arg, filter, limit, true),
        "compound" => compound_search(collection, rtxn, arg, filter, limit),
        other => Err(Error::Query(format!(
            "unsupported $search operator `{other}`"
        ))),
    }
}

fn text_search(
    collection: &Collection<'_>,
    rtxn: &crate::heed::RoTxn<'_>,
    arg: &Value,
    filter: &Value,
    limit: usize,
    phrase: bool,
) -> Result<Vec<Value>> {
    let object = arg
        .as_object()
        .ok_or_else(|| Error::Query("$search text requires an object".into()))?;
    let query = object
        .get("query")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Query("$search text requires `query`".into()))?;
    let fields = parse_paths(object.get("path"))?;
    let fuzzy = object.get("fuzzy");
    if phrase && fuzzy.is_some() {
        return Err(Error::Query(
            "$search phrase does not support fuzzy options".into(),
        ));
    }
    let (mode, max_distance) = if let Some(fuzzy) = fuzzy {
        let fuzzy = fuzzy
            .as_object()
            .ok_or_else(|| Error::Query("$search text.fuzzy requires an object".into()))?;
        if fuzzy
            .get("prefixLength")
            .and_then(as_usize)
            .is_some_and(|value| value > 0)
        {
            return Err(Error::Query(
                "$search text.fuzzy.prefixLength is not supported".into(),
            ));
        }
        let edits = fuzzy.get("maxEdits").and_then(as_usize).unwrap_or(2);
        (SearchMode::Fuzzy, Some(edits.min(10)))
    } else {
        (SearchMode::Lexical, None)
    };
    let query = if phrase {
        format!("\"{}\"", query.replace('"', "\\\""))
    } else {
        query.to_string()
    };
    let out = collection.search_all_in(
        rtxn,
        &SearchAllRequest {
            coll: collection.name().to_string(),
            mode,
            query: Some(query),
            fields,
            vector: None,
            vector_field: None,
            filter: filter.clone(),
            skip: 0,
            limit: Some(limit),
            max_distance,
            rrf_k: Some(RRF_K),
        },
    )?;
    Ok(out.results)
}

fn field_filter(
    collection: &Collection<'_>,
    rtxn: &crate::heed::RoTxn<'_>,
    arg: &Value,
    filter: &Value,
    limit: usize,
    range: bool,
) -> Result<Vec<Value>> {
    let object = arg
        .as_object()
        .ok_or_else(|| Error::Query("$search filter requires an object".into()))?;
    let path = object
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Query("$search filter requires `path`".into()))?;
    let condition = if range {
        let mut condition = Map::new();
        for (key, value) in object {
            if matches!(key.as_str(), "path" | "score") {
                continue;
            }
            if !matches!(key.as_str(), "gt" | "gte" | "lt" | "lte") {
                return Err(Error::Query(format!(
                    "unsupported $search range comparison `{key}`"
                )));
            }
            condition.insert(format!("${key}"), value.clone());
        }
        if condition.is_empty() {
            return Err(Error::Query("$search range requires gt/gte/lt/lte".into()));
        }
        Value::Object(condition)
    } else {
        let value = object
            .get("value")
            .ok_or_else(|| Error::Query("$search equals requires `value`".into()))?;
        value.clone()
    };
    let mut field_filter = Map::new();
    field_filter.insert(path.to_string(), condition);
    let combined = combine_filters(filter, &Value::Object(field_filter))?;
    let mut docs = collection.find_in(
        rtxn,
        &FindQuery {
            filter: combined,
            limit: Some(limit.min(MAX_LIMIT)),
            ..Default::default()
        },
    )?;
    for doc in &mut docs {
        if let Some(object) = doc.as_object_mut() {
            object.insert("_score".to_string(), json!(1.0));
        }
    }
    Ok(docs)
}

fn compound_search(
    collection: &Collection<'_>,
    rtxn: &crate::heed::RoTxn<'_>,
    arg: &Value,
    filter: &Value,
    limit: usize,
) -> Result<Vec<Value>> {
    let object = arg
        .as_object()
        .ok_or_else(|| Error::Query("$search compound requires an object".into()))?;
    for key in object.keys() {
        if !matches!(key.as_str(), "must" | "should" | "mustNot" | "filter") {
            return Err(Error::Query(format!(
                "unsupported $search compound clause `{key}`"
            )));
        }
    }
    let must = clauses(object.get("must"))?;
    let should = clauses(object.get("should"))?;
    let must_not = clauses(object.get("mustNot"))?;
    let filters = clauses(object.get("filter"))?;

    if must.is_empty() && should.is_empty() && filters.is_empty() {
        return Err(Error::Query(
            "$search compound requires must, should, or filter".into(),
        ));
    }

    let mut scores: BTreeMap<String, (Value, f64)> = BTreeMap::new();
    if !must.is_empty() {
        let mut intersection: Option<BTreeSet<String>> = None;
        for clause in &must {
            let docs = eval_search(collection, rtxn, clause, filter, limit)?;
            let ids: BTreeSet<String> = docs.iter().map(id_of).collect();
            intersection = Some(match intersection {
                Some(current) => current.intersection(&ids).cloned().collect(),
                None => ids,
            });
            for (rank, doc) in docs.into_iter().enumerate() {
                let id = id_of(&doc);
                let entry = scores.entry(id).or_insert((doc, 0.0));
                entry.1 += 1.0 / (RRF_K as f64 + rank as f64 + 1.0);
            }
        }
        let allowed = intersection.unwrap_or_default();
        scores.retain(|id, _| allowed.contains(id));
    }

    if !should.is_empty() {
        for clause in &should {
            let docs = eval_search(collection, rtxn, clause, filter, limit)?;
            for (rank, doc) in docs.into_iter().enumerate() {
                let id = id_of(&doc);
                let entry = scores.entry(id).or_insert((doc, 0.0));
                entry.1 += 1.0 / (RRF_K as f64 + rank as f64 + 1.0);
            }
        }
    }

    if !filters.is_empty() {
        let mut allowed: Option<BTreeSet<String>> = None;
        for clause in &filters {
            let docs = eval_search(collection, rtxn, clause, filter, limit)?;
            let ids: BTreeSet<String> = docs.iter().map(id_of).collect();
            allowed = Some(match allowed {
                Some(current) => current.intersection(&ids).cloned().collect(),
                None => ids,
            });
            for doc in docs {
                let id = id_of(&doc);
                scores.entry(id).or_insert((doc, 0.0));
            }
        }
        let allowed = allowed.unwrap_or_default();
        scores.retain(|id, _| allowed.contains(id));
    }

    if !must_not.is_empty() {
        let mut excluded = BTreeSet::new();
        for clause in &must_not {
            let docs = eval_search(collection, rtxn, clause, filter, limit)?;
            excluded.extend(docs.iter().map(id_of));
        }
        scores.retain(|id, _| !excluded.contains(id));
    }

    let mut out: Vec<Value> = scores
        .into_iter()
        .map(|(_id, (mut doc, score))| {
            if let Some(object) = doc.as_object_mut() {
                object.insert("_score".to_string(), json!(score));
            }
            doc
        })
        .collect();
    out.sort_by(|a, b| {
        score_of(b)
            .total_cmp(&score_of(a))
            .then_with(|| id_of(a).cmp(&id_of(b)))
    });
    out.truncate(limit);
    Ok(out)
}

fn clauses(value: Option<&Value>) -> Result<Vec<&Value>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    match value {
        Value::Object(_) => Ok(vec![value]),
        Value::Array(items) => Ok(items.iter().collect()),
        _ => Err(Error::Query(
            "$search compound clauses must be objects or arrays".into(),
        )),
    }
}

fn parse_paths(value: Option<&Value>) -> Result<Vec<String>> {
    let Some(value) = value else {
        return Err(Error::Query("$search text requires `path`".into()));
    };
    match value {
        Value::String(path) => Ok(vec![path.clone()]),
        Value::Array(paths) => paths
            .iter()
            .map(|path| {
                path.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| Error::Query("$search path entries must be strings".into()))
            })
            .collect(),
        _ => Err(Error::Query(
            "$search path must be a string or array of strings".into(),
        )),
    }
}

fn combine_filters(left: &Value, right: &Value) -> Result<Value> {
    let left_empty = left.is_null() || left.as_object().is_some_and(Map::is_empty);
    let right_empty = right.is_null() || right.as_object().is_some_and(Map::is_empty);
    Ok(if left_empty {
        right.clone()
    } else if right_empty {
        left.clone()
    } else {
        json!({"$and": [left, right]})
    })
}

fn as_usize(value: &Value) -> Option<usize> {
    let number = crate::numeric::as_f64(value)?;
    if number.is_finite() && number >= 0.0 {
        Some(number as usize)
    } else {
        None
    }
}

fn id_of(doc: &Value) -> String {
    doc.get("_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn score_of(doc: &Value) -> f64 {
    doc.get("_score").and_then(Value::as_f64).unwrap_or(0.0)
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
    fn compound_text_and_filter() {
        let (_dir, store) = setup();
        let c = store.collection("products").unwrap();
        c.insert_one(json!({"_id": "a", "title": "coffee beans", "status": "active"}))
            .unwrap();
        c.insert_one(json!({"_id": "b", "title": "coffee grinder", "status": "hidden"}))
            .unwrap();
        c.create_text_index("title").unwrap();
        let rtxn = c.read_txn().unwrap();
        let docs = search_stage_in(
            &c,
            &rtxn,
            &json!({"compound": {
                "must": {"text": {"query": "coffee", "path": "title"}},
                "filter": {"equals": {"path": "status", "value": "active"}}
            }}),
            10,
        )
        .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["_id"], "a");
        rtxn.commit().unwrap();
    }

    #[test]
    fn aggregation_search_and_meta_stages() {
        let (_dir, store) = setup();
        let c = store.collection("products").unwrap();
        c.insert_one(json!({"_id": "a", "title": "coffee beans", "status": "active"}))
            .unwrap();
        c.insert_one(json!({"_id": "b", "title": "coffee grinder", "status": "active"}))
            .unwrap();
        c.insert_one(json!({"_id": "c", "title": "tea", "status": "active"}))
            .unwrap();
        c.create_text_index("title").unwrap();

        let docs = c
            .aggregate(&[
                json!({"$search": {"text": {"query": "coffee", "path": "title"}}}),
                json!({"$limit": 1}),
                json!({"$project": {"_id": 1, "_score": 1}}),
            ])
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert!(docs[0]["_score"].as_f64().unwrap() > 0.0);

        let meta = c
            .aggregate(&[json!({"$searchMeta": {
                "search": {"text": {"query": "coffee", "path": "title"}}
            }})])
            .unwrap();
        assert_eq!(meta[0]["count"]["total"], 2);
    }

    #[test]
    fn aggregation_vector_search_stage() {
        let (_dir, store) = setup();
        let c = store.collection("vectors").unwrap();
        c.insert_one(json!({"_id": "a", "v": [1.0, 0.0]})).unwrap();
        c.insert_one(json!({"_id": "b", "v": [0.0, 1.0]})).unwrap();
        c.create_vector_index("v", 2, crate::VectorMetric::Cosine)
            .unwrap();
        let docs = c
            .aggregate(&[json!({"$vectorSearch": {
                "path": "v",
                "queryVector": [1.0, 0.0],
                "limit": 1,
                "numCandidates": 10
            }})])
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["_id"], "a");
        assert!(docs[0]["vectorSearchScore"].as_f64().unwrap() > 0.0);
    }

    #[test]
    fn fuzzy_max_edits_zero_is_exact() {
        let (_dir, store) = setup();
        let c = store.collection("products").unwrap();
        c.insert_one(json!({"_id": "a", "title": "coffee beans"}))
            .unwrap();
        c.create_fuzzy_index("title").unwrap();
        let rtxn = c.read_txn().unwrap();
        let docs = search_stage_in(
            &c,
            &rtxn,
            &json!({"text": {
                "query": "cofee",
                "path": "title",
                "fuzzy": {"maxEdits": 0}
            }}),
            10,
        )
        .unwrap();
        assert!(docs.is_empty(), "{docs:?}");
        rtxn.commit().unwrap();
    }

    #[test]
    fn unsupported_operator_is_rejected() {
        let (_dir, store) = setup();
        let c = store.collection("products").unwrap();
        let rtxn = c.read_txn().unwrap();
        let err = search_stage_in(&c, &rtxn, &json!({"autocomplete": {}}), 10).unwrap_err();
        assert!(err.to_string().contains("unsupported $search operator"));
        rtxn.commit().unwrap();
    }

    #[test]
    fn vector_stage_returns_score() {
        let (_dir, store) = setup();
        let c = store.collection("vectors").unwrap();
        c.insert_one(json!({"_id": "a", "v": [1.0, 0.0]})).unwrap();
        c.insert_one(json!({"_id": "b", "v": [0.0, 1.0]})).unwrap();
        c.create_vector_index("v", 2, crate::VectorMetric::Cosine)
            .unwrap();
        let rtxn = c.read_txn().unwrap();
        let docs = vector_search_stage_in(
            &c,
            &rtxn,
            &json!({"path": "v", "queryVector": [1.0, 0.0], "limit": 1}),
        )
        .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["_id"], "a");
        assert!(docs[0]["vectorSearchScore"].as_f64().unwrap() > 0.0);
        rtxn.commit().unwrap();
    }
}
