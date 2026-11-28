//! Search and graph tests for the experimental columnar profile.

use nonodb_core::search_all::{SearchAllRequest, SearchMode};
use nonodb_core::{CollectionProfile, Store, VectorMetric};
use serde_json::json;

fn docs() -> Vec<serde_json::Value> {
    vec![
        json!({"_id": "a", "body": "rust database engine", "name": "hello", "embedding": [1.0, 0.0]}),
        json!({"_id": "b", "body": "python web framework", "name": "world", "embedding": [0.0, 1.0]}),
        json!({"_id": "c", "body": "rust web service", "name": "helper", "embedding": [0.7, 0.7]}),
    ]
}

fn text_ids(c: &nonodb_core::Collection<'_>, q: &str) -> Vec<String> {
    let mut ids: Vec<String> = c
        .search("body", q, &json!({}), 0, Some(10))
        .unwrap()
        .into_iter()
        .map(|doc| doc["_id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    ids
}

#[test]
fn columnar_text_fuzzy_and_vector_indexes_backfill_and_track_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let c = store
        .create_columnar_collection("docs", &nonodb_core::columnar::Settings::default())
        .unwrap();
    for doc in docs() {
        c.insert_one(doc).unwrap();
    }
    c.create_text_segment_index("body").unwrap();
    c.create_fuzzy_segment_index("name").unwrap();
    c.create_vector_index("embedding", 2, VectorMetric::Cosine)
        .unwrap();

    assert_eq!(text_ids(&c, "rust"), vec!["a", "c"]);
    assert_eq!(
        c.fuzzy_search("name", "helo", 1, &json!({}), Some(10))
            .unwrap()[0]["_id"],
        "a"
    );
    let vector = c
        .vector_search("embedding", &[1.0, 0.0], &json!({}), 0, Some(3))
        .unwrap();
    assert_eq!(vector[0]["_id"], "a");

    c.update(
        &json!({"_id": "a"}),
        &json!({"$set": {"body": "python only", "name": "hellos"}}),
        false,
    )
    .unwrap();
    assert_eq!(text_ids(&c, "rust"), vec!["c"]);
    assert_eq!(text_ids(&c, "python"), vec!["a", "b"]);
    c.delete(&json!({"_id": "b"}), false).unwrap();
    assert_eq!(text_ids(&c, "python"), vec!["a"]);
    assert!(c
        .fuzzy_search("name", "world", 1, &json!({}), Some(10))
        .unwrap()
        .is_empty());

    let report = c.reindex().unwrap();
    assert_eq!(report.text, vec!["body"]);
    assert_eq!(report.fuzzy, vec!["name"]);
    assert_eq!(report.vector, vec!["embedding"]);
    assert_eq!(text_ids(&c, "python"), vec!["a"]);
}

#[test]
fn columnar_search_all_matches_standard_in_every_mode() {
    let sd = tempfile::tempdir().unwrap();
    let cd = tempfile::tempdir().unwrap();
    let ss = Store::open(sd.path()).unwrap();
    let cs = Store::open(cd.path()).unwrap();
    let sc = ss
        .create_collection("docs", CollectionProfile::Standard)
        .unwrap();
    let cc = cs
        .create_columnar_collection("docs", &nonodb_core::columnar::Settings::default())
        .unwrap();
    for doc in docs() {
        sc.insert_one(doc.clone()).unwrap();
        cc.insert_one(doc).unwrap();
    }
    for collection in [&sc, &cc] {
        collection.create_text_index("body").unwrap();
        collection.create_fuzzy_index("name").unwrap();
        collection
            .create_vector_index("embedding", 2, VectorMetric::Cosine)
            .unwrap();
    }
    let request = SearchAllRequest {
        coll: "docs".into(),
        mode: SearchMode::Lexical,
        query: None,
        fields: Vec::new(),
        vector: None,
        vector_field: None,
        filter: serde_json::Value::Null,
        skip: 0,
        limit: Some(10),
        max_distance: None,
        rrf_k: None,
    };
    let cases = [
        SearchAllRequest {
            mode: SearchMode::Lexical,
            query: Some("rust".into()),
            ..request.clone()
        },
        SearchAllRequest {
            mode: SearchMode::Fuzzy,
            query: Some("helo".into()),
            ..request.clone()
        },
        SearchAllRequest {
            mode: SearchMode::Vector,
            vector: Some(vec![1.0, 0.0]),
            vector_field: Some("embedding".into()),
            ..request.clone()
        },
        SearchAllRequest {
            mode: SearchMode::Hybrid,
            query: Some("rust".into()),
            vector: Some(vec![1.0, 0.0]),
            vector_field: Some("embedding".into()),
            ..request.clone()
        },
    ];
    for case in cases {
        let expected = sc.search_all(&case).unwrap();
        let actual = cc.search_all(&case).unwrap();
        assert_eq!(actual.mode, expected.mode, "mode {case:?}");
        assert_eq!(actual.sources, expected.sources, "sources {case:?}");
        assert_eq!(actual.results, expected.results, "results {case:?}");
    }
}

#[test]
fn columnar_graph_mutations_and_traversal_match_standard() {
    let sd = tempfile::tempdir().unwrap();
    let cd = tempfile::tempdir().unwrap();
    let ss = Store::open(sd.path()).unwrap();
    let cs = Store::open(cd.path()).unwrap();

    for (store, columnar) in [(&ss, false), (&cs, true)] {
        if columnar {
            store
                .create_columnar_collection("nodes", &nonodb_core::columnar::Settings::default())
                .unwrap();
            store
                .create_columnar_collection("edges", &nonodb_core::columnar::Settings::default())
                .unwrap();
        } else {
            store
                .create_collection("nodes", CollectionProfile::Standard)
                .unwrap();
            store
                .create_collection("edges", CollectionProfile::Standard)
                .unwrap();
        }
    }
    let sn = ss.collection("nodes").unwrap();
    let se = ss.collection("edges").unwrap();
    let cn = cs.collection("nodes").unwrap();
    let ce = cs.collection("edges").unwrap();
    let def: nonodb_core::graph::GraphDef = serde_json::from_value(json!({
        "node_collection": "nodes",
        "edge_collection": "edges"
    }))
    .unwrap();
    sn.set_graph(def.clone()).unwrap();
    cn.set_graph(def.clone()).unwrap();

    for id in ["n1", "n2", "n3"] {
        sn.insert_one(json!({"_id": id, "kind": "node"})).unwrap();
        cn.insert_one(json!({"_id": id, "kind": "node"})).unwrap();
    }
    for (id, from, to) in [("e1", "n1", "n2"), ("e2", "n2", "n3")] {
        se.insert_one(json!({"_id": id, "from": from, "to": to}))
            .unwrap();
        ce.insert_one(json!({"_id": id, "from": from, "to": to}))
            .unwrap();
    }
    let opts: nonodb_core::graph::TraverseOptions = serde_json::from_value(json!({
        "start": ["n1"],
        "max_depth": 2,
        "include_edges": true
    }))
    .unwrap();
    let expected = sn.graph_traverse(opts.clone()).unwrap();
    let actual = cn.graph_traverse(opts).unwrap();
    assert_eq!(actual, expected);

    let shortest: nonodb_core::graph::ShortestPathOptions =
        serde_json::from_value(json!({"start": "n1", "target": "n3", "max_depth": 4})).unwrap();
    assert_eq!(
        cn.graph_shortest_path(shortest.clone()).unwrap(),
        sn.graph_shortest_path(shortest).unwrap()
    );
    let all: nonodb_core::graph::AllShortestPathsOptions =
        serde_json::from_value(json!({"start": "n1", "target": "n3", "max_depth": 4})).unwrap();
    assert_eq!(
        cn.graph_all_shortest_paths(all.clone()).unwrap(),
        sn.graph_all_shortest_paths(all).unwrap()
    );
    let neighbors: nonodb_core::graph::NeighborsOptions =
        serde_json::from_value(json!({"start": "n2", "include_edges": true})).unwrap();
    assert_eq!(
        cn.graph_neighbors(neighbors.clone()).unwrap(),
        sn.graph_neighbors(neighbors).unwrap()
    );
    let algorithm: nonodb_core::graph::GraphAlgorithmOptions =
        serde_json::from_value(json!({"kind": "pagerank", "limit": 10})).unwrap();
    assert_eq!(
        cn.graph_algorithm(algorithm.clone()).unwrap(),
        sn.graph_algorithm(algorithm).unwrap()
    );

    let lookup = json!([
        {"$graphLookup": {
            "from": "edges",
            "startWith": "n3",
            "connectFromField": "from",
            "connectToField": "to",
            "as": "chain",
            "maxDepth": 2
        }}
    ]);
    let expected = sn.aggregate(lookup.as_array().unwrap()).unwrap();
    let actual = cn.aggregate(lookup.as_array().unwrap()).unwrap();
    assert_eq!(actual, expected);

    ce.update(&json!({"_id": "e2"}), &json!({"$set": {"to": "n2"}}), false)
        .unwrap();
    se.update(&json!({"_id": "e2"}), &json!({"$set": {"to": "n2"}}), false)
        .unwrap();
    let expected = sn
        .graph_traverse(serde_json::from_value(json!({"start": ["n1"], "max_depth": 2})).unwrap())
        .unwrap();
    let actual = cn
        .graph_traverse(serde_json::from_value(json!({"start": ["n1"], "max_depth": 2})).unwrap())
        .unwrap();
    assert_eq!(actual, expected);
}
