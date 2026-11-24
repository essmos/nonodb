use nonodb_core::cypher::CypherOptions;
use nonodb_core::graph::GraphDef;
use nonodb_core::Store;
use serde_json::{json, Value};
use tempfile::TempDir;

fn graph_store() -> (TempDir, Store) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store
        .create_collection("nodes", nonodb_core::CollectionProfile::Standard)
        .unwrap();
    store
        .create_collection("edges", nonodb_core::CollectionProfile::Standard)
        .unwrap();
    let nodes = store.collection("nodes").unwrap();
    let edges = store.collection("edges").unwrap();
    for doc in [
        json!({"_id": "alice", "name": "Alice", "age": 30, "_labels": ["Person"]}),
        json!({"_id": "bob", "name": "Bob", "age": 20, "_labels": ["Person"]}),
        json!({"_id": "carol", "name": "Carol", "age": 40, "_labels": ["Person"]}),
        json!({"_id": "acme", "name": "Acme", "_labels": ["Company"]}),
    ] {
        nodes.insert_one(doc).unwrap();
    }
    for doc in [
        json!({"_id": "e1", "from": "alice", "to": "bob", "label": "KNOWS", "since": 2020}),
        json!({"_id": "e2", "from": "bob", "to": "carol", "label": "KNOWS", "since": 2021}),
        json!({"_id": "e3", "from": "alice", "to": "carol", "label": "KNOWS", "since": 2022}),
        json!({"_id": "e4", "from": "alice", "to": "acme", "label": "WORKS_AT"}),
    ] {
        edges.insert_one(doc).unwrap();
    }
    let def: GraphDef = serde_json::from_value(json!({
        "node_collection": "nodes",
        "edge_collection": "edges"
    }))
    .unwrap();
    nodes.set_graph(def).unwrap();
    (dir, store)
}

fn run(store: &Store, query: &str) -> Value {
    let collection = store.collection("nodes").unwrap();
    let options: CypherOptions = serde_json::from_value(json!({
        "query": query,
        "max_rows": 100,
        "max_operations": 100_000
    }))
    .unwrap();
    collection
        .graph_cypher(options)
        .unwrap_or_else(|error| panic!("{query}: {error}"))
}

#[test]
fn cypher_reads_nodes_paths_and_aggregates() {
    let (_dir, store) = graph_store();
    let out = run(
        &store,
        "MATCH (n:Person) WHERE n.age > 20 RETURN n.name AS name ORDER BY name",
    );
    assert_eq!(out["columns"], json!(["name"]));
    assert_eq!(out["rows"], json!([{"name": "Alice"}, {"name": "Carol"}]));

    let out = run(
        &store,
        "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person) \
         RETURN b.name AS name, r.since AS since ORDER BY since",
    );
    assert_eq!(
        out["rows"],
        json!([
            {"name": "Bob", "since": 2020},
            {"name": "Carol", "since": 2022}
        ])
    );

    let out = run(
        &store,
        "MATCH (n:Person) RETURN count(n) AS count, avg(n.age) AS avg_age",
    );
    assert_eq!(out["rows"][0]["count"], 3);
    assert_eq!(out["rows"][0]["avg_age"], 30.0);
}

#[test]
fn cypher_variable_paths_and_shortest_path() {
    let (_dir, store) = graph_store();
    let out = run(
        &store,
        "MATCH p=(a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b:Person) \
         RETURN b.name AS name, length(p) AS len ORDER BY name, len",
    );
    assert_eq!(
        out["rows"],
        json!([
            {"name": "Bob", "len": 1},
            {"name": "Carol", "len": 1},
            {"name": "Carol", "len": 2}
        ])
    );

    let out = run(
        &store,
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Carol'}) \
         MATCH p=shortestPath((a)-[:KNOWS*1..3]->(b)) RETURN length(p) AS len",
    );
    assert_eq!(out["row_count"], 1);
    assert_eq!(out["rows"][0]["len"], 1);
}

#[test]
fn cypher_rejects_write_queries() {
    let (_dir, store) = graph_store();
    let collection = store.collection("nodes").unwrap();
    let options: CypherOptions = serde_json::from_value(json!({
        "query": "CREATE (n:Person {name: 'Eve'}) RETURN n"
    }))
    .unwrap();
    let error = collection.graph_cypher(options).unwrap_err().to_string();
    assert!(error.contains("read-only"));
}

#[test]
fn cypher_gds_procedures_are_bounded_and_callable() {
    let (_dir, store) = graph_store();
    let out = run(
        &store,
        "CALL gds.pagerank.stream() YIELD nodeId, score \
         RETURN nodeId, score ORDER BY nodeId",
    );
    assert_eq!(out["row_count"], 4);
    assert!(out["rows"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["score"].as_f64().is_some()));

    let out = run(
        &store,
        "CALL gds.degree.stream() YIELD nodeId, degree \
         RETURN nodeId, degree ORDER BY nodeId",
    );
    assert_eq!(out["row_count"], 4);
    assert!(out["rows"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["nodeId"] == "carol" && row["degree"] == 2));

    let out = run(
        &store,
        "CALL gds.connectedComponents.stream() YIELD nodeId, componentId \
         RETURN nodeId, componentId ORDER BY nodeId",
    );
    assert_eq!(out["row_count"], 4);

    let out = run(
        &store,
        "CALL gds.nodeSimilarity.stream() YIELD node1, node2, similarity \
         RETURN node1, node2, similarity ORDER BY node1, node2",
    );
    assert!(out["row_count"].as_u64().unwrap() > 0);
}

#[test]
fn cypher_clauses_join_optional_match_and_branching() {
    let (_dir, store) = graph_store();
    let out = run(
        &store,
        "UNWIND [3, 1, 2] AS x WITH x ORDER BY x SKIP 1 LIMIT 1 RETURN x",
    );
    assert_eq!(out["rows"], json!([{"x": 2}]));

    let out = run(
        &store,
        "MATCH (n:Person) WITH n.age AS age ORDER BY age RETURN collect(age) AS ages",
    );
    assert_eq!(out["rows"], json!([{"ages": [20, 30, 40]}]));

    let out = run(
        &store,
        "MATCH (n:Person) OPTIONAL MATCH (n)-[:MISSING]->(m) \
         RETURN n.name AS name, m.name AS missing ORDER BY name",
    );
    assert_eq!(out["row_count"], 3);
    assert!(out["rows"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["missing"].is_null()));

    let out = run(
        &store,
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b), \
         (a)-[:WORKS_AT]->(c) RETURN b.name AS friend, c.name AS company",
    );
    assert_eq!(
        out["rows"],
        json!([{"friend": "Bob", "company": "Acme"}, {"friend": "Carol", "company": "Acme"}])
    );

    let out = run(
        &store,
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Carol'}) \
         MATCH p=allShortestPaths((a)-[:KNOWS*1..3]->(b)) RETURN length(p) AS len",
    );
    assert_eq!(out["row_count"], 1);
    assert_eq!(out["rows"][0]["len"], 1);
}

#[test]
fn cypher_distinct_and_explain_are_deterministic() {
    let (_dir, store) = graph_store();
    let out = run(
        &store,
        "MATCH (n) RETURN DISTINCT labels(n) AS labels ORDER BY labels",
    );
    assert_eq!(out["row_count"], 2);

    let out = run(
        &store,
        "EXPLAIN MATCH (n:Person) WHERE n.age > 20 RETURN n.name",
    );
    assert_eq!(out["row_count"], 1);
    assert!(out["rows"][0]["plan"].as_str().unwrap().contains("Scan"));
}

#[test]
fn cypher_limits_and_nesting_fail_explicitly() {
    let (_dir, store) = graph_store();
    let collection = store.collection("nodes").unwrap();
    let options: CypherOptions = serde_json::from_value(json!({
        "query": "UNWIND range(1, 1000000) AS x RETURN sum(x)",
        "max_rows": 10,
        "max_operations": 50
    }))
    .unwrap();
    let error = collection.graph_cypher(options).unwrap_err().to_string();
    assert!(error.contains("operation budget"));

    let options: CypherOptions = serde_json::from_value(json!({
        "query": "RETURN [[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]] AS x"
    }))
    .unwrap();
    let error = collection.graph_cypher(options).unwrap_err().to_string();
    assert!(error.contains("nesting"));

    let options: CypherOptions = serde_json::from_value(json!({
        "query": "UNWIND range(1, 1000) AS x RETURN x",
        "max_memory_bytes": 100
    }))
    .unwrap();
    let error = collection.graph_cypher(options).unwrap_err().to_string();
    assert!(error.contains("memory budget"), "{error}");
}

#[test]
fn cypher_unbound_variables_fail_explicitly() {
    let (_dir, store) = graph_store();
    let collection = store.collection("nodes").unwrap();
    let options: CypherOptions = serde_json::from_value(json!({
        "query": "RETURN missing"
    }))
    .unwrap();
    let error = collection.graph_cypher(options).unwrap_err().to_string();
    assert!(error.contains("unbound variable"));
}

#[test]
fn cypher_is_deterministic_and_rejects_vault_collections() {
    let (_dir, store) = graph_store();
    let collection = store.collection("nodes").unwrap();
    let options: CypherOptions = serde_json::from_value(json!({
        "query": "MATCH (n:Person) RETURN n.name AS name ORDER BY name"
    }))
    .unwrap();
    let first = collection.graph_cypher(options.clone()).unwrap();
    let second = collection.graph_cypher(options).unwrap();
    assert_eq!(first, second);
    assert_eq!(
        first["rows"],
        json!([{"name": "Alice"}, {"name": "Bob"}, {"name": "Carol"}])
    );

    store
        .create_collection("vault", nonodb_core::CollectionProfile::Vault)
        .unwrap();
    let vault = store.collection("vault").unwrap();
    let options: CypherOptions = serde_json::from_value(json!({"query": "RETURN 1 AS x"})).unwrap();
    let error = vault.graph_cypher(options).unwrap_err().to_string();
    assert!(error.contains("vault"));
}

#[test]
fn cypher_timeout_cancels_long_queries() {
    let (_dir, store) = graph_store();
    let collection = store.collection("nodes").unwrap();
    let options: CypherOptions = serde_json::from_value(json!({
        "query": "UNWIND range(1, 100000000) AS x RETURN sum(x)",
        "timeout_ms": 1,
        "max_operations": 1_000_000_000
    }))
    .unwrap();
    let error = collection.graph_cypher(options).unwrap_err().to_string();
    assert!(error.contains("timed out"), "{error}");
}

#[test]
fn cypher_gds_handles_empty_graphs_and_row_bounds() {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store
        .create_collection("nodes", nonodb_core::CollectionProfile::Standard)
        .unwrap();
    store
        .create_collection("edges", nonodb_core::CollectionProfile::Standard)
        .unwrap();
    let nodes = store.collection("nodes").unwrap();
    let def: GraphDef = serde_json::from_value(json!({
        "node_collection": "nodes",
        "edge_collection": "edges"
    }))
    .unwrap();
    nodes.set_graph(def).unwrap();
    let out = run(
        &store,
        "CALL gds.pagerank.stream() YIELD nodeId, score RETURN nodeId, score",
    );
    assert_eq!(out["row_count"], 0);

    let (_dir, store) = graph_store();
    let collection = store.collection("nodes").unwrap();
    let options: CypherOptions = serde_json::from_value(json!({
        "query": "CALL gds.pagerank.stream() YIELD nodeId, score RETURN nodeId, score",
        "max_rows": 1
    }))
    .unwrap();
    let out = collection.graph_cypher(options).unwrap();
    assert_eq!(out["row_count"], 1);
    assert_eq!(out["truncated"], true);
}

#[test]
fn cypher_optional_match_preserves_bound_variables() {
    let (_dir, store) = graph_store();
    let out = run(
        &store,
        "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) \
         RETURN n.name AS name, m.name AS friend ORDER BY name, friend",
    );
    assert_eq!(
        out["rows"],
        json!([
            {"name": "Alice", "friend": "Bob"},
            {"name": "Alice", "friend": "Carol"},
            {"name": "Bob", "friend": "Carol"},
            {"name": "Carol", "friend": null}
        ])
    );

    let out = run(
        &store,
        "MATCH (n:Person) OPTIONAL MATCH (n)-[:MISSING]->(m) \
         RETURN n.name AS name, m.name AS friend ORDER BY name",
    );
    assert_eq!(
        out["rows"],
        json!([
            {"name": "Alice", "friend": null},
            {"name": "Bob", "friend": null},
            {"name": "Carol", "friend": null}
        ])
    );
}

#[test]
fn cypher_pattern_expressions_and_comprehensions() {
    let (_dir, store) = graph_store();
    let out = run(
        &store,
        "MATCH (n:Person) WHERE (n)-[:KNOWS]->() RETURN n.name AS name ORDER BY name",
    );
    assert_eq!(out["rows"], json!([{"name": "Alice"}, {"name": "Bob"}]));

    let out = run(
        &store,
        "MATCH (n:Person) WHERE NOT (n)-[:KNOWS]->() RETURN n.name AS name ORDER BY name",
    );
    assert_eq!(out["rows"], json!([{"name": "Carol"}]));

    let out = run(
        &store,
        "MATCH (n:Person) RETURN n.name AS name, \
         [(n)-[:KNOWS]->(m) | m.name] AS friends ORDER BY name",
    );
    assert_eq!(
        out["rows"],
        json!([
            {"name": "Alice", "friends": ["Bob", "Carol"]},
            {"name": "Bob", "friends": ["Carol"]},
            {"name": "Carol", "friends": []}
        ])
    );
}

#[test]
fn cypher_rejects_non_gds_procedures() {
    let (_dir, store) = graph_store();
    for name in [
        "apoc.pagerank.stream",
        "my.pagerank.stream",
        "evil.similarity",
    ] {
        let collection = store.collection("nodes").unwrap();
        let options: CypherOptions = serde_json::from_value(json!({
            "query": format!("CALL {name}() YIELD nodeId RETURN nodeId")
        }))
        .unwrap();
        let error = collection.graph_cypher(options).unwrap_err().to_string();
        assert!(
            error.contains("unsupported Cypher procedure"),
            "{name}: {error}"
        );
    }
}
