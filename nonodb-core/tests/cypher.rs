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
    collection.graph_cypher(options).unwrap()
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
