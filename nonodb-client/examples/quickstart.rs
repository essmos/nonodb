use nonodb_client::{Client, FindQuery};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Client::from_env()?;
    db.create_collection("users", "standard")?;
    db.set_collection_history("users", 5)?;
    let capabilities = db.capabilities()?;
    assert_eq!(capabilities["schema_version"], 1);
    assert_eq!(capabilities["features"]["ttl"]["date_field"], true);

    let users = db.collection("users");

    let id = users.insert(&json!({"name": "Ada", "age": 36, "tags": ["db", "rust"]}))?;
    println!("inserted {id}");

    let keys = vec![id.clone(), "missing".to_string(), id.clone()];
    let chunked = users.lookup_chunked("_id", &keys, None, 1)?;
    assert_eq!(chunked.len(), 3);
    assert_eq!(chunked[0]["_id"], json!(id));
    assert!(chunked[1].is_null());
    assert_eq!(chunked[2]["_id"], json!(id));
    assert!(users.lookup_chunked("_id", &keys, None, 0).is_err());

    let local = vec![json!({"_id": id}), json!({"_id": "missing"})];
    let joined = users.lookup_join(&users, &local, "_id", Default::default())?;
    assert_eq!(joined[0]["joined"]["name"], json!("Ada"));
    assert!(joined[1]["joined"].is_null());
    assert_eq!(joined.len(), local.len());

    let adults = users.find(&FindQuery::new(json!({"age": {"$gte": 18}})).sort("age", -1))?;
    println!(
        "{} adult(s): {}",
        adults.len(),
        serde_json::to_string(&adults)?
    );

    let one = users.find_one(&json!({"_id": id}), None)?;
    println!("one: {}", serde_json::to_string(&one)?);

    let rows = users.aggregate(&[
        json!({"$match": {"age": {"$gte": 18}}}),
        json!({"$group": {"_id": null, "n": {"$sum": 1}}}),
    ])?;
    println!("aggregate: {}", serde_json::to_string(&rows)?);

    let sql_rows = db.sql(
        "SELECT COUNT(*) AS n FROM users WHERE age >= $1",
        &[json!(18)],
    )?;
    println!("sql: {}", serde_json::to_string(&sql_rows)?);
    let sql_plan = db.explain_sql("SELECT * FROM users WHERE age >= $1", &[json!(18)])?;
    println!("sql plan: {}", sql_plan["access_path"]);
    let sql_insert = db.sql(
        "INSERT INTO users (name, age) VALUES ($1, $2)",
        &[json!("Grace"), json!(31)],
    )?;
    println!("sql insert: {}", serde_json::to_string(&sql_insert)?);
    let sql_update = db.sql(
        "UPDATE users SET age = $1 WHERE name = $2",
        &[json!(32), json!("Grace")],
    )?;
    println!("sql update: {}", serde_json::to_string(&sql_update)?);

    let revisions = users.history(&id, Some(10), true)?;
    println!("revisions: {}", revisions.len());
    if let Some(first) = revisions.last() {
        let version = first["rev"].as_u64().expect("revision");
        let old = users.get_version(&id, version)?;
        println!("old: {}", serde_json::to_string(&old)?);
    }

    users.create_text_index("name")?;
    let unified = users.search_all(json!({
        "mode": "lexical",
        "query": "Grace",
        "fields": ["name"]
    }))?;
    println!("search_all: {}", serde_json::to_string(&unified)?);

    println!("collections: {:?}", db.collections()?);

    let columnar = "quickstart_columnar";
    db.create_collection_with_columnar(
        columnar,
        json!({"bucket_max_docs": 64, "bucket_max_bytes": 65536, "compression": "max"}),
    )?;
    let columnar_coll = db.collection(columnar);
    columnar_coll.insert(&json!({"kind": "columnar", "n": 1}))?;
    let settings = db.get_collection_columnar(columnar)?;
    assert_eq!(settings["columnar"]["bucket_max_docs"], json!(64));
    assert_eq!(settings["stats"]["rows"], json!(1));
    db.set_collection_columnar(columnar, json!({"compression": "balanced"}))?;
    assert_eq!(
        db.get_collection_columnar(columnar)?["columnar"]["compression"],
        json!("balanced")
    );
    let listed = db.exec(json!({"op": "list_collections"}))?;
    assert!(listed["collection_info"]
        .as_array()
        .is_some_and(|items| items
            .iter()
            .any(|item| item["name"] == json!(columnar) && item["profile"] == json!("columnar"))));
    Ok(())
}
