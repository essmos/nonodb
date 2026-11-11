use nonodb_client::{Client, FindQuery};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Client::from_env()?;
    let users = db.collection("users");

    let id = users.insert(&json!({"name": "Ada", "age": 36, "tags": ["db", "rust"]}))?;
    println!("inserted {id}");

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

    if matches!(
        std::env::var("NONODB_HISTORY")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    ) {
        let revisions = users.history(&id, Some(10), true)?;
        println!("revisions: {}", revisions.len());
        if let Some(first) = revisions.last() {
            let version = first["rev"].as_u64().expect("revision");
            let old = users.get_version(&id, version)?;
            println!("old: {}", serde_json::to_string(&old)?);
        }
    }

    println!("collections: {:?}", db.collections()?);
    Ok(())
}
