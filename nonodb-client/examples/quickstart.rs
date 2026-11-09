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

    println!("collections: {:?}", db.collections()?);
    Ok(())
}
