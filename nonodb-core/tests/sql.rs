use nonodb_core::{sql, Store};
use serde_json::json;
use tempfile::TempDir;

fn fixture() -> (TempDir, Store) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let c = store.collection("users").unwrap();
    for doc in [
        json!({"_id": "a", "name": "Ada", "age": 36, "city": "London", "team": "core", "score": 10}),
        json!({"_id": "b", "name": "Grace", "age": 31, "city": "New York", "team": "core", "score": 20}),
        json!({"_id": "c", "name": "Linus", "age": 25, "city": "Helsinki", "team": "kernel", "score": 30}),
        json!({"_id": "d", "name": "Margaret", "age": 28, "city": "New York", "team": "kernel", "score": 40}),
        json!({"_id": "e", "name": "Noor", "age": 36, "city": null, "team": "core"}),
    ] {
        c.insert_one(doc).unwrap();
    }
    (dir, store)
}

#[test]
fn select_projects_columns_and_excludes_id() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();
    let rows = sql::execute(
        &c,
        "SELECT name, age FROM users WHERE age >= 31 ORDER BY age DESC",
        &[],
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![
            json!({"age": 36, "name": "Ada"}),
            json!({"age": 36, "name": "Noor"}),
            json!({"age": 31, "name": "Grace"}),
        ]
    );
}

#[test]
fn select_star_supports_where_order_limit_offset() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();
    let rows = sql::execute(
        &c,
        "SELECT * FROM users WHERE team = 'core' ORDER BY score DESC LIMIT 1 OFFSET 1",
        &[],
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["_id"], "b");
}

#[test]
fn aliases_table_alias_and_parameters_work() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();
    let rows = sql::execute(
        &c,
        "SELECT u.name AS person, u.age FROM users AS u WHERE u.city = $1 AND u.age > ? ORDER BY person",
        &[json!("New York"), json!(29)],
    )
    .unwrap();
    assert_eq!(rows, vec![json!({"person": "Grace", "age": 31})]);
}

#[test]
fn in_between_is_null_and_like_predicates_work() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();

    let rows = sql::execute(
        &c,
        "SELECT name FROM users WHERE age BETWEEN 26 AND 36 AND city IN ('London', 'New York')",
        &[],
    )
    .unwrap();
    assert_eq!(rows.len(), 3);

    let rows = sql::execute(&c, "SELECT name FROM users WHERE city IS NULL", &[]).unwrap();
    assert_eq!(rows, vec![json!({"name": "Noor"})]);

    let rows = sql::execute(&c, "SELECT name FROM users WHERE name LIKE 'M%'", &[]).unwrap();
    assert_eq!(rows, vec![json!({"name": "Margaret"})]);

    let rows = sql::execute(
        &c,
        "SELECT name FROM users WHERE name NOT LIKE '%a%' ORDER BY name",
        &[],
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![json!({"name": "Linus"}), json!({"name": "Noor"})]
    );
}

#[test]
fn aggregate_without_group_returns_one_row() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();
    let rows = sql::execute(
        &c,
        "SELECT COUNT(*) AS n, SUM(score) AS total, AVG(score) AS avg_score, MIN(age) AS youngest, MAX(age) AS oldest FROM users",
        &[],
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["n"], 5);
    assert_eq!(rows[0]["total"], 100.0);
    assert_eq!(rows[0]["avg_score"], 25.0);
    assert_eq!(rows[0]["youngest"], 25);
    assert_eq!(rows[0]["oldest"], 36);
}

#[test]
fn group_by_aggregates_and_orders() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();
    let rows = sql::execute(
        &c,
        "SELECT team, COUNT(*) AS n, SUM(score) AS total FROM users GROUP BY team ORDER BY total DESC",
        &[],
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], json!({"team": "kernel", "n": 2, "total": 70}));
    assert_eq!(rows[1], json!({"team": "core", "n": 3, "total": 30}));
}

#[test]
fn invalid_sql_is_reported() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();
    let err = sql::execute(&c, "DELETE FROM users", &[]).unwrap_err();
    assert!(err.to_string().contains("invalid SQL"), "{err}");
    let err = sql::execute(&c, "SELECT name FROM users WHERE", &[]).unwrap_err();
    assert!(err.to_string().contains("invalid SQL"), "{err}");
    let err = sql::execute(&c, "SELECT name FROM users WHERE age = $2", &[json!(1)]).unwrap_err();
    assert!(err.to_string().contains("missing SQL parameter"), "{err}");
}

#[test]
fn collection_name_can_be_extracted_for_authz() {
    assert_eq!(
        sql::collection_name("SELECT name FROM users WHERE age > ?").unwrap(),
        "users"
    );
    assert!(sql::collection_name("not sql").is_err());
}

#[test]
fn unicode_strings_and_standalone_offset_work() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();
    c.insert_one(json!({"_id": "u", "name": "Zażółć gęślą jaźń", "age": 20}))
        .unwrap();
    let rows = sql::execute(
        &c,
        "SELECT name FROM users WHERE name = 'Zażółć gęślą jaźń'",
        &[],
    )
    .unwrap();
    assert_eq!(rows, vec![json!({"name": "Zażółć gęślą jaźń"})]);

    let rows = sql::execute(&c, "SELECT name FROM users ORDER BY name OFFSET 1", &[]).unwrap();
    assert_eq!(rows.len(), 5);
}

#[test]
fn id_can_be_renamed_without_leaking_id() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();
    let rows = sql::execute(
        &c,
        "SELECT _id AS id, name AS n FROM users WHERE name = 'Ada'",
        &[],
    )
    .unwrap();
    assert_eq!(rows, vec![json!({"id": "a", "n": "Ada"})]);
}

#[test]
fn distinct_projects_and_deduplicates() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();
    let rows = sql::execute(&c, "SELECT DISTINCT team FROM users ORDER BY team", &[]).unwrap();
    assert_eq!(
        rows,
        vec![json!({"team": "core"}), json!({"team": "kernel"})]
    );

    let rows = sql::execute(
        &c,
        "SELECT DISTINCT lower(city) AS city FROM users WHERE city IS NOT NULL ORDER BY city",
        &[],
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![
            json!({"city": "helsinki"}),
            json!({"city": "london"}),
            json!({"city": "new york"}),
        ]
    );
}

#[test]
fn having_filters_groups_and_can_reference_unselected_aggregates() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();
    let rows = sql::execute(
        &c,
        "SELECT team, COUNT(*) AS n FROM users GROUP BY team HAVING COUNT(*) > 2",
        &[],
    )
    .unwrap();
    assert_eq!(rows, vec![json!({"team": "core", "n": 3})]);

    let rows = sql::execute(
        &c,
        "SELECT name AS person, team AS squad, COUNT(*) AS n FROM users GROUP BY name, team \
         HAVING n >= 1 AND squad = 'core' ORDER BY person",
        &[],
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![
            json!({"person": "Ada", "squad": "core", "n": 1}),
            json!({"person": "Grace", "squad": "core", "n": 1}),
            json!({"person": "Noor", "squad": "core", "n": 1}),
        ]
    );

    let rows = sql::execute(
        &c,
        "SELECT team FROM users GROUP BY team HAVING SUM(score) >= 50 ORDER BY team",
        &[],
    )
    .unwrap();
    assert_eq!(rows, vec![json!({"team": "kernel"})]);
}

#[test]
fn case_functions_and_arithmetic_work() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();
    let rows = sql::execute(
        &c,
        "SELECT name, age + 1 AS next_age, upper(name) AS shout, length(name) AS n, \
         coalesce(city, 'unknown') AS city FROM users WHERE name = 'Ada'",
        &[],
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![json!({
            "name": "Ada",
            "next_age": 37,
            "shout": "ADA",
            "n": 3,
            "city": "London"
        })]
    );

    let rows = sql::execute(
        &c,
        "SELECT CASE WHEN age >= 35 THEN 'senior' WHEN age >= 30 THEN 'mid' ELSE 'junior' END AS band \
         FROM users WHERE name = 'Ada'",
        &[],
    )
    .unwrap();
    assert_eq!(rows, vec![json!({"band": "senior"})]);

    let rows = sql::execute(
        &c,
        "SELECT substr(name, 1, 2) AS prefix, name || '!' AS bang, nullif(age, 36) AS maybe, \
         NOT (age >= 40) AS young FROM users WHERE name = 'Ada'",
        &[],
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![json!({"prefix": "da", "bang": "Ada!", "maybe": null, "young": true})]
    );
}

#[test]
fn explain_uses_find_for_simple_selects_and_pipeline_for_groups() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();
    c.create_index("age", false).unwrap();

    let plan = sql::explain(&c, "SELECT name FROM users WHERE age > 30 LIMIT 2", &[]).unwrap();
    assert_eq!(plan["access_path"], "index_scan", "{plan}");
    assert_eq!(plan["index_used"], true, "{plan}");
    assert_eq!(plan["index"]["fields"], json!(["age"]), "{plan}");

    let plan = sql::explain(
        &c,
        "SELECT team, COUNT(*) AS n FROM users GROUP BY team HAVING COUNT(*) > 1",
        &[],
    )
    .unwrap();
    assert_eq!(plan["access_path"], "aggregation_group", "{plan}");
    assert_eq!(plan["has_having"], true, "{plan}");
    assert!(plan["pipeline"].as_array().is_some_and(|p| !p.is_empty()));
}

#[test]
fn distinct_can_combine_with_group_by_and_having() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();

    let rows = sql::execute(
        &c,
        "SELECT DISTINCT team, COUNT(*) AS n FROM users GROUP BY team HAVING COUNT(*) > 2",
        &[],
    )
    .unwrap();
    assert_eq!(rows, vec![json!({"team": "core", "n": 3})]);

    let rows = sql::execute(
        &c,
        "SELECT DISTINCT team FROM users GROUP BY team HAVING COUNT(*) >= 2 ORDER BY team",
        &[],
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![json!({"team": "core"}), json!({"team": "kernel"})]
    );

    let rows = sql::execute(&c, "SELECT DISTINCT * FROM users WHERE team = 'core'", &[]).unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn insert_and_update_statements_write_documents() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();

    let insert = sql::PreparedSql::parse(
        "INSERT INTO users (name, age, team, profile.score) \
         VALUES ('New', 20, 'ops', 5), ('Other', ?, 'ops', 7)",
    )
    .unwrap();
    assert!(!insert.is_read_only());
    let mut wtxn = store.write_txn().unwrap();
    let out = insert.execute_write(&c, &mut wtxn, &[json!(30)]).unwrap();
    wtxn.commit().unwrap();
    assert_eq!(out["inserted"], 2);
    assert_eq!(out["inserted_ids"].as_array().unwrap().len(), 2);

    let rows = sql::execute(
        &c,
        "SELECT name, age, profile.score AS score FROM users WHERE team = 'ops' ORDER BY name",
        &[],
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![
            json!({"name": "New", "age": 20, "score": 5}),
            json!({"name": "Other", "age": 30, "score": 7}),
        ]
    );

    let update = sql::PreparedSql::parse(
        "UPDATE users AS u SET u.age = $1, u.profile.score = $2 WHERE u.team = $3",
    )
    .unwrap();
    assert!(!update.is_read_only());
    let mut wtxn = store.write_txn().unwrap();
    let out = update
        .execute_write(&c, &mut wtxn, &[json!(40), json!(9), json!("ops")])
        .unwrap();
    wtxn.commit().unwrap();
    assert_eq!(out["matched"], 2);
    assert_eq!(out["modified"], 2);

    let rows = sql::execute(
        &c,
        "SELECT name, age, profile.score AS score FROM users WHERE team = 'ops' ORDER BY name",
        &[],
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![
            json!({"name": "New", "age": 40, "score": 9}),
            json!({"name": "Other", "age": 40, "score": 9}),
        ]
    );

    let err = sql::execute(&c, "UPDATE users SET age = 1", &[]).unwrap_err();
    assert!(err.to_string().contains("not a SELECT"), "{err}");
}

#[test]
fn sql_statement_access_distinguishes_reads_and_writes() {
    assert!(sql::is_read_only("SELECT * FROM users").unwrap());
    assert!(!sql::is_read_only("INSERT INTO users (name) VALUES ('Ada')").unwrap());
    assert!(!sql::is_read_only("UPDATE users SET name = 'Ada'").unwrap());
    assert_eq!(
        sql::statement_access("UPDATE users SET name = 'Ada'").unwrap(),
        ("users".to_string(), false)
    );
    assert!(sql::explain(
        &Store::open(TempDir::new().unwrap().path())
            .unwrap()
            .collection("users")
            .unwrap(),
        "INSERT INTO users (name) VALUES ('Ada')",
        &[]
    )
    .is_err());
}
