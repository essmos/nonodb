//! Request-to-scope authorization.

use nonodb_auth::{Action, Principal};

use crate::protocol::{ApiError, Request, WriteOp};

pub fn authorize_request(
    principal: &Principal,
    req: &Request,
    database: &str,
) -> Result<(), ApiError> {
    if req
        .collections()
        .iter()
        .any(|coll| coll == nonodb_core::CONTROL_DB)
    {
        return Err(ApiError::forbidden());
    }
    match req {
        Request::Insert { coll, .. }
        | Request::InsertMany { coll, .. }
        | Request::Update { coll, .. }
        | Request::FindAndModify { coll, .. }
        | Request::Delete { coll, .. } => {
            allow(principal, Action::Write, database, coll, req.op_name())
        }

        Request::Aggregate { coll, pipeline, .. } => {
            allow(principal, Action::Read, database, coll, req.op_name())?;
            let spec =
                nonodb_core::aggregate::write_stage_spec(pipeline).map_err(ApiError::from)?;
            if let Some(spec) = spec {
                if let Some(target_database) = &spec.database {
                    if target_database != database {
                        return Err(ApiError {
                            code: "invalid_query".into(),
                            message: "cross-database $out/$merge is not supported".into(),
                        });
                    }
                }
                allow(
                    principal,
                    Action::Write,
                    database,
                    &spec.target,
                    req.op_name(),
                )?;
            }
            Ok(())
        }

        Request::Find { coll, .. }
        | Request::FindOne { coll, .. }
        | Request::Lookup { coll, .. }
        | Request::ExplainLookup { coll, .. }
        | Request::Watch { coll, .. }
        | Request::Explain { coll, .. }
        | Request::Count { coll, .. }
        | Request::CursorFind { coll, .. }
        | Request::CursorAggregate { coll, .. }
        | Request::ListIndexes { coll }
        | Request::ListTextIndexes { coll }
        | Request::IndexStatus { coll }
        | Request::ListFuzzyIndexes { coll }
        | Request::ListVectorIndexes { coll }
        | Request::VectorSearch { coll, .. }
        | Request::GetGraph { coll }
        | Request::Traverse { coll, .. }
        | Request::ShortestPath { coll, .. }
        | Request::Neighbors { coll, .. }
        | Request::HybridSearch { coll, .. }
        | Request::GraphRebuildStatus { coll }
        | Request::Search { coll, .. }
        | Request::Fuzzy { coll, .. }
        | Request::History { coll, .. }
        | Request::GetVersion { coll, .. }
        | Request::GetCollectionTtl { coll }
        | Request::GetCollectionHistory { coll }
        | Request::GetCollectionEvents { coll }
        | Request::GetCollectionMetrics { coll }
        | Request::ListSubscriptions { coll } => {
            allow(principal, Action::Read, database, coll, req.op_name())
        }
        Request::SearchAll { request } => allow(
            principal,
            Action::Read,
            database,
            &request.coll,
            req.op_name(),
        ),
        Request::ListWebhooks | Request::ListWebhookDeliveries { .. } => {
            if principal.allows_database_admin(database) {
                Ok(())
            } else {
                Err(ApiError::forbidden())
            }
        }

        Request::Sql { query, .. } => {
            let (coll, read_only) =
                nonodb_core::sql::statement_access(query).map_err(|e| ApiError {
                    code: "invalid_sql".into(),
                    message: e.to_string(),
                })?;
            let action = if read_only {
                Action::Read
            } else {
                Action::Write
            };
            allow(principal, action, database, &coll, req.op_name())
        }
        Request::ExplainSql { query, .. } => {
            let coll = nonodb_core::sql::collection_name(query).map_err(|e| ApiError {
                code: "invalid_sql".into(),
                message: e.to_string(),
            })?;
            allow(principal, Action::Read, database, &coll, req.op_name())
        }

        Request::CreateIndex { coll, .. }
        | Request::CreateIndexes { coll, .. }
        | Request::DropIndex { coll, .. }
        | Request::CreateTextIndex { coll, .. }
        | Request::DropTextIndex { coll, .. }
        | Request::CreateFuzzyIndex { coll, .. }
        | Request::DropFuzzyIndex { coll, .. }
        | Request::CreateVectorIndex { coll, .. }
        | Request::Reindex { coll }
        | Request::DropGraph { coll }
        | Request::ReindexGraph { coll, .. }
        | Request::CancelGraphRebuild { coll }
        | Request::DropVectorIndex { coll, .. }
        | Request::CreateCollection { coll, .. }
        | Request::DropCollection { coll }
        | Request::SetCollectionTtl { coll, .. }
        | Request::SetCollectionTtlAfter { coll, .. }
        | Request::BackfillCollectionTtl { coll, .. }
        | Request::SetCollectionHistory { coll, .. }
        | Request::SetCollectionEvents { coll, .. }
        | Request::CreateSubscription { coll, .. }
        | Request::RenewSubscription { coll, .. }
        | Request::DeleteSubscription { coll, .. } => allow(
            principal,
            Action::AdminCollection,
            database,
            coll,
            req.op_name(),
        ),
        Request::SetGraph { coll, def } => {
            allow(
                principal,
                Action::AdminCollection,
                database,
                coll,
                req.op_name(),
            )?;
            if def.edge_collection != *coll {
                allow(
                    principal,
                    Action::AdminCollection,
                    database,
                    &def.edge_collection,
                    req.op_name(),
                )?;
            }
            Ok(())
        }
        Request::CreateWebhook { rule } => allow(
            principal,
            Action::AdminCollection,
            database,
            &rule.coll,
            req.op_name(),
        ),
        Request::DeleteWebhook { .. } | Request::RetryWebhookDelivery { .. } => {
            if principal.allows_database_admin(database) {
                Ok(())
            } else {
                Err(ApiError::forbidden())
            }
        }

        Request::ListDatabases => Ok(()),
        Request::CreateDatabase { .. } | Request::DropDatabase { .. } => {
            if principal.allows_admin_database() {
                Ok(())
            } else {
                Err(ApiError::forbidden())
            }
        }
        Request::GetQuota => Ok(()),
        Request::SetQuota { .. } => {
            if principal.allows_database_admin(database) {
                Ok(())
            } else {
                Err(ApiError::forbidden())
            }
        }
        Request::Sync | Request::MapInfo | Request::ResizeMap { .. } => {
            if principal.allows_database_admin(database) {
                Ok(())
            } else {
                Err(ApiError::forbidden())
            }
        }

        // The cursor registry verifies ownership. `get_more`/`close_cursor`
        // deliberately carry no collection name; the cursor stores it.
        Request::GetMore { .. } | Request::CloseCursor { .. } => Ok(()),
        Request::ListCollections | Request::DatabaseStats => Ok(()),
        Request::Batch { ops } => {
            for op in ops {
                authorize_request(principal, op, database)?;
            }
            Ok(())
        }
        Request::WriteBatch { ops, .. } => {
            for op in ops {
                authorize_write_op(principal, op, database)?;
            }
            Ok(())
        }
    }
}

pub fn allow_collection(
    principal: &Principal,
    action: Action,
    database: &str,
    coll: &str,
    op: &'static str,
) -> Result<(), ApiError> {
    allow(principal, action, database, coll, op)
}

pub fn authorize_write_op(
    principal: &Principal,
    op: &WriteOp,
    database: &str,
) -> Result<(), ApiError> {
    let coll = match op {
        WriteOp::Insert { coll, .. }
        | WriteOp::InsertMany { coll, .. }
        | WriteOp::Update { coll, .. }
        | WriteOp::Delete { coll, .. } => coll,
    };
    allow(principal, Action::Write, database, coll, op.op_name())
}

fn allow(
    principal: &Principal,
    action: Action,
    database: &str,
    coll: &str,
    op: &'static str,
) -> Result<(), ApiError> {
    if principal.allows(action, database, coll) {
        Ok(())
    } else {
        tracing::warn!(
            principal = principal.name(),
            op,
            database,
            coll,
            "authorization denied"
        );
        Err(ApiError::forbidden())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn principal(scopes: &[&str]) -> Principal {
        let scopes: Vec<String> = scopes.iter().map(|s| s.to_string()).collect();
        Principal::new("test", &scopes).unwrap()
    }

    fn insert(coll: &str) -> Request {
        Request::Insert {
            coll: coll.into(),
            doc: json!({"x": 1}),
            durability: None,
        }
    }

    #[test]
    fn collection_scopes_are_enforced() {
        let app = principal(&["read:orders", "write:orders", "admin:db"]);
        assert!(authorize_request(&app, &insert("orders"), "default").is_ok());
        assert!(authorize_request(&app, &insert("customers"), "default").is_err());
        assert!(authorize_request(
            &app,
            &Request::Find {
                coll: "orders".into(),
                filter: json!({}),
                projection: None,
                sort: vec![],
                skip: 0,
                limit: None,
                comment: None,
                max_time_ms: None,
                hint: None,
            },
            "default"
        )
        .is_ok());
        assert!(authorize_request(
            &app,
            &Request::Find {
                coll: "customers".into(),
                filter: json!({}),
                projection: None,
                sort: vec![],
                skip: 0,
                limit: None,
                comment: None,
                max_time_ms: None,
                hint: None,
            },
            "default"
        )
        .is_err());
        assert!(authorize_request(&app, &Request::MapInfo, "default").is_ok());
    }

    #[test]
    fn write_batch_is_checked_per_operation() {
        let app = principal(&["write:orders"]);
        let ok = Request::WriteBatch {
            durability: None,
            ops: vec![WriteOp::Insert {
                coll: "orders".into(),
                doc: json!({"x": 1}),
            }],
        };
        assert!(authorize_request(&app, &ok, "default").is_ok());
        let mixed = Request::WriteBatch {
            durability: None,
            ops: vec![
                WriteOp::Insert {
                    coll: "orders".into(),
                    doc: json!({"x": 1}),
                },
                WriteOp::Insert {
                    coll: "customers".into(),
                    doc: json!({"x": 1}),
                },
            ],
        };
        assert!(authorize_request(&app, &mixed, "default").is_err());
    }

    #[test]
    fn admin_collection_is_separate_from_write() {
        let writer = principal(&["write:orders"]);
        let index = Request::CreateIndex {
            coll: "orders".into(),
            field: Some("x".into()),
            fields: None,
            unique: false,
            sparse: false,
            partial_filter_expression: None,
            include: vec![],
            geo: false,
        };
        assert!(authorize_request(&writer, &index, "default").is_err());
        let admin = principal(&["admin:orders"]);
        assert!(authorize_request(&admin, &index, "default").is_ok());
        assert!(authorize_request(&admin, &Request::MapInfo, "default").is_err());
    }

    #[test]
    fn sql_uses_read_scope_from_the_query() {
        let reader = principal(&["read:orders"]);
        let ok = Request::Sql {
            query: "SELECT * FROM orders WHERE id = $1".into(),
            params: vec![json!(1)],
            durability: None,
        };
        assert!(authorize_request(&reader, &ok, "default").is_ok());

        let denied = Request::Sql {
            query: "SELECT * FROM customers".into(),
            params: vec![],
            durability: None,
        };
        assert!(authorize_request(&reader, &denied, "default").is_err());

        let write = Request::Sql {
            query: "INSERT INTO orders (id) VALUES ($1)".into(),
            params: vec![json!(1)],
            durability: None,
        };
        assert!(authorize_request(&principal(&["write:orders"]), &write, "default").is_ok());
        assert!(authorize_request(&reader, &write, "default").is_err());
        let update = Request::Sql {
            query: "UPDATE orders SET id = $1 WHERE id = $2".into(),
            params: vec![json!(2), json!(1)],
            durability: None,
        };
        assert!(authorize_request(&principal(&["write:orders"]), &update, "default").is_ok());
        assert!(authorize_request(&reader, &update, "default").is_err());

        let explain = Request::ExplainSql {
            query: "SELECT * FROM orders".into(),
            params: vec![],
        };
        assert!(authorize_request(&reader, &explain, "default").is_ok());
        let explain_denied = Request::ExplainSql {
            query: "SELECT * FROM customers".into(),
            params: vec![],
        };
        assert!(authorize_request(&reader, &explain_denied, "default").is_err());

        let delete = Request::Sql {
            query: "DELETE FROM orders WHERE id = $1".into(),
            params: vec![json!(1)],
            durability: None,
        };
        assert!(authorize_request(&principal(&["write:orders"]), &delete, "default").is_ok());
        assert!(authorize_request(&reader, &delete, "default").is_err());

        let bad = Request::Sql {
            query: "CREATE TABLE orders (id INT)".into(),
            params: vec![],
            durability: None,
        };
        let err = authorize_request(&reader, &bad, "default").unwrap_err();
        assert_eq!(err.code, "invalid_sql");
    }

    #[test]
    fn wildcard_scopes_work() {
        let all = principal(&["*:*"]);
        assert!(authorize_request(&all, &insert("anything"), "default").is_ok());
        assert!(authorize_request(&all, &Request::MapInfo, "default").is_ok());
        let reader = principal(&["read:*"]);
        assert!(authorize_request(&reader, &insert("anything"), "default").is_err());
        assert!(authorize_request(
            &reader,
            &Request::Find {
                coll: "anything".into(),
                filter: json!({}),
                projection: None,
                sort: vec![],
                skip: 0,
                limit: None,
                comment: None,
                max_time_ms: None,
                hint: None,
            },
            "default"
        )
        .is_ok());
    }

    #[test]
    fn database_qualified_scopes_are_enforced() {
        let app = principal(&["read:app1/orders", "write:app1/orders", "admin:app2/*"]);
        assert!(authorize_request(&app, &insert("orders"), "app1").is_ok());
        assert!(authorize_request(&app, &insert("orders"), "app2").is_err());
        assert!(authorize_request(&app, &insert("customers"), "app1").is_err());
        let index = Request::CreateIndex {
            coll: "anything".into(),
            field: Some("x".into()),
            fields: None,
            unique: false,
            sparse: false,
            partial_filter_expression: None,
            include: vec![],
            geo: false,
        };
        assert!(authorize_request(&app, &index, "app2").is_ok());
        assert!(authorize_request(&app, &index, "app1").is_err());
        assert!(authorize_request(&app, &Request::MapInfo, "app2").is_ok());
        assert!(authorize_request(&app, &Request::MapInfo, "app1").is_err());
        assert!(authorize_request(&app, &Request::ResizeMap { size: 4096 }, "app2").is_ok());
        assert!(authorize_request(&app, &Request::Sync, "app2").is_ok());
        assert!(authorize_request(&app, &Request::ListWebhooks, "app2").is_ok());
        assert!(authorize_request(&app, &Request::ListWebhooks, "app1").is_err());
        assert!(authorize_request(
            &app,
            &Request::CreateDatabase {
                name: "newdb".into(),
            },
            "app2"
        )
        .is_err());
        assert!(authorize_request(
            &app,
            &Request::DropDatabase {
                name: "app2".into(),
            },
            "app2"
        )
        .is_err());
        let legacy = principal(&["read:orders"]);
        assert!(authorize_request(&legacy, &insert("orders"), "app1").is_err());
        assert!(authorize_request(
            &legacy,
            &Request::Find {
                coll: "orders".into(),
                filter: json!({}),
                projection: None,
                sort: vec![],
                skip: 0,
                limit: None,
                comment: None,
                max_time_ms: None,
                hint: None,
            },
            "app1"
        )
        .is_ok());
    }
}
