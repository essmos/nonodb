//! Request-to-scope authorization.

use nonodb_auth::{Action, Principal};

use crate::protocol::{ApiError, Request, WriteOp};

pub fn authorize_request(principal: &Principal, req: &Request) -> Result<(), ApiError> {
    match req {
        Request::Insert { coll, .. }
        | Request::InsertMany { coll, .. }
        | Request::Update { coll, .. }
        | Request::FindAndModify { coll, .. }
        | Request::Delete { coll, .. } => allow(principal, Action::Write, coll, req.op_name()),

        Request::Find { coll, .. }
        | Request::FindOne { coll, .. }
        | Request::Explain { coll, .. }
        | Request::Count { coll, .. }
        | Request::Aggregate { coll, .. }
        | Request::CursorFind { coll, .. }
        | Request::CursorAggregate { coll, .. }
        | Request::ListIndexes { coll }
        | Request::ListTextIndexes { coll }
        | Request::ListFuzzyIndexes { coll }
        | Request::Search { coll, .. }
        | Request::Fuzzy { coll, .. }
        | Request::History { coll, .. }
        | Request::GetVersion { coll, .. }
        | Request::GetCollectionTtl { coll } => allow(principal, Action::Read, coll, req.op_name()),

        Request::CreateIndex { coll, .. }
        | Request::DropIndex { coll, .. }
        | Request::CreateTextIndex { coll, .. }
        | Request::DropTextIndex { coll, .. }
        | Request::CreateFuzzyIndex { coll, .. }
        | Request::DropFuzzyIndex { coll, .. }
        | Request::CreateCollection { coll, .. }
        | Request::SetCollectionTtl { coll, .. } => {
            allow(principal, Action::AdminCollection, coll, req.op_name())
        }

        Request::MapInfo | Request::ResizeMap { .. } => {
            if principal.allows_admin_database() {
                Ok(())
            } else {
                Err(ApiError::forbidden())
            }
        }

        // The cursor registry verifies ownership. `get_more`/`close_cursor`
        // deliberately carry no collection name; the cursor stores it.
        Request::GetMore { .. } | Request::CloseCursor { .. } => Ok(()),
        Request::ListCollections => Ok(()),
        Request::Batch { ops } => {
            for op in ops {
                authorize_request(principal, op)?;
            }
            Ok(())
        }
        Request::WriteBatch { ops } => {
            for op in ops {
                authorize_write_op(principal, op)?;
            }
            Ok(())
        }
    }
}

pub fn authorize_write_op(principal: &Principal, op: &WriteOp) -> Result<(), ApiError> {
    let coll = match op {
        WriteOp::Insert { coll, .. }
        | WriteOp::InsertMany { coll, .. }
        | WriteOp::Update { coll, .. }
        | WriteOp::Delete { coll, .. } => coll,
    };
    allow(principal, Action::Write, coll, op.op_name())
}

fn allow(
    principal: &Principal,
    action: Action,
    coll: &str,
    op: &'static str,
) -> Result<(), ApiError> {
    if principal.allows(action, coll) {
        Ok(())
    } else {
        tracing::warn!(
            principal = principal.name(),
            op,
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
        }
    }

    #[test]
    fn collection_scopes_are_enforced() {
        let app = principal(&["read:orders", "write:orders", "admin:db"]);
        assert!(authorize_request(&app, &insert("orders")).is_ok());
        assert!(authorize_request(&app, &insert("customers")).is_err());
        assert!(authorize_request(
            &app,
            &Request::Find {
                coll: "orders".into(),
                filter: json!({}),
                projection: None,
                sort: vec![],
                skip: 0,
                limit: None,
            }
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
            }
        )
        .is_err());
        assert!(authorize_request(&app, &Request::MapInfo).is_ok());
    }

    #[test]
    fn write_batch_is_checked_per_operation() {
        let app = principal(&["write:orders"]);
        let ok = Request::WriteBatch {
            ops: vec![WriteOp::Insert {
                coll: "orders".into(),
                doc: json!({"x": 1}),
            }],
        };
        assert!(authorize_request(&app, &ok).is_ok());
        let mixed = Request::WriteBatch {
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
        assert!(authorize_request(&app, &mixed).is_err());
    }

    #[test]
    fn admin_collection_is_separate_from_write() {
        let writer = principal(&["write:orders"]);
        let index = Request::CreateIndex {
            coll: "orders".into(),
            field: Some("x".into()),
            fields: None,
            unique: false,
            include: vec![],
        };
        assert!(authorize_request(&writer, &index).is_err());
        let admin = principal(&["admin:orders"]);
        assert!(authorize_request(&admin, &index).is_ok());
        assert!(authorize_request(&admin, &Request::MapInfo).is_err());
    }

    #[test]
    fn wildcard_scopes_work() {
        let all = principal(&["*:*"]);
        assert!(authorize_request(&all, &insert("anything")).is_ok());
        assert!(authorize_request(&all, &Request::MapInfo).is_ok());
        let reader = principal(&["read:*"]);
        assert!(authorize_request(&reader, &insert("anything")).is_err());
        assert!(authorize_request(
            &reader,
            &Request::Find {
                coll: "anything".into(),
                filter: json!({}),
                projection: None,
                sort: vec![],
                skip: 0,
                limit: None,
            }
        )
        .is_ok());
    }
}
