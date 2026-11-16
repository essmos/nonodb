//! Experimental MongoDB wire-protocol frontend.
//!
//! This is deliberately a small compatibility layer over the existing reader
//! and single-writer paths. It implements handshake, basic CRUD, cursors, and
//! a small aggregate subset; unsupported commands fail with MongoDB-style
//! errors instead of silently behaving differently.

use std::io;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::time::Duration;

use bson::{Bson, Document};
use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::cursor;
use crate::protocol::{ApiError, Durability, WriteReq};

mod bson_map;

const OP_REPLY: i32 = 1;
const OP_QUERY: i32 = 2004;
const OP_MSG: i32 = 2013;
const MAX_MESSAGE_BYTES: usize = 48_000_000;
const MAX_BSON_BYTES: i32 = 16 * 1024 * 1024;
const MAX_WRITE_BATCH_SIZE: i32 = 100_000;
const MAX_WIRE_VERSION: i32 = 9;
static NEXT_RESPONSE_ID: AtomicI32 = AtomicI32::new(1);
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

struct Message {
    request_id: i32,
    op_code: i32,
    body: Document,
}

/// Starts the experimental MongoDB wire listener.
pub async fn serve(
    addr: &str,
    state: super::AppState,
    mut shutdown: watch::Receiver<bool>,
    drain_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?.to_string();
    tracing::warn!(
        %addr,
        bound = %bound,
        "experimental unauthenticated MongoDB wire listener enabled; handshake plus insert/find/getMore/killCursors only"
    );

    let mut connections = JoinSet::new();
    loop {
        let (stream, peer) = tokio::select! {
            _ = super::wait_for_shutdown(&mut shutdown) => break,
            accepted = listener.accept() => accepted?,
        };
        let shutdown = shutdown.clone();
        let state = state.clone();
        connections.spawn(async move {
            if let Err(error) = handle_connection(stream, state, shutdown).await {
                tracing::debug!(?error, %peer, "Mongo wire connection closed");
            }
        });
    }
    drop(listener);
    super::drain_connections(connections, drain_timeout, "mongo-wire").await;
    Ok(())
}

async fn handle_connection<S>(
    mut stream: S,
    state: super::AppState,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
    loop {
        let message = tokio::select! {
            _ = super::wait_for_shutdown(&mut shutdown) => return Ok(()),
            message = read_message(&mut stream) => message?,
        };
        let Some(message) = message else {
            return Ok(());
        };
        let response = dispatch_command(&state, connection_id, &message.body).await;
        let bytes = if message.op_code == OP_QUERY {
            encode_op_reply(
                NEXT_RESPONSE_ID.fetch_add(1, Ordering::Relaxed),
                message.request_id,
                &response,
            )?
        } else {
            encode_op_msg(
                NEXT_RESPONSE_ID.fetch_add(1, Ordering::Relaxed),
                message.request_id,
                &response,
            )?
        };
        stream.write_all(&bytes).await?;
        stream.flush().await?;
    }
}

async fn dispatch_command(
    state: &super::AppState,
    connection_id: u64,
    body: &Document,
) -> Document {
    let command = body
        .keys()
        .find(|key| !key.starts_with('$'))
        .map(String::as_str);
    let result = match command {
        Some("insert") => insert_command(state, body).await,
        Some("find") => find_command(state, connection_id, body).await,
        Some("getMore") => get_more_command(state, connection_id, body).await,
        Some("killCursors") => kill_cursors_command(state, connection_id, body),
        Some("update") => update_command(state, body).await,
        Some("delete") => delete_command(state, body).await,
        Some("findAndModify") | Some("findandmodify") => find_and_modify_command(state, body).await,
        Some("count") => count_command(state, body).await,
        Some("distinct") => distinct_command(state, body).await,
        Some("aggregate") => aggregate_command(state, body).await,
        Some("createIndexes") => create_indexes_command(state, body).await,
        Some("listIndexes") => list_indexes_command(state, body).await,
        Some("dropIndexes") => drop_indexes_command(state, body).await,
        Some("endSessions") | Some("refreshSessions") => Ok(doc_ok()),
        Some("ping")
        | Some("hello")
        | Some("isMaster")
        | Some("ismaster")
        | Some("buildInfo")
        | Some("getParameter")
        | Some("connectionStatus") => return command_reply(body),
        _ => return command_not_found(command.unwrap_or("<missing>")),
    };
    match result {
        Ok(reply) => reply,
        Err(error) => mongo_error(&error),
    }
}

fn effective_database(name: &str) -> String {
    if name.is_empty() || matches!(name, "admin" | "config" | "local") {
        crate::databases::DEFAULT_DATABASE.to_string()
    } else {
        name.to_string()
    }
}

fn database_name(body: &Document) -> String {
    effective_database(body.get_str("$db").unwrap_or("default"))
}

async fn database_for_write(
    root: &super::AppState,
    database: &str,
) -> Result<super::AppState, ApiError> {
    match root.databases.get(database).await {
        Ok(_) => root.for_database(database).await,
        Err(error) if error.code == "not_found" => {
            root.databases.create(database).await?;
            root.for_database(database).await
        }
        Err(error) => Err(error),
    }
}

async fn database_for_read(root: &super::AppState, database: &str) -> Option<super::AppState> {
    match root.for_database(database).await {
        Ok(state) => Some(state),
        Err(error) if error.code == "not_found" => None,
        Err(_) => None,
    }
}

async fn insert_command(state: &super::AppState, body: &Document) -> Result<Document, ApiError> {
    let collection = body
        .get_str("insert")
        .map_err(|_| bad_value("insert must name a collection"))?
        .to_string();
    let database = database_name(body);
    let documents = body
        .get_array("documents")
        .map_err(|_| bad_value("insert requires a documents array"))?;
    if documents.len() > MAX_WRITE_BATCH_SIZE as usize {
        return Err(ApiError {
            code: "invalid_query".into(),
            message: format!("insert batch exceeds {MAX_WRITE_BATCH_SIZE} documents"),
        });
    }
    let ordered = body.get_bool("ordered").unwrap_or(true);
    let runtime = database_for_write(state, &database).await?;
    let mut inserted = 0i32;
    let mut errors = Vec::new();
    for (index, item) in documents.iter().enumerate() {
        let Bson::Document(document) = item else {
            errors.push(write_error(
                index,
                &bad_value("insert document must be a BSON document"),
            ));
            if ordered {
                break;
            }
            continue;
        };
        let mut value = match bson_map::document_to_value(document) {
            Ok(value) => value,
            Err(message) => {
                errors.push(write_error(index, &bad_value(message)));
                if ordered {
                    break;
                }
                continue;
            }
        };
        if value.get("_id").is_none() {
            let oid = bson::oid::ObjectId::new();
            value
                .as_object_mut()
                .expect("document")
                .insert("_id".to_string(), json!({"$oid": oid.to_hex()}));
        }
        let (_, response) = super::submit_write(
            &runtime,
            WriteReq::Insert {
                coll: collection.clone(),
                doc: value,
                vault: None,
            },
            Durability::Full,
        )
        .await;
        let api = response.0;
        if let Some(error) = api.error {
            errors.push(write_error(index, &error));
            if ordered {
                break;
            }
        } else {
            inserted += 1;
        }
    }
    let mut reply = Document::new();
    reply.insert("n", Bson::Int32(inserted));
    reply.insert("ok", Bson::Double(1.0));
    if !errors.is_empty() {
        reply.insert("writeErrors", Bson::Array(errors));
    }
    Ok(reply)
}

async fn update_command(state: &super::AppState, body: &Document) -> Result<Document, ApiError> {
    let collection = body
        .get_str("update")
        .map_err(|_| bad_value("update must name a collection"))?
        .to_string();
    let database = database_name(body);
    let updates = body
        .get_array("updates")
        .map_err(|_| bad_value("update requires an updates array"))?;
    let ordered = body.get_bool("ordered").unwrap_or(true);
    let runtime = database_for_write(state, &database).await?;
    let mut matched = 0i64;
    let mut modified = 0i64;
    let mut upserted = Vec::new();
    let mut errors = Vec::new();
    for (index, item) in updates.iter().enumerate() {
        let Bson::Document(spec) = item else {
            errors.push(write_error(
                index,
                &bad_value("update entry must be a document"),
            ));
            if ordered {
                break;
            }
            continue;
        };
        if spec.contains_key("collation") || spec.contains_key("hint") || spec.contains_key("let") {
            errors.push(write_error(
                index,
                &bad_value("collation, hint, and let are not supported by the wire update command"),
            ));
            if ordered {
                break;
            }
            continue;
        }
        let filter = match document_field(spec, "q")? {
            Some(value) => value,
            None => json!({}),
        };
        let update = match spec.get("u") {
            Some(Bson::Document(document)) => {
                bson_map::document_to_value(document).map_err(bad_value)?
            }
            Some(Bson::Array(stages)) => {
                let mut values = Vec::with_capacity(stages.len());
                for stage in stages {
                    let Bson::Document(document) = stage else {
                        errors.push(write_error(
                            index,
                            &bad_value("update pipeline stages must be documents"),
                        ));
                        break;
                    };
                    values.push(bson_map::document_to_value(document).map_err(bad_value)?);
                }
                Value::Array(values)
            }
            _ => {
                errors.push(write_error(
                    index,
                    &bad_value("update requires an update document"),
                ));
                if ordered {
                    break;
                }
                continue;
            }
        };
        let mut array_filters = Vec::new();
        if let Ok(filters) = spec.get_array("arrayFilters") {
            for filter in filters {
                let Bson::Document(document) = filter else {
                    errors.push(write_error(
                        index,
                        &bad_value("arrayFilters entries must be documents"),
                    ));
                    break;
                };
                array_filters.push(bson_map::document_to_value(document).map_err(bad_value)?);
            }
        }
        let (_, response) = super::submit_write(
            &runtime,
            WriteReq::Update {
                coll: collection.clone(),
                filter,
                update,
                multi: spec.get_bool("multi").unwrap_or(false),
                upsert: spec.get_bool("upsert").unwrap_or(false),
                array_filters,
                vault: None,
            },
            Durability::Full,
        )
        .await;
        let api = response.0;
        if let Some(error) = api.error {
            errors.push(write_error(index, &error));
            if ordered {
                break;
            }
            continue;
        }
        let result = api.result.unwrap_or(Value::Null);
        let operation_matched = result["matched"].as_u64().unwrap_or(0) as i64;
        matched += operation_matched;
        modified += result["modified"].as_u64().unwrap_or(0) as i64;
        if let Some(key) = result["upserted_id"].as_str() {
            matched += 1;
            let id = resolve_internal_id(&runtime, &collection, key).await;
            upserted.push(bson::doc! {
                "index": index as i32,
                "_id": id.map(|value| bson_map::value_to_bson(&value).unwrap_or(Bson::Null)).unwrap_or(Bson::Null),
            });
        }
    }
    let mut reply = bson::doc! {
        "n": Bson::Int32(matched as i32),
        "nModified": Bson::Int32(modified as i32),
        "ok": 1.0,
    };
    if !upserted.is_empty() {
        reply.insert(
            "upserted",
            Bson::Array(upserted.into_iter().map(Bson::Document).collect()),
        );
    }
    if !errors.is_empty() {
        reply.insert("writeErrors", Bson::Array(errors));
    }
    Ok(reply)
}

async fn delete_command(state: &super::AppState, body: &Document) -> Result<Document, ApiError> {
    let collection = body
        .get_str("delete")
        .map_err(|_| bad_value("delete must name a collection"))?
        .to_string();
    let database = database_name(body);
    let deletes = body
        .get_array("deletes")
        .map_err(|_| bad_value("delete requires a deletes array"))?;
    let ordered = body.get_bool("ordered").unwrap_or(true);
    let runtime = database_for_write(state, &database).await?;
    let mut deleted = 0i64;
    let mut errors = Vec::new();
    for (index, item) in deletes.iter().enumerate() {
        let Bson::Document(spec) = item else {
            errors.push(write_error(
                index,
                &bad_value("delete entry must be a document"),
            ));
            if ordered {
                break;
            }
            continue;
        };
        if spec.contains_key("collation") || spec.contains_key("hint") {
            errors.push(write_error(
                index,
                &bad_value("collation and hint are not supported by the wire delete command"),
            ));
            if ordered {
                break;
            }
            continue;
        }
        let filter = document_field(spec, "q")?.unwrap_or_else(|| json!({}));
        let limit = int_field(spec, "limit").unwrap_or(0);
        if limit != 0 && limit != 1 {
            errors.push(write_error(
                index,
                &bad_value("delete limit must be 0 or 1"),
            ));
            if ordered {
                break;
            }
            continue;
        }
        let (_, response) = super::submit_write(
            &runtime,
            WriteReq::Delete {
                coll: collection.clone(),
                filter,
                multi: limit == 0,
                vault: None,
            },
            Durability::Full,
        )
        .await;
        let api = response.0;
        if let Some(error) = api.error {
            errors.push(write_error(index, &error));
            if ordered {
                break;
            }
        } else {
            let result = api.result.unwrap_or(Value::Null);
            deleted += result["deleted"].as_u64().unwrap_or(0) as i64;
        }
    }
    let mut reply = bson::doc! {
        "n": Bson::Int32(deleted as i32),
        "ok": 1.0,
    };
    if !errors.is_empty() {
        reply.insert("writeErrors", Bson::Array(errors));
    }
    Ok(reply)
}

async fn find_and_modify_command(
    state: &super::AppState,
    body: &Document,
) -> Result<Document, ApiError> {
    let collection = body
        .get_str("findAndModify")
        .or_else(|_| body.get_str("findandmodify"))
        .map_err(|_| bad_value("findAndModify must name a collection"))?
        .to_string();
    let database = database_name(body);
    let filter = document_field(body, "query")?.unwrap_or_else(|| json!({}));
    let projection = document_field(body, "fields")?;
    let sort = sort_fields(body)?;
    let remove = body.get_bool("remove").unwrap_or(false);
    let update = match body.get("update") {
        None => None,
        Some(Bson::Document(document)) => {
            Some(bson_map::document_to_value(document).map_err(bad_value)?)
        }
        Some(Bson::Array(stages)) => {
            let mut values = Vec::with_capacity(stages.len());
            for stage in stages {
                let Bson::Document(document) = stage else {
                    return Err(bad_value("update pipeline stages must be documents"));
                };
                values.push(bson_map::document_to_value(document).map_err(bad_value)?);
            }
            Some(Value::Array(values))
        }
        Some(_) => return Err(bad_value("update must be a document or pipeline")),
    };
    if remove && update.is_some() {
        return Err(bad_value("findAndModify cannot combine remove and update"));
    }
    if !remove && update.is_none() {
        return Err(bad_value("findAndModify requires remove or update"));
    }
    let mut array_filters = Vec::new();
    if let Ok(filters) = body.get_array("arrayFilters") {
        for filter in filters {
            let Bson::Document(document) = filter else {
                return Err(bad_value("arrayFilters entries must be documents"));
            };
            array_filters.push(bson_map::document_to_value(document).map_err(bad_value)?);
        }
    }
    let runtime = database_for_write(state, &database).await?;
    let (_, response) = super::submit_write(
        &runtime,
        WriteReq::FindAndModify {
            coll: collection.clone(),
            filter,
            update,
            sort,
            new: body.get_bool("new").unwrap_or(false),
            upsert: body.get_bool("upsert").unwrap_or(false),
            remove,
            array_filters,
            projection,
            vault: None,
        },
        Durability::Full,
    )
    .await;
    let api = response.0;
    if let Some(error) = api.error {
        return Err(error);
    }
    let result = api.result.unwrap_or(Value::Null);
    let value = result.get("value").cloned().unwrap_or(Value::Null);
    let matched = result["matched"].as_u64().unwrap_or(0) as i32;
    let mut last_error = bson::doc! {
        "n": matched,
        "updatedExisting": matched > 0,
    };
    if let Some(key) = result["upserted_id"].as_str() {
        if let Some(id) = resolve_internal_id(&runtime, &collection, key).await {
            last_error.insert(
                "upserted",
                bson_map::value_to_bson(&id).unwrap_or(Bson::Null),
            );
        }
        last_error.insert("n", 1i32);
        last_error.insert("updatedExisting", false);
    }
    let value_bson = if value.is_null() {
        Bson::Null
    } else {
        bson_map::value_to_bson(&value).map_err(ApiError::internal)?
    };
    Ok(bson::doc! {
        "lastErrorObject": Bson::Document(last_error),
        "value": value_bson,
        "ok": 1.0,
    })
}

async fn count_command(state: &super::AppState, body: &Document) -> Result<Document, ApiError> {
    let collection = body
        .get_str("count")
        .map_err(|_| bad_value("count must name a collection"))?
        .to_string();
    let database = database_name(body);
    let filter = document_field(body, "query")?.unwrap_or_else(|| json!({}));
    let max_time_ms = int_field(body, "maxTimeMS")
        .filter(|value| *value > 0)
        .map(|value| value as u64);
    let Some(runtime) = database_for_read(state, &database).await else {
        return Ok(bson::doc! {"n": 0i64, "ok": 1.0});
    };
    let store = runtime.store.clone();
    let coll = collection.clone();
    let count = tokio::task::spawn_blocking(move || {
        let c = store.collection(&coll)?;
        c.count_with_options(&filter, max_time_ms)
    })
    .await
    .map_err(|error| ApiError::internal(format!("count join error: {error}")))??;
    Ok(bson::doc! {
        "n": Bson::Int64(count as i64),
        "ok": 1.0,
    })
}

async fn distinct_command(state: &super::AppState, body: &Document) -> Result<Document, ApiError> {
    let collection = body
        .get_str("distinct")
        .map_err(|_| bad_value("distinct must name a collection"))?
        .to_string();
    let database = database_name(body);
    let key = body
        .get_str("key")
        .map_err(|_| bad_value("distinct requires a key"))?
        .to_string();
    let filter = document_field(body, "query")?.unwrap_or_else(|| json!({}));
    let Some(runtime) = database_for_read(state, &database).await else {
        return Ok(bson::doc! {"values": Bson::Array(Vec::new()), "ok": 1.0});
    };
    let field = format!("${key}");
    let pipeline = vec![
        json!({"$match": filter}),
        json!({"$unwind": {"path": field.clone(), "preserveNullAndEmptyArrays": true}}),
        json!({"$group": {"_id": field}}),
    ];
    let store = runtime.store.clone();
    let coll = collection.clone();
    let values = tokio::task::spawn_blocking(move || {
        let c = store.collection(&coll)?;
        let docs = c.aggregate(&pipeline)?;
        Ok::<Vec<Value>, nonodb_core::Error>(
            docs.into_iter()
                .map(|doc| doc.get("_id").cloned().unwrap_or(Value::Null))
                .collect(),
        )
    })
    .await
    .map_err(|error| ApiError::internal(format!("distinct join error: {error}")))??;
    let values = values_to_bson(values)?;
    Ok(bson::doc! {
        "values": Bson::Array(values),
        "ok": 1.0,
    })
}

async fn aggregate_command(state: &super::AppState, body: &Document) -> Result<Document, ApiError> {
    let collection = body
        .get_str("aggregate")
        .map_err(|_| bad_value("aggregate must name a collection"))?;
    if collection == "1" {
        return Err(bad_value("database-level aggregate is not supported"));
    }
    let collection = collection.to_string();
    let database = database_name(body);
    let stages = body
        .get_array("pipeline")
        .map_err(|_| bad_value("aggregate requires a pipeline array"))?;
    let mut pipeline = Vec::with_capacity(stages.len());
    for stage in stages {
        let Bson::Document(document) = stage else {
            return Err(bad_value("aggregate stages must be documents"));
        };
        pipeline.push(bson_map::document_to_value(document).map_err(bad_value)?);
    }
    let max_time_ms = int_field(body, "maxTimeMS")
        .filter(|value| *value > 0)
        .map(|value| value as u64);
    if nonodb_core::aggregate::write_stage_spec(&pipeline)?.is_some() {
        let runtime = database_for_write(state, &database).await?;
        let (_, response) = super::submit_write(
            &runtime,
            WriteReq::Aggregate {
                coll: collection,
                pipeline,
                max_time_ms,
                database: database.clone(),
            },
            Durability::Full,
        )
        .await;
        let api = response.0;
        if let Some(error) = api.error {
            return Err(error);
        }
        return Ok(cursor_reply(
            0,
            &format!("{database}.$cmd.aggregate"),
            Vec::new(),
            true,
        ));
    }
    let Some(runtime) = database_for_read(state, &database).await else {
        return Ok(cursor_reply(
            0,
            &format!("{database}.{collection}"),
            Vec::new(),
            true,
        ));
    };
    let store = runtime.store.clone();
    let coll = collection.clone();
    let docs = tokio::task::spawn_blocking(move || {
        let c = store.collection(&coll)?;
        c.aggregate_with_options(&pipeline, max_time_ms)
    })
    .await
    .map_err(|error| ApiError::internal(format!("aggregate join error: {error}")))??;
    let docs = values_to_bson(docs)?;
    Ok(cursor_reply(
        0,
        &format!("{database}.{collection}"),
        docs,
        true,
    ))
}

async fn create_indexes_command(
    state: &super::AppState,
    body: &Document,
) -> Result<Document, ApiError> {
    let collection = body
        .get_str("createIndexes")
        .map_err(|_| bad_value("createIndexes must name a collection"))?
        .to_string();
    let database = database_name(body);
    let indexes = body
        .get_array("indexes")
        .map_err(|_| bad_value("createIndexes requires an indexes array"))?;
    let mut specs = Vec::with_capacity(indexes.len());
    for item in indexes {
        let Bson::Document(spec) = item else {
            return Err(bad_value("index specification must be a document"));
        };
        let key = spec
            .get_document("key")
            .map_err(|_| bad_value("index specification requires a key document"))?;
        if key.is_empty() {
            return Err(bad_value("index key document cannot be empty"));
        }
        let mut fields = Vec::with_capacity(key.len());
        let mut default_parts = Vec::with_capacity(key.len());
        let mut geo = false;
        for (field, direction) in key {
            let (direction, direction_name) = match direction {
                Bson::Int32(value) => (i64::from(*value), value.to_string()),
                Bson::Int64(value) => (*value, value.to_string()),
                Bson::Double(value) if value.fract() == 0.0 => (*value as i64, value.to_string()),
                Bson::String(value) if value == "2dsphere" && key.len() == 1 => {
                    geo = true;
                    (1, value.clone())
                }
                _ => {
                    return Err(bad_value(format!(
                        "unsupported index direction for field `{field}`"
                    )))
                }
            };
            if direction != 1 && direction != -1 {
                return Err(bad_value("index direction must be 1 or -1"));
            }
            fields.push(field.clone());
            default_parts.push(format!("{field}_{direction_name}"));
        }
        for unsupported in [
            "partialFilterExpression",
            "collation",
            "expireAfterSeconds",
            "wildcardProjection",
            "weights",
        ] {
            if spec.contains_key(unsupported) {
                return Err(bad_value(format!(
                    "index option `{unsupported}` is not supported by NonoDB"
                )));
            }
        }
        if spec.get_bool("sparse").unwrap_or(false) {
            return Err(bad_value("sparse indexes are not supported by NonoDB"));
        }
        if spec.get_bool("hidden").unwrap_or(false) {
            return Err(bad_value("hidden indexes are not supported by NonoDB"));
        }
        if let Ok(name) = spec.get_str("name") {
            let default = default_parts.join("_");
            let logical = fields.join("+");
            if name != default && name != logical {
                return Err(bad_value(
                    "custom MongoDB index names are not supported; use the default name",
                ));
            }
        }
        specs.push(nonodb_core::IndexSpec {
            fields,
            unique: spec.get_bool("unique").unwrap_or(false),
            include: Vec::new(),
            geo,
        });
    }
    let runtime = database_for_write(state, &database).await?;
    let (_, response) = super::submit_write(
        &runtime,
        WriteReq::CreateIndexes {
            coll: collection,
            indexes: specs,
        },
        Durability::Full,
    )
    .await;
    let api = response.0;
    if let Some(error) = api.error {
        return Err(error);
    }
    Ok(bson::doc! {
        "createdCollectionAutomatically": false,
        "numIndexesBefore": 1i32,
        "numIndexesAfter": 1i32,
        "ok": 1.0,
    })
}

async fn list_indexes_command(
    state: &super::AppState,
    body: &Document,
) -> Result<Document, ApiError> {
    let collection = body
        .get_str("listIndexes")
        .map_err(|_| bad_value("listIndexes must name a collection"))?
        .to_string();
    let database = database_name(body);
    let namespace = format!("{database}.{collection}");
    let Some(runtime) = database_for_read(state, &database).await else {
        return Ok(cursor_reply(0, &namespace, Vec::new(), true));
    };
    let store = runtime.store.clone();
    let coll = collection.clone();
    let defs = tokio::task::spawn_blocking(move || {
        let rtxn = store.read_txn()?;
        let Some(c) = store.collection_at(&rtxn, &coll)? else {
            rtxn.commit()?;
            return Ok::<Option<Vec<nonodb_core::IndexDef>>, nonodb_core::Error>(None);
        };
        let defs = c.index_defs_in(&rtxn)?;
        rtxn.commit()?;
        Ok(Some(defs))
    })
    .await
    .map_err(|error| ApiError::internal(format!("listIndexes join error: {error}")))??;
    let Some(defs) = defs else {
        let mut error = mongo_error(&ApiError {
            code: "not_found".into(),
            message: format!("ns not found: {namespace}"),
        });
        error.insert("code", 26i32);
        error.insert("codeName", "NamespaceNotFound");
        return Ok(error);
    };
    let mut batch = Vec::with_capacity(defs.len() + 1);
    let mut id_key = Document::new();
    id_key.insert("_id", 1i32);
    batch.push(Bson::Document(bson::doc! {
        "v": 2i32,
        "key": Bson::Document(id_key),
        "name": "_id_",
        "ns": namespace.clone(),
    }));
    for def in defs {
        let mut key = Document::new();
        for field in def.effective_fields() {
            key.insert(field, 1i32);
        }
        let mut doc = bson::doc! {
            "v": 2i32,
            "key": Bson::Document(key),
            "name": def.name(),
            "ns": namespace.clone(),
        };
        if def.unique {
            doc.insert("unique", true);
        }
        if def.geo {
            doc.insert("2dsphere", true);
        }
        batch.push(Bson::Document(doc));
    }
    Ok(cursor_reply(0, &namespace, batch, true))
}

async fn drop_indexes_command(
    state: &super::AppState,
    body: &Document,
) -> Result<Document, ApiError> {
    let collection = body
        .get_str("dropIndexes")
        .map_err(|_| bad_value("dropIndexes must name a collection"))?
        .to_string();
    let database = database_name(body);
    let runtime = database_for_write(state, &database).await?;
    let store = runtime.store.clone();
    let coll = collection.clone();
    let defs = tokio::task::spawn_blocking(move || {
        let rtxn = store.read_txn()?;
        let Some(c) = store.collection_at(&rtxn, &coll)? else {
            rtxn.commit()?;
            return Ok::<Option<Vec<nonodb_core::IndexDef>>, nonodb_core::Error>(None);
        };
        let defs = c.index_defs_in(&rtxn)?;
        rtxn.commit()?;
        Ok(Some(defs))
    })
    .await
    .map_err(|error| ApiError::internal(format!("dropIndexes join error: {error}")))??;
    let Some(defs) = defs else {
        return Err(ApiError {
            code: "not_found".into(),
            message: format!("ns not found: {database}.{collection}"),
        });
    };
    let before = defs.len() as i32 + 1;
    let target = body
        .get("index")
        .ok_or_else(|| bad_value("dropIndexes requires an index name, key document, or `*`"))?;
    let targets: Vec<String> = match target {
        Bson::String(name) if name == "*" => defs.iter().map(|def| def.name()).collect(),
        Bson::String(name) => {
            if name == "_id_" {
                return Err(bad_value("the _id index cannot be dropped"));
            }
            let found = defs.iter().find(|def| def.name() == *name);
            match found {
                Some(def) => vec![def.name()],
                None => {
                    return Err(ApiError {
                        code: "index_not_found".into(),
                        message: format!("index not found with name [{name}]"),
                    })
                }
            }
        }
        Bson::Document(key) => {
            let mut fields = Vec::new();
            for (field, direction) in key {
                if !matches!(
                    direction,
                    Bson::Int32(1) | Bson::Int32(-1) | Bson::Int64(1) | Bson::Int64(-1)
                ) {
                    return Err(bad_value("index key direction must be 1 or -1"));
                }
                fields.push(field.clone());
            }
            let name = fields.join("+");
            if !defs.iter().any(|def| def.name() == name) {
                return Err(ApiError {
                    code: "index_not_found".into(),
                    message: "index not found".into(),
                });
            }
            vec![name]
        }
        _ => return Err(bad_value("dropIndexes index must be a name, key, or `*`")),
    };
    for name in &targets {
        let (_, response) = super::submit_write(
            &runtime,
            WriteReq::DropIndex {
                coll: collection.clone(),
                field: name.clone(),
            },
            Durability::Full,
        )
        .await;
        let api = response.0;
        if let Some(error) = api.error {
            return Err(error);
        }
    }
    Ok(bson::doc! {
        "nIndexesWas": before,
        "msg": if target.as_str() == Some("*") {
            "non-_id indexes dropped for collection"
        } else {
            "index dropped"
        },
        "ok": 1.0,
    })
}

fn sort_fields(body: &Document) -> Result<Vec<(String, i32)>, ApiError> {
    let Some(value) = body.get("sort") else {
        return Ok(Vec::new());
    };
    let Bson::Document(sort) = value else {
        return Err(bad_value("sort must be a document"));
    };
    let mut fields = Vec::with_capacity(sort.len());
    for (field, direction) in sort {
        let Some(direction) = bson_int(direction).and_then(|value| i32::try_from(value).ok())
        else {
            return Err(bad_value("sort directions must be 1 or -1"));
        };
        if direction != 1 && direction != -1 {
            return Err(bad_value("sort directions must be 1 or -1"));
        }
        fields.push((field.clone(), direction));
    }
    Ok(fields)
}

async fn resolve_internal_id(
    runtime: &super::AppState,
    collection: &str,
    key: &str,
) -> Option<Value> {
    let store = runtime.store.clone();
    let collection = collection.to_string();
    let key = key.to_string();
    tokio::task::spawn_blocking(move || {
        store
            .collection(&collection)
            .ok()
            .and_then(|c| c.find_by_key(&key).ok().flatten())
            .and_then(|doc| doc.get("_id").cloned())
    })
    .await
    .ok()
    .flatten()
}

async fn find_command(
    state: &super::AppState,
    connection_id: u64,
    body: &Document,
) -> Result<Document, ApiError> {
    let collection = body
        .get_str("find")
        .map_err(|_| bad_value("find must name a collection"))?
        .to_string();
    let database = database_name(body);
    let filter = document_field(body, "filter")?.unwrap_or_else(|| json!({}));
    let projection = document_field(body, "projection")?;
    let hint = if let Some(value) = body.get("hint") {
        Some(bson_map::bson_to_value(value).map_err(bad_value)?)
    } else {
        None
    };
    let ascending = sort_direction(body)?;
    let skip = non_negative(body, "skip")?.unwrap_or(0);
    let raw_limit = int_field(body, "limit").unwrap_or(0);
    let single_batch = body.get_bool("singleBatch").unwrap_or(false) || raw_limit < 0;
    let mut limit = raw_limit.unsigned_abs() as usize;
    if limit == 0 {
        limit = usize::MAX;
    }
    let batch_size = non_negative(body, "batchSize")?.unwrap_or(cursor::DEFAULT_BATCH_SIZE);
    let batch_size = cursor::normalize_batch(Some(batch_size));
    if single_batch {
        limit = limit.min(batch_size);
    }
    let limit = (limit != usize::MAX).then_some(limit);
    let comment = body.get_str("comment").ok().map(str::to_string);
    let max_time_ms = int_field(body, "maxTimeMS")
        .filter(|value| *value > 0)
        .map(|value| value as u64);
    let namespace = format!("{database}.{collection}");
    let owner = format!("mongo:{connection_id}");
    let mut cursor_state = cursor::State::new(
        owner,
        database.clone(),
        cursor::Kind::Find {
            coll: collection,
            filter,
            projection,
            ascending,
            skip,
            comment,
            max_time_ms,
            hint,
        },
        limit,
        Some(batch_size),
    );
    let Some(runtime) = database_for_read(state, &database).await else {
        return Ok(cursor_reply(0, &namespace, Vec::new(), true));
    };
    if batch_size == 0 {
        let id = state
            .cursors
            .open(cursor_state)
            .map_err(|message| ApiError {
                code: "cursor".into(),
                message,
            })?;
        return Ok(cursor_reply(id as i64, &namespace, Vec::new(), true));
    }
    let store = runtime.store.clone();
    let page = tokio::task::spawn_blocking(move || {
        super::cursor_page(&store, None, &mut cursor_state, None)
            .map(|(docs, has_more)| (docs, has_more, cursor_state))
    })
    .await
    .map_err(|error| ApiError::internal(format!("find join error: {error}")))??;
    let (docs, has_more, cursor_state) = page;
    let cursor_id = if has_more && !single_batch {
        state
            .cursors
            .open(cursor_state)
            .map_err(|message| ApiError {
                code: "cursor".into(),
                message,
            })? as i64
    } else {
        0
    };
    let docs = values_to_bson(docs)?;
    Ok(cursor_reply(cursor_id, &namespace, docs, true))
}

async fn get_more_command(
    state: &super::AppState,
    connection_id: u64,
    body: &Document,
) -> Result<Document, ApiError> {
    let cursor_id =
        int_field(body, "getMore").ok_or_else(|| bad_value("getMore requires an id"))?;
    if cursor_id <= 0 {
        return Err(cursor_not_found());
    }
    let owner = format!("mongo:{connection_id}");
    let mut cursor_state = state
        .cursors
        .begin(cursor_id as u64, &owner)
        .map_err(|_| cursor_not_found())?;
    let database = cursor_state.database.clone();
    let collection = cursor_state.kind.coll().to_string();
    let namespace = format!("{database}.{collection}");
    let Some(runtime) = database_for_read(state, &database).await else {
        state.cursors.finish(cursor_id as u64, cursor_state, false);
        return Err(cursor_not_found());
    };
    let batch_size = non_negative(body, "batchSize")?;
    let store = runtime.store.clone();
    let registry = state.cursors.clone();
    let page = tokio::task::spawn_blocking(move || {
        let result = super::cursor_page(&store, None, &mut cursor_state, batch_size);
        let alive = result
            .as_ref()
            .map(|(_, has_more)| *has_more)
            .unwrap_or(true);
        registry.finish(cursor_id as u64, cursor_state, alive);
        result
    })
    .await
    .map_err(|error| ApiError::internal(format!("getMore join error: {error}")))??;
    let (docs, has_more) = page;
    let docs = values_to_bson(docs)?;
    Ok(cursor_reply(
        if has_more { cursor_id } else { 0 },
        &namespace,
        docs,
        false,
    ))
}

fn kill_cursors_command(
    state: &super::AppState,
    connection_id: u64,
    body: &Document,
) -> Result<Document, ApiError> {
    let ids = body
        .get_array("cursors")
        .map_err(|_| bad_value("killCursors requires a cursors array"))?;
    let owner = format!("mongo:{connection_id}");
    let mut killed = Vec::new();
    let mut not_found = Vec::new();
    for id in ids {
        let Some(id) = bson_int(id) else {
            continue;
        };
        if id > 0 && state.cursors.close(id as u64, &owner) {
            killed.push(Bson::Int64(id));
        } else {
            not_found.push(Bson::Int64(id));
        }
    }
    Ok(bson::doc! {
        "cursorsKilled": Bson::Array(killed),
        "cursorsNotFound": Bson::Array(not_found),
        "cursorsAlive": Bson::Array(Vec::new()),
        "cursorsUnknown": Bson::Array(Vec::new()),
        "ok": 1.0,
    })
}

fn cursor_reply(id: i64, namespace: &str, batch: Vec<Bson>, first: bool) -> Document {
    let mut cursor = Document::new();
    cursor.insert("id", Bson::Int64(id));
    cursor.insert("ns", Bson::String(namespace.to_string()));
    cursor.insert(
        if first { "firstBatch" } else { "nextBatch" },
        Bson::Array(batch),
    );
    bson::doc! {"cursor": Bson::Document(cursor), "ok": 1.0}
}

fn document_field(body: &Document, field: &str) -> Result<Option<Value>, ApiError> {
    match body.get(field) {
        None | Some(Bson::Null) => Ok(None),
        Some(Bson::Document(document)) => bson_map::document_to_value(document)
            .map(Some)
            .map_err(bad_value),
        Some(_) => Err(bad_value(format!("{field} must be a document"))),
    }
}

fn int_field(body: &Document, field: &str) -> Option<i64> {
    body.get(field).and_then(bson_int)
}

fn bson_int(value: &Bson) -> Option<i64> {
    match value {
        Bson::Int32(value) => Some(i64::from(*value)),
        Bson::Int64(value) => Some(*value),
        Bson::Double(value) if value.fract() == 0.0 => Some(*value as i64),
        _ => None,
    }
}

fn non_negative(body: &Document, field: &str) -> Result<Option<usize>, ApiError> {
    match int_field(body, field) {
        None => Ok(None),
        Some(value) if value >= 0 => Ok(Some(value as usize)),
        Some(_) => Err(bad_value(format!("{field} must be non-negative"))),
    }
}

fn sort_direction(body: &Document) -> Result<bool, ApiError> {
    let Some(value) = body.get("sort") else {
        return Ok(true);
    };
    let Bson::Document(sort) = value else {
        return Err(bad_value("sort must be a document"));
    };
    if sort.is_empty() {
        return Ok(true);
    }
    if sort.len() != 1 {
        return Err(bad_value(
            "Mongo wire find currently supports sorting only by _id",
        ));
    }
    match sort.get("_id").and_then(bson_int) {
        Some(1) => Ok(true),
        Some(-1) => Ok(false),
        _ => Err(bad_value(
            "Mongo wire find currently supports sorting only by _id",
        )),
    }
}

fn values_to_bson(values: Vec<Value>) -> Result<Vec<Bson>, ApiError> {
    values
        .into_iter()
        .map(|value| bson_map::value_to_bson(&value).map_err(ApiError::internal))
        .collect()
}

fn bad_value(message: impl Into<String>) -> ApiError {
    ApiError {
        code: "invalid_query".into(),
        message: message.into(),
    }
}

fn cursor_not_found() -> ApiError {
    ApiError {
        code: "cursor_not_found".into(),
        message: "CursorNotFound: cursor id not found".into(),
    }
}

fn mongo_code(error: &ApiError) -> (i32, &'static str) {
    match error.code.as_str() {
        "duplicate_key" => (11000, "DuplicateKey"),
        "cursor_not_found" => (43, "CursorNotFound"),
        "index_not_found" => (27, "IndexNotFound"),
        "not_found" => (26, "NamespaceNotFound"),
        "timeout" => (50, "MaxTimeMSExpired"),
        "document_too_large" => (10334, "BSONObjectTooLarge"),
        "invalid_query" | "invalid_id" | "invalid_json" | "invalid_collection" => (2, "BadValue"),
        _ => (1, "InternalError"),
    }
}

fn mongo_error(error: &ApiError) -> Document {
    let (code, code_name) = mongo_code(error);
    bson::doc! {
        "ok": 0.0,
        "errmsg": error.message.clone(),
        "code": code,
        "codeName": code_name,
    }
}

fn write_error(index: usize, error: &ApiError) -> Bson {
    let (code, code_name) = mongo_code(error);
    Bson::Document(bson::doc! {
        "index": index as i32,
        "code": code,
        "codeName": code_name,
        "errmsg": error.message.clone(),
    })
}

async fn read_message<S>(stream: &mut S) -> io::Result<Option<Message>>
where
    S: AsyncRead + Unpin,
{
    let mut header = [0u8; 16];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let length = i32::from_le_bytes(header[0..4].try_into().expect("four bytes")) as usize;
    let request_id = i32::from_le_bytes(header[4..8].try_into().expect("four bytes"));
    let op_code = i32::from_le_bytes(header[12..16].try_into().expect("four bytes"));
    if !(16..=MAX_MESSAGE_BYTES).contains(&length) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Mongo wire message length is out of bounds",
        ));
    }
    let mut payload = vec![0u8; length - 16];
    stream.read_exact(&mut payload).await?;
    let body = match op_code {
        OP_MSG => parse_op_msg(&payload)?,
        OP_QUERY => parse_op_query(&payload)?,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported Mongo wire opcode {op_code}"),
            ));
        }
    };
    Ok(Some(Message {
        request_id,
        op_code,
        body,
    }))
}

fn parse_op_msg(payload: &[u8]) -> io::Result<Document> {
    if payload.len() < 5 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "OP_MSG payload is too short",
        ));
    }
    let flags = u32::from_le_bytes(payload[0..4].try_into().expect("four bytes"));
    let checksum = flags & 1 != 0;
    let mut end = payload.len();
    if checksum {
        if end < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "OP_MSG checksum is truncated",
            ));
        }
        end -= 4;
    }
    let mut position = 4;
    let mut body = None;
    let mut sequences = Vec::new();
    while position < end {
        let kind = payload[position];
        position += 1;
        match kind {
            0 => {
                let (doc, next) = read_bson_at(payload, position, end)?;
                body = Some(doc);
                position = next;
            }
            1 => {
                if position + 4 > end {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "OP_MSG document sequence is truncated",
                    ));
                }
                let size = i32::from_le_bytes(
                    payload[position..position + 4]
                        .try_into()
                        .expect("four bytes"),
                ) as usize;
                if size < 5 || position + size > end {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "OP_MSG document sequence size is invalid",
                    ));
                }
                let sequence_end = position + size;
                position += 4;
                let identifier_start = position;
                while position < sequence_end && payload[position] != 0 {
                    position += 1;
                }
                if position >= sequence_end {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "OP_MSG document sequence identifier is invalid",
                    ));
                }
                let identifier = std::str::from_utf8(&payload[identifier_start..position])
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "OP_MSG document sequence identifier is not UTF-8",
                        )
                    })?
                    .to_string();
                position += 1;
                let mut documents = Vec::new();
                while position < sequence_end {
                    let (doc, next) = read_bson_at(payload, position, sequence_end)?;
                    documents.push(Bson::Document(doc));
                    position = next;
                }
                sequences.push((identifier, documents));
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unsupported OP_MSG section kind",
                ));
            }
        }
    }
    let mut body =
        body.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "OP_MSG has no body"))?;
    for (identifier, documents) in sequences {
        body.insert(identifier, Bson::Array(documents));
    }
    Ok(body)
}

fn parse_op_query(payload: &[u8]) -> io::Result<Document> {
    if payload.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "OP_QUERY payload is too short",
        ));
    }
    let mut position = 4;
    while position < payload.len() && payload[position] != 0 {
        position += 1;
    }
    if position >= payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "OP_QUERY namespace is truncated",
        ));
    }
    position += 1;
    position = position
        .checked_add(8)
        .filter(|position| *position <= payload.len())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "OP_QUERY skip/return fields are truncated",
            )
        })?;
    let (doc, _) = read_bson_at(payload, position, payload.len())?;
    Ok(doc)
}

fn read_bson_at(payload: &[u8], position: usize, end: usize) -> io::Result<(Document, usize)> {
    if position + 4 > end {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "BSON document length is truncated",
        ));
    }
    let length = i32::from_le_bytes(
        payload[position..position + 4]
            .try_into()
            .expect("four bytes"),
    );
    if length < 5 || position + length as usize > end {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "BSON document length is invalid",
        ));
    }
    let next = position + length as usize;
    let doc = bson::deserialize_from_slice(&payload[position..next]).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid BSON document: {error}"),
        )
    })?;
    Ok((doc, next))
}

fn command_reply(body: &Document) -> Document {
    let command = body.keys().next().map(String::as_str).unwrap_or("");
    match command {
        "hello" | "isMaster" | "ismaster" => hello_reply(),
        "ping" => doc_ok(),
        "buildInfo" | "buildinfo" => build_info(),
        "getParameter" => get_parameter(),
        "connectionStatus" => connection_status(),
        _ => command_not_found(command),
    }
}

fn doc_ok() -> Document {
    let mut doc = Document::new();
    doc.insert("ok", 1.0);
    doc
}

fn hello_reply() -> Document {
    let mut doc = doc_ok();
    doc.insert("isWritablePrimary", true);
    doc.insert("ismaster", true);
    doc.insert("helloOk", true);
    doc.insert("maxBsonObjectSize", MAX_BSON_BYTES);
    doc.insert("maxMessageSizeBytes", MAX_MESSAGE_BYTES as i32);
    doc.insert("maxWriteBatchSize", MAX_WRITE_BATCH_SIZE);
    doc.insert("localTime", bson::DateTime::now());
    doc.insert("minWireVersion", 0);
    doc.insert("maxWireVersion", MAX_WIRE_VERSION);
    doc.insert("readOnly", false);
    doc.insert("compression", Bson::Array(Vec::new()));
    doc
}

fn build_info() -> Document {
    let mut doc = doc_ok();
    doc.insert("version", env!("CARGO_PKG_VERSION"));
    doc.insert(
        "versionArray",
        Bson::Array(
            env!("CARGO_PKG_VERSION")
                .split('.')
                .map(|part| Bson::Int32(part.parse::<i32>().unwrap_or_default()))
                .collect(),
        ),
    );
    doc.insert("gitVersion", "");
    doc.insert("modules", Bson::Array(Vec::new()));
    doc.insert("allocator", "system");
    doc.insert("javascriptEngine", "none");
    doc.insert("maxBsonObjectSize", MAX_BSON_BYTES);
    doc
}

fn get_parameter() -> Document {
    let mut doc = doc_ok();
    let mut compatibility = Document::new();
    compatibility.insert("version", "4.4");
    doc.insert("featureCompatibilityVersion", compatibility);
    doc
}

fn connection_status() -> Document {
    let mut doc = doc_ok();
    let mut auth_info = Document::new();
    auth_info.insert("authenticatedUsers", Bson::Array(Vec::new()));
    auth_info.insert("authenticatedUserRoles", Bson::Array(Vec::new()));
    doc.insert("authInfo", auth_info);
    doc
}

fn command_not_found(command: &str) -> Document {
    let mut doc = Document::new();
    doc.insert("ok", 0.0);
    doc.insert("errmsg", format!("no such command: '{command}'"));
    doc.insert("code", 59);
    doc.insert("codeName", "CommandNotFound");
    doc
}

fn encode_op_msg(request_id: i32, response_to: i32, doc: &Document) -> io::Result<Vec<u8>> {
    let body = bson::serialize_to_vec(doc).map_err(bson_error)?;
    let length = 16 + 4 + 1 + body.len();
    let mut out = Vec::with_capacity(length);
    out.extend_from_slice(&(length as i32).to_le_bytes());
    out.extend_from_slice(&request_id.to_le_bytes());
    out.extend_from_slice(&response_to.to_le_bytes());
    out.extend_from_slice(&OP_MSG.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.push(0);
    out.extend_from_slice(&body);
    Ok(out)
}

fn encode_op_reply(request_id: i32, response_to: i32, doc: &Document) -> io::Result<Vec<u8>> {
    let body = bson::serialize_to_vec(doc).map_err(bson_error)?;
    let length = 16 + 4 + 8 + 4 + 4 + body.len();
    let mut out = Vec::with_capacity(length);
    out.extend_from_slice(&(length as i32).to_le_bytes());
    out.extend_from_slice(&request_id.to_le_bytes());
    out.extend_from_slice(&response_to.to_le_bytes());
    out.extend_from_slice(&OP_REPLY.to_le_bytes());
    out.extend_from_slice(&0i32.to_le_bytes());
    out.extend_from_slice(&0i64.to_le_bytes());
    out.extend_from_slice(&0i32.to_le_bytes());
    out.extend_from_slice(&1i32.to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

fn bson_error(error: bson::error::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op_msg_with_body(doc: &Document) -> Vec<u8> {
        let body = bson::serialize_to_vec(doc).unwrap();
        let mut payload = Vec::new();
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.push(0);
        payload.extend_from_slice(&body);
        payload
    }

    #[test]
    fn parses_op_msg_body() {
        let mut request = Document::new();
        request.insert("ping", 1);
        let payload = op_msg_with_body(&request);
        let parsed = parse_op_msg(&payload).unwrap();
        assert_eq!(parsed.get_i32("ping").unwrap(), 1);
    }

    #[test]
    fn parses_op_msg_document_sequences() {
        let body = bson::doc! { "insert": "items", "$db": "test" };
        let body_bytes = bson::serialize_to_vec(&body).unwrap();
        let documents = [bson::doc! { "name": "a" }, bson::doc! { "name": "b" }];
        let mut sequence = Vec::new();
        sequence.extend_from_slice(b"documents\0");
        for document in &documents {
            sequence.extend_from_slice(&bson::serialize_to_vec(document).unwrap());
        }
        let mut payload = Vec::new();
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.push(0);
        payload.extend_from_slice(&body_bytes);
        payload.push(1);
        payload.extend_from_slice(&((sequence.len() + 4) as i32).to_le_bytes());
        payload.extend_from_slice(&sequence);

        let parsed = parse_op_msg(&payload).unwrap();
        let docs = parsed.get_array("documents").unwrap();
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].as_document().unwrap().get_str("name").unwrap(), "a");
    }

    #[test]
    fn find_sort_support_is_explicit() {
        assert!(sort_direction(&bson::doc! { "sort": { "_id": 1 } }).unwrap());
        assert!(!sort_direction(&bson::doc! { "sort": { "_id": -1 } }).unwrap());
        assert!(sort_direction(&bson::doc! { "sort": { "name": 1 } }).is_err());
    }

    #[test]
    fn hello_reply_advertises_wire_contract() {
        let reply = command_reply(&bson::doc! { "hello": 1 });
        assert_eq!(reply.get_f64("ok").unwrap(), 1.0);
        assert_eq!(reply.get_i32("maxWireVersion").unwrap(), MAX_WIRE_VERSION);
        assert_eq!(reply.get_i32("maxBsonObjectSize").unwrap(), MAX_BSON_BYTES);
        assert!(reply.get_bool("isWritablePrimary").unwrap());
    }

    #[test]
    fn ping_and_unknown_commands_have_mongodb_errors() {
        assert_eq!(
            command_reply(&bson::doc! { "ping": 1 })
                .get_f64("ok")
                .unwrap(),
            1.0
        );
        let reply = command_reply(&bson::doc! { "notACommand": 1 });
        assert_eq!(reply.get_f64("ok").unwrap(), 0.0);
        assert_eq!(reply.get_i32("code").unwrap(), 59);
        assert_eq!(reply.get_str("codeName").unwrap(), "CommandNotFound");
    }

    #[test]
    fn encoded_replies_have_valid_opcodes_and_lengths() {
        let reply = doc_ok();
        let op_msg = encode_op_msg(7, 3, &reply).unwrap();
        assert_eq!(
            i32::from_le_bytes(op_msg[0..4].try_into().unwrap()) as usize,
            op_msg.len()
        );
        assert_eq!(
            i32::from_le_bytes(op_msg[12..16].try_into().unwrap()),
            OP_MSG
        );

        let op_reply = encode_op_reply(8, 4, &reply).unwrap();
        assert_eq!(
            i32::from_le_bytes(op_reply[0..4].try_into().unwrap()) as usize,
            op_reply.len()
        );
        assert_eq!(
            i32::from_le_bytes(op_reply[12..16].try_into().unwrap()),
            OP_REPLY
        );
    }
}
