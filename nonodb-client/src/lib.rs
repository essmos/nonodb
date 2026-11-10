//! Thin Rust client for nonodb.
//!
//! The client speaks HTTP/JSON to `nonodb-daemon`. On Unix it can connect to the
//! daemon's Unix socket; with a base URL it uses TCP (optionally TLS). The client
//! keeps no sessions and performs no query logic itself.
//!
//! ```no_run
//! use nonodb_client::Client;
//! use serde_json::json;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let db = Client::unix("/tmp/nonodb-run/nonodb.sock")?;
//! let users = db.collection("users");
//! let id = users.insert(&json!({"name": "Ada", "age": 36}))?;
//! let adults = users.find(&nonodb_client::FindQuery::new(json!({"age": {"$gte": 18}})))?;
//! println!("inserted {id}, found {} documents", adults.len());
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Error returned by the client or by the daemon.
#[derive(Debug)]
pub enum Error {
    /// The daemon returned `{"ok": false, "error": {"code", "message"}}`.
    Api { code: String, message: String },
    /// Transport or HTTP-layer error.
    Http(reqwest::Error),
    /// JSON serialization/deserialization error.
    Json(serde_json::Error),
    /// The daemon returned a response that was not understood.
    InvalidResponse(String),
    /// Invalid client configuration.
    Config(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Api { code, message } => write!(f, "{code}: {message}"),
            Error::Http(err) => write!(f, "{err}"),
            Error::Json(err) => write!(f, "{err}"),
            Error::InvalidResponse(message) => write!(f, "invalid response: {message}"),
            Error::Config(message) => write!(f, "configuration: {message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Http(err) => Some(err),
            Error::Json(err) => Some(err),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for Error {
    fn from(value: reqwest::Error) -> Self {
        Error::Http(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Error::Json(value)
    }
}

/// Result type used by the client.
pub type Result<T> = std::result::Result<T, Error>;

/// Builder for [`Client`].
#[derive(Clone, Debug, Default)]
pub struct ClientBuilder {
    socket_path: Option<PathBuf>,
    base_url: Option<String>,
    token: Option<String>,
    timeout: Option<Duration>,
    verify: Option<bool>,
}

impl ClientBuilder {
    /// Connect through a Unix domain socket (Unix only).
    pub fn socket_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.socket_path = Some(path.into());
        self
    }

    /// Connect through HTTP over TCP/TLS, for example `http://127.0.0.1:7878`.
    ///
    /// When `base_url` is set, it takes precedence over `socket_path`.
    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = Some(url.into());
        self
    }

    /// Bearer token. Defaults to `NONODB_TOKEN`.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Request timeout. Defaults to `NONODB_TIMEOUT` seconds, then 10 seconds.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Verify TLS certificates. Defaults to `NONODB_TLS_VERIFY` (enabled).
    pub fn verify(mut self, verify: bool) -> Self {
        self.verify = Some(verify);
        self
    }

    /// Build the client.
    pub fn build(self) -> Result<Client> {
        let base_url_given = self.base_url.is_some();
        let env_base_url = (!base_url_given)
            .then(|| std::env::var("NONODB_URL").ok())
            .flatten();
        let base_url = self
            .base_url
            .or(env_base_url)
            .unwrap_or_else(default_base_url)
            .trim_end_matches('/')
            .to_string();

        #[cfg(unix)]
        let socket_path = self
            .socket_path
            .or_else(|| std::env::var_os("NONODB_SOCK").map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from("/run/nonodb.sock"));

        let timeout = self.timeout.unwrap_or_else(|| {
            std::env::var("NONODB_TIMEOUT")
                .ok()
                .and_then(|value| value.parse::<f64>().ok())
                .filter(|seconds| *seconds > 0.0)
                .map(Duration::from_secs_f64)
                .unwrap_or(Duration::from_secs(10))
        });

        let verify = self.verify.unwrap_or_else(|| {
            !matches!(
                std::env::var("NONODB_TLS_VERIFY")
                    .unwrap_or_else(|_| "1".into())
                    .to_ascii_lowercase()
                    .as_str(),
                "0" | "false" | "no"
            )
        });

        let token = self
            .token
            .or_else(|| std::env::var("NONODB_TOKEN").ok())
            .filter(|value| !value.is_empty());

        let builder = reqwest::blocking::Client::builder()
            .timeout(timeout)
            .danger_accept_invalid_certs(!verify);

        #[cfg(unix)]
        let builder = if !base_url_given && std::env::var_os("NONODB_URL").is_none() {
            builder.unix_socket(socket_path)
        } else {
            builder
        };

        let http = builder.build()?;
        Ok(Client {
            http,
            base_url,
            token,
        })
    }
}

/// A thin, synchronous nonodb client.
#[derive(Clone)]
pub struct Client {
    http: reqwest::blocking::Client,
    base_url: String,
    token: Option<String>,
}

impl Client {
    /// Start configuring a client.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Connect through a Unix socket (Unix only).
    pub fn unix(path: impl Into<PathBuf>) -> Result<Self> {
        Self::builder().socket_path(path).build()
    }

    /// Connect through HTTP over TCP/TLS.
    pub fn tcp(base_url: impl Into<String>) -> Result<Self> {
        Self::builder().base_url(base_url).build()
    }

    /// Read connection settings from `NONODB_SOCK`, `NONODB_URL`, and
    /// `NONODB_TOKEN`.
    pub fn from_env() -> Result<Self> {
        Self::builder().build()
    }

    /// Check `GET /healthz`.
    pub fn health(&self) -> bool {
        self.http
            .get(format!("{}/healthz", self.base_url))
            .send()
            .is_ok_and(|response| response.status().is_success())
    }

    /// Execute an arbitrary `/v1/exec` payload and return its `result`.
    pub fn exec(&self, payload: Value) -> Result<Value> {
        let attempts = if is_read_op(&payload) { 3 } else { 1 };
        let mut last_error = None;
        for attempt in 0..attempts {
            match self.exec_once(&payload) {
                Ok(result) => return Ok(result),
                Err(Error::Http(err)) if attempt + 1 < attempts => {
                    last_error = Some(Error::Http(err));
                    std::thread::sleep(Duration::from_millis(10 * (attempt as u64 + 1)));
                }
                Err(err) => return Err(err),
            }
        }
        Err(last_error
            .unwrap_or_else(|| Error::InvalidResponse("request failed without an error".into())))
    }

    fn exec_once(&self, payload: &Value) -> Result<Value> {
        let mut request = self
            .http
            .post(format!("{}/v1/exec", self.base_url))
            .json(payload);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request.send()?;
        let status = response.status();
        let bytes = response.bytes()?;
        let body: Value = serde_json::from_slice(&bytes).map_err(|_| {
            Error::InvalidResponse(format!(
                "HTTP {status}: {}",
                String::from_utf8_lossy(&bytes[..bytes.len().min(200)])
            ))
        })?;
        if body.get("ok").and_then(Value::as_bool) != Some(true) {
            let error = body.get("error").cloned().unwrap_or(Value::Null);
            return Err(Error::Api {
                code: error
                    .get("code")
                    .and_then(Value::as_str)
                    .unwrap_or("error")
                    .to_string(),
                message: error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            });
        }
        Ok(body.get("result").cloned().unwrap_or(Value::Null))
    }

    /// List collection names.
    pub fn collections(&self) -> Result<Vec<String>> {
        let result = self.exec(json!({"op": "list_collections"}))?;
        result
            .get("collections")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(Error::from)
            .map(|value| value.unwrap_or_default())
    }

    /// Execute read-only operations in one LMDB transaction.
    pub fn batch(&self, ops: &[Value]) -> Result<Vec<Value>> {
        let result = self.exec(json!({"op": "batch", "ops": ops}))?;
        value_array(result.get("results").cloned().unwrap_or(Value::Null))
    }

    /// Execute writes atomically: all operations commit or none do.
    pub fn write_batch(&self, ops: &[Value]) -> Result<Vec<Value>> {
        let result = self.exec(json!({"op": "write_batch", "ops": ops}))?;
        value_array(result.get("results").cloned().unwrap_or(Value::Null))
    }

    /// Return a handle for one collection.
    pub fn collection(&self, name: impl Into<String>) -> Collection<'_> {
        Collection {
            client: self,
            name: name.into(),
        }
    }
}

fn default_base_url() -> String {
    #[cfg(unix)]
    {
        "http://localhost".into()
    }
    #[cfg(not(unix))]
    {
        "http://127.0.0.1:7878".into()
    }
}

fn value_array(value: Value) -> Result<Vec<Value>> {
    match value {
        Value::Array(values) => Ok(values),
        Value::Null => Ok(Vec::new()),
        other => Err(Error::InvalidResponse(format!(
            "expected an array, got {other}"
        ))),
    }
}

fn is_read_op(payload: &Value) -> bool {
    matches!(
        payload.get("op").and_then(Value::as_str),
        Some(
            "find"
                | "find_one"
                | "count"
                | "list_indexes"
                | "list_collections"
                | "list_text_indexes"
                | "list_fuzzy_indexes"
                | "search"
                | "fuzzy"
                | "history"
                | "get_version"
                | "batch"
        )
    )
}

/// A handle for one collection.
pub struct Collection<'a> {
    client: &'a Client,
    name: String,
}

impl Collection<'_> {
    /// Collection name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Insert a document and return its `_id`.
    pub fn insert<T: Serialize>(&self, doc: &T) -> Result<String> {
        let result = self.client.exec(json!({
            "op": "insert",
            "coll": self.name,
            "doc": doc,
        }))?;
        result
            .get("inserted_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| Error::InvalidResponse("missing inserted_id".into()))
    }

    /// Insert many documents and return their `_id` values.
    pub fn insert_many<I, T>(&self, docs: I) -> Result<Vec<String>>
    where
        I: IntoIterator<Item = T>,
        T: Serialize,
    {
        let docs = docs
            .into_iter()
            .map(|doc| serde_json::to_value(doc))
            .collect::<serde_json::Result<Vec<_>>>()?;
        let result = self.client.exec(json!({
            "op": "insert_many",
            "coll": self.name,
            "docs": docs,
        }))?;
        result
            .get("inserted_ids")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(Error::from)
            .map(|value| value.unwrap_or_default())
    }

    /// Update matching documents and return the number of modified documents.
    pub fn update(&self, filter: &Value, update: &Value, multi: bool, upsert: bool) -> Result<u64> {
        Ok(self.update_result(filter, update, multi, upsert)?.modified)
    }

    /// Update matching documents and return counts plus an optional upsert id.
    pub fn update_result(
        &self,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
    ) -> Result<UpdateResult> {
        let result = self.client.exec(json!({
            "op": "update",
            "coll": self.name,
            "filter": filter,
            "update": update,
            "multi": multi,
            "upsert": upsert,
        }))?;
        serde_json::from_value(result).map_err(Error::from)
    }

    /// Delete matching documents and return the number deleted.
    pub fn delete(&self, filter: &Value, multi: bool) -> Result<u64> {
        let result = self.client.exec(json!({
            "op": "delete",
            "coll": self.name,
            "filter": filter,
            "multi": multi,
        }))?;
        result
            .get("deleted")
            .and_then(Value::as_u64)
            .ok_or_else(|| Error::InvalidResponse("missing deleted".into()))
    }

    /// Atomically write several operations in this collection.
    ///
    /// Each operation may contain its own `coll`; otherwise this collection's
    /// name is added.
    pub fn write_batch(&self, ops: &[Value]) -> Result<Vec<Value>> {
        let ops = ops
            .iter()
            .map(|op| {
                let mut op = op.clone();
                if let Some(object) = op.as_object_mut() {
                    object
                        .entry("coll")
                        .or_insert_with(|| Value::String(self.name.clone()));
                }
                op
            })
            .collect::<Vec<_>>();
        self.client.write_batch(&ops)
    }

    /// Create a single-field or compound index.
    pub fn create_index(&self, fields: &[&str], unique: bool, include: &[&str]) -> Result<bool> {
        self.client.exec(json!({
            "op": "create_index",
            "coll": self.name,
            "fields": fields,
            "unique": unique,
            "include": include,
        }))?;
        Ok(true)
    }

    /// Convenience wrapper for a single-field index.
    pub fn create_field_index(&self, field: &str, unique: bool) -> Result<bool> {
        self.create_index(&[field], unique, &[])
    }

    /// Drop an index by field name.
    pub fn drop_index(&self, field: &str) -> Result<bool> {
        let result = self.client.exec(json!({
            "op": "drop_index",
            "coll": self.name,
            "field": field,
        }))?;
        result
            .get("dropped")
            .and_then(Value::as_bool)
            .ok_or_else(|| Error::InvalidResponse("missing dropped".into()))
    }

    /// List secondary index definitions.
    pub fn list_indexes(&self) -> Result<Vec<Value>> {
        let result = self
            .client
            .exec(json!({"op": "list_indexes", "coll": self.name}))?;
        value_array(result.get("indexes").cloned().unwrap_or(Value::Null))
    }

    /// Fetch many documents by `_id` in one read batch.
    pub fn get_many<I, S>(&self, ids: I) -> Result<Vec<Value>>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let ops = ids
            .into_iter()
            .map(|id| {
                json!({
                    "op": "find_one",
                    "coll": self.name,
                    "filter": {"_id": id.into()},
                })
            })
            .collect::<Vec<_>>();
        let results = self.client.batch(&ops)?;
        Ok(results
            .into_iter()
            .filter_map(|result| {
                if result.get("ok").and_then(Value::as_bool) == Some(true) {
                    result
                        .get("result")
                        .cloned()
                        .filter(|value| !value.is_null())
                } else {
                    None
                }
            })
            .collect())
    }

    /// List document revisions, newest first.
    ///
    /// History is enabled by the daemon with `NONODB_HISTORY=1`. Revisions use
    /// global monotonic `rev` values.
    pub fn history(&self, id: &str, limit: Option<u64>, include_docs: bool) -> Result<Vec<Value>> {
        let result = self.client.exec(json!({
            "op": "history",
            "coll": self.name,
            "id": id,
            "limit": limit,
            "include_docs": include_docs,
        }))?;
        value_array(result.get("revisions").cloned().unwrap_or(Value::Null))
    }

    /// Read one exact document revision.
    pub fn get_version(&self, id: &str, version: u64) -> Result<Option<Value>> {
        let result = self.client.exec(json!({
            "op": "get_version",
            "coll": self.name,
            "id": id,
            "version": version,
        }))?;
        Ok((!result.is_null()).then_some(result))
    }

    /// Create a full-text index.
    pub fn create_text_index(&self, field: &str) -> Result<bool> {
        self.client.exec(json!({
            "op": "create_text_index",
            "coll": self.name,
            "field": field,
        }))?;
        Ok(true)
    }

    /// Drop a full-text index.
    pub fn drop_text_index(&self, field: &str) -> Result<bool> {
        let result = self.client.exec(json!({
            "op": "drop_text_index",
            "coll": self.name,
            "field": field,
        }))?;
        result
            .get("dropped")
            .and_then(Value::as_bool)
            .ok_or_else(|| Error::InvalidResponse("missing dropped".into()))
    }

    /// List full-text index fields.
    pub fn list_text_indexes(&self) -> Result<Vec<String>> {
        let result = self.client.exec(json!({
            "op": "list_text_indexes",
            "coll": self.name,
        }))?;
        result
            .get("text_indexes")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(Error::from)
            .map(|value| value.unwrap_or_default())
    }

    /// Create a fuzzy index.
    pub fn create_fuzzy_index(&self, field: &str) -> Result<bool> {
        self.client.exec(json!({
            "op": "create_fuzzy_index",
            "coll": self.name,
            "field": field,
        }))?;
        Ok(true)
    }

    /// Drop a fuzzy index.
    pub fn drop_fuzzy_index(&self, field: &str) -> Result<bool> {
        let result = self.client.exec(json!({
            "op": "drop_fuzzy_index",
            "coll": self.name,
            "field": field,
        }))?;
        result
            .get("dropped")
            .and_then(Value::as_bool)
            .ok_or_else(|| Error::InvalidResponse("missing dropped".into()))
    }

    /// List fuzzy index fields.
    pub fn list_fuzzy_indexes(&self) -> Result<Vec<String>> {
        let result = self.client.exec(json!({
            "op": "list_fuzzy_indexes",
            "coll": self.name,
        }))?;
        result
            .get("fuzzy_indexes")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(Error::from)
            .map(|value| value.unwrap_or_default())
    }

    /// BM25 full-text search. Results contain `_score`, highest first.
    pub fn search(
        &self,
        field: &str,
        query: &str,
        filter: Option<&Value>,
        skip: u64,
        limit: u64,
    ) -> Result<Vec<Value>> {
        let result = self.client.exec(json!({
            "op": "search",
            "coll": self.name,
            "field": field,
            "query": query,
            "filter": filter.cloned().unwrap_or_else(|| json!({})),
            "skip": skip,
            "limit": limit,
        }))?;
        value_array(result.get("results").cloned().unwrap_or(Value::Null))
    }

    /// Fuzzy search. Results contain `_distance`, lowest first.
    pub fn fuzzy(
        &self,
        field: &str,
        query: &str,
        max_distance: u32,
        filter: Option<&Value>,
        limit: u64,
    ) -> Result<Vec<Value>> {
        let result = self.client.exec(json!({
            "op": "fuzzy",
            "coll": self.name,
            "field": field,
            "query": query,
            "max_distance": max_distance,
            "filter": filter.cloned().unwrap_or_else(|| json!({})),
            "limit": limit,
        }))?;
        value_array(result.get("results").cloned().unwrap_or(Value::Null))
    }

    /// Find documents using a [`FindQuery`].
    pub fn find(&self, query: &FindQuery) -> Result<Vec<Value>> {
        let mut payload = json!({
            "op": "find",
            "coll": self.name,
            "filter": query.filter,
            "skip": query.skip,
        });
        let object = payload
            .as_object_mut()
            .ok_or_else(|| Error::InvalidResponse("payload is not an object".into()))?;
        if let Some(projection) = &query.projection {
            object.insert("projection".into(), projection.clone());
        }
        if let Some(sort) = &query.sort {
            object.insert("sort".into(), json!(sort));
        }
        if let Some(limit) = query.limit {
            object.insert("limit".into(), json!(limit));
        }
        value_array(self.client.exec(payload)?)
    }

    /// Find one document, or `None`.
    pub fn find_one(&self, filter: &Value, projection: Option<&Value>) -> Result<Option<Value>> {
        let mut payload = json!({
            "op": "find_one",
            "coll": self.name,
            "filter": filter,
        });
        if let Some(projection) = projection {
            payload
                .as_object_mut()
                .expect("object payload")
                .insert("projection".into(), projection.clone());
        }
        let result = self.client.exec(payload)?;
        Ok((!result.is_null()).then_some(result))
    }

    /// Count documents matching a filter.
    pub fn count(&self, filter: Option<&Value>) -> Result<u64> {
        let result = self.client.exec(json!({
            "op": "count",
            "coll": self.name,
            "filter": filter.cloned().unwrap_or_else(|| json!({})),
        }))?;
        result
            .get("count")
            .and_then(Value::as_u64)
            .ok_or_else(|| Error::InvalidResponse("missing count".into()))
    }
}

/// Options for [`Collection::find`].
#[derive(Debug, Clone)]
pub struct FindQuery {
    pub filter: Value,
    pub projection: Option<Value>,
    pub sort: Option<Vec<(String, i32)>>,
    pub skip: u64,
    pub limit: Option<u64>,
}

impl Default for FindQuery {
    fn default() -> Self {
        Self {
            filter: json!({}),
            projection: None,
            sort: None,
            skip: 0,
            limit: None,
        }
    }
}

impl FindQuery {
    /// Query with a filter.
    pub fn new(filter: Value) -> Self {
        Self {
            filter,
            ..Self::default()
        }
    }

    /// Match all documents.
    pub fn all() -> Self {
        Self::default()
    }

    /// Add a projection.
    pub fn projection(mut self, projection: Value) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Add a sort field. Use `1` for ascending and `-1` for descending.
    pub fn sort(mut self, field: impl Into<String>, direction: i32) -> Self {
        self.sort
            .get_or_insert_with(Vec::new)
            .push((field.into(), direction));
        self
    }

    /// Skip documents.
    pub fn skip(mut self, skip: u64) -> Self {
        self.skip = skip;
        self
    }

    /// Limit the result size.
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Result of an update or upsert.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct UpdateResult {
    pub matched: u64,
    pub modified: u64,
    #[serde(default)]
    pub upserted_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_query_builds_sort_and_limit() {
        let query = FindQuery::new(json!({"age": {"$gte": 18}}))
            .sort("age", -1)
            .skip(10)
            .limit(5);
        assert_eq!(query.sort, Some(vec![("age".into(), -1)]));
        assert_eq!(query.skip, 10);
        assert_eq!(query.limit, Some(5));
    }

    #[test]
    fn read_ops_are_retried() {
        assert!(is_read_op(&json!({"op": "find"})));
        assert!(is_read_op(&json!({"op": "batch"})));
        assert!(!is_read_op(&json!({"op": "write_batch"})));
        assert!(!is_read_op(&json!({"op": "insert"})));
    }
}
