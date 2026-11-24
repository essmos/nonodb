//! Native asynchronous webhooks.
//!
//! The writer never evaluates webhook filters or aggregates. A background
//! worker reads the change outbox, maintains simple count/sum/avg state, and
//! enqueues durable deliveries. HTTP delivery happens on the same background
//! worker after the write transaction has committed.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nonodb_core::events::EventConfig;
use nonodb_core::query::path;
use nonodb_core::{events, FindQuery, Store};
use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::protocol::{ApiError, Durability, WriteReq};
use crate::writer::WriteJob;

pub const WEBHOOKS_COLLECTION: &str = "_webhooks";
pub const DELIVERIES_COLLECTION: &str = "_webhook_deliveries";
const STATE_FILE: &str = "webhooks-state.json";
const MAX_EVENTS_PER_PASS: usize = 100;
const MAX_DELIVERIES_PER_PASS: usize = 20;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WebhookThreshold {
    #[serde(default)]
    pub filter: Value,
    /// count, sum, or avg.
    #[serde(default)]
    pub aggregate: String,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub gt: Option<f64>,
    #[serde(default)]
    pub gte: Option<f64>,
    #[serde(default)]
    pub lt: Option<f64>,
    #[serde(default)]
    pub lte: Option<f64>,
    #[serde(default)]
    pub eq: Option<f64>,
}

impl WebhookThreshold {
    fn validate(&self) -> Result<(), String> {
        if !matches!(self.aggregate.as_str(), "count" | "sum" | "avg") {
            return Err("webhook threshold.aggregate must be count, sum, or avg".into());
        }
        if matches!(self.aggregate.as_str(), "sum" | "avg")
            && self.field.as_deref().unwrap_or("").trim().is_empty()
        {
            return Err("sum/avg webhook thresholds require a field".into());
        }
        if [self.gt, self.gte, self.lt, self.lte, self.eq]
            .iter()
            .filter(|value| value.is_some())
            .count()
            != 1
        {
            return Err("webhook threshold requires exactly one of gt/gte/lt/lte/eq".into());
        }
        Ok(())
    }

    fn matches(&self, value: f64) -> bool {
        self.gt.is_some_and(|v| value > v)
            || self.gte.is_some_and(|v| value >= v)
            || self.lt.is_some_and(|v| value < v)
            || self.lte.is_some_and(|v| value <= v)
            || self.eq.is_some_and(|v| (value - v).abs() < f64::EPSILON)
    }

    fn metric(&self, count: u64, sum: f64) -> f64 {
        match self.aggregate.as_str() {
            "sum" => sum,
            "avg" => {
                if count == 0 {
                    0.0
                } else {
                    sum / count as f64
                }
            }
            _ => count as f64,
        }
    }

    fn matches_doc(&self, doc: &Value) -> bool {
        matches_filter(doc, &self.filter)
    }

    fn numeric(&self, doc: &Value) -> Option<f64> {
        match self.aggregate.as_str() {
            "count" => Some(1.0),
            _ => self
                .field
                .as_deref()
                .and_then(|field| path::get(doc, field))
                .and_then(Value::as_f64),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WebhookRule {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub coll: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub ops: Vec<String>,
    #[serde(default)]
    pub filter: Value,
    #[serde(default)]
    pub threshold: Option<WebhookThreshold>,
    #[serde(default)]
    pub once: bool,
    #[serde(default)]
    pub cooldown_seconds: u64,
    #[serde(default)]
    pub secret_env: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub max_attempts: u64,
    /// Per-request HTTP timeout. Zero uses the 10-second default.
    #[serde(default)]
    pub timeout_ms: u64,
    #[serde(default)]
    pub enabled: bool,
    /// Event id at which this rule starts. Set by the writer when the rule is
    /// created so the async worker does not miss events written just after it.
    #[serde(default)]
    pub start_cursor: String,
}

impl WebhookRule {
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() || self.name.len() > 200 {
            return Err("webhook name must be 1-200 characters".into());
        }
        if self.name.starts_with('_') {
            return Err("webhook name must not start with `_`".into());
        }
        if self.coll.is_empty() || self.coll.starts_with('_') {
            return Err("webhook collection must be a user collection".into());
        }
        let parsed = reqwest::Url::parse(&self.url)
            .map_err(|error| format!("invalid webhook URL: {error}"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err("webhook URL must use http or https".into());
        }
        for op in &self.ops {
            if !matches!(op.as_str(), "insert" | "update" | "delete") {
                return Err(format!("unsupported webhook op `{op}`"));
            }
        }
        if let Some(threshold) = &self.threshold {
            threshold.validate()?;
        }
        if self.timeout_ms > 300_000 {
            return Err("webhook timeout_ms must be at most 300000".into());
        }
        if self
            .secret_env
            .as_deref()
            .is_some_and(|value| value.trim().is_empty() || value.starts_with('='))
        {
            return Err("webhook secret_env must be an environment variable name".into());
        }
        Ok(())
    }

    fn watches_op(&self, op: &str) -> bool {
        self.ops.is_empty() || self.ops.iter().any(|candidate| candidate == op)
    }

    fn effective_threshold(&self) -> Option<WebhookThreshold> {
        self.threshold.as_ref().map(|threshold| {
            let mut threshold = threshold.clone();
            if threshold.filter.is_null() {
                threshold.filter = self.filter.clone();
            }
            threshold
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WebhookDelivery {
    #[serde(default)]
    pub rule: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub event_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub attempts: u64,
    #[serde(default)]
    pub max_attempts: u64,
    #[serde(default)]
    pub timeout_ms: u64,
    #[serde(default)]
    pub next_attempt_ms: u64,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: u64,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub response_status: Option<u16>,
    #[serde(default)]
    pub secret_env: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RuleState {
    #[serde(default)]
    fingerprint: String,
    #[serde(default)]
    cursor: String,
    #[serde(default)]
    count: u64,
    #[serde(default)]
    sum: f64,
    #[serde(default)]
    fired: bool,
    #[serde(default)]
    last_sent_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct State {
    #[serde(default)]
    rules: BTreeMap<String, RuleState>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn matches_filter(doc: &Value, filter: &Value) -> bool {
    filter.is_null()
        || filter.as_object().is_some_and(|object| object.is_empty())
        || nonodb_core::query::matches(doc, filter)
}

fn event_document(event: &Value, op: &str) -> Value {
    let key = if op == "delete" { "before" } else { "after" };
    event
        .get("payload")
        .and_then(|payload| payload.get(key))
        .cloned()
        .unwrap_or(Value::Null)
}

fn collection_docs(store: &Store, coll: &str) -> Result<Vec<Value>, String> {
    let c = store.collection(coll).map_err(|e| e.to_string())?;
    c.find(&FindQuery {
        filter: json!({}),
        projection: None,
        sort: Vec::new(),
        skip: 0,
        limit: None,
        comment: None,
        max_time_ms: None,
        hint: None,
    })
    .map_err(|e| e.to_string())
}

fn latest_event_id(store: &Store) -> Result<String, String> {
    if !store
        .collections()
        .map_err(|e| e.to_string())?
        .iter()
        .any(|name| name == events::EVENTS_COLLECTION)
    {
        return Ok(String::new());
    }
    let c = store
        .collection(events::EVENTS_COLLECTION)
        .map_err(|e| e.to_string())?;
    let docs = c
        .find(&FindQuery {
            filter: json!({}),
            projection: Some(json!({"_id": 1})),
            sort: vec![("_id".to_string(), -1)],
            skip: 0,
            limit: Some(1),
            comment: None,
            max_time_ms: None,
            hint: None,
        })
        .map_err(|e| e.to_string())?;
    Ok(docs
        .first()
        .and_then(|doc| doc.get("_id"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string())
}

fn load_rules(store: &Store) -> Result<Vec<WebhookRule>, String> {
    if !store
        .collections()
        .map_err(|e| e.to_string())?
        .iter()
        .any(|name| name == WEBHOOKS_COLLECTION)
    {
        return Ok(Vec::new());
    }
    let docs = collection_docs(store, WEBHOOKS_COLLECTION)?;
    let mut rules = Vec::new();
    for doc in docs {
        match serde_json::from_value::<WebhookRule>(doc) {
            Ok(rule) if rule.enabled => rules.push(rule),
            Ok(_) => {}
            Err(error) => tracing::warn!(?error, "invalid webhook rule ignored"),
        }
    }
    Ok(rules)
}

fn rule_fingerprint(rule: &WebhookRule) -> String {
    serde_json::to_string(rule).unwrap_or_default()
}

fn bootstrap(store: &Store, rule: &WebhookRule) -> Result<RuleState, String> {
    let fingerprint = rule_fingerprint(rule);
    let Some(threshold) = rule.effective_threshold() else {
        return Ok(RuleState {
            fingerprint,
            cursor: rule.start_cursor.clone(),
            ..RuleState::default()
        });
    };
    let rtxn = store.read_txn().map_err(|e| e.to_string())?;
    let mut state = RuleState {
        fingerprint,
        ..RuleState::default()
    };
    if let Some(collection) = store
        .collection_at(&rtxn, &rule.coll)
        .map_err(|e| e.to_string())?
    {
        let docs = collection
            .find_in(
                &rtxn,
                &FindQuery {
                    filter: json!({}),
                    projection: None,
                    sort: Vec::new(),
                    skip: 0,
                    limit: None,
                    comment: None,
                    max_time_ms: None,
                    hint: None,
                },
            )
            .map_err(|e| e.to_string())?;
        for doc in docs {
            if threshold.matches_doc(&doc) {
                if let Some(value) = threshold.numeric(&doc) {
                    state.count += 1;
                    state.sum += value;
                }
            }
        }
    }
    if let Some(collection) = store
        .collection_at(&rtxn, events::EVENTS_COLLECTION)
        .map_err(|e| e.to_string())?
    {
        let docs = collection
            .find_in(
                &rtxn,
                &FindQuery {
                    filter: json!({}),
                    projection: Some(json!({"_id": 1})),
                    sort: vec![("_id".to_string(), -1)],
                    skip: 0,
                    limit: Some(1),
                    comment: None,
                    max_time_ms: None,
                    hint: None,
                },
            )
            .map_err(|e| e.to_string())?;
        state.cursor = docs
            .first()
            .and_then(|doc| doc.get("_id"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
    }
    rtxn.commit().map_err(|e| e.to_string())?;
    Ok(state)
}

fn ingest(state: &mut RuleState, threshold: &WebhookThreshold, before: &Value, after: &Value) {
    let old = !before.is_null() && threshold.matches_doc(before);
    let new = !after.is_null() && threshold.matches_doc(after);
    if old {
        if let Some(value) = threshold.numeric(before) {
            state.count = state.count.saturating_sub(1);
            state.sum -= value;
        }
    }
    if new {
        if let Some(value) = threshold.numeric(after) {
            state.count += 1;
            state.sum += value;
        }
    }
    if state.count == 0 {
        state.sum = 0.0;
    }
}

fn submit(write_tx: &mpsc::Sender<WriteJob>, req: WriteReq) -> Result<Value, String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    if write_tx
        .send(WriteJob {
            req,
            resp: tx,
            isolated: false,
            durability: Durability::Full,
        })
        .is_err()
    {
        return Err("writer unavailable".into());
    }
    match rx.blocking_recv() {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(ApiError { code, message })) => Err(format!("{code}: {message}")),
        Err(_) => Err("writer dropped webhook request".into()),
    }
}

fn enqueue_delivery(
    write_tx: &mpsc::Sender<WriteJob>,
    rule: &WebhookRule,
    event: &Value,
    event_id: &str,
    threshold_value: Option<f64>,
) -> Result<(), String> {
    let mut payload = event.clone();
    payload["webhook"] = json!({
        "rule": rule.name,
        "threshold_value": threshold_value,
    });
    let delivery = WebhookDelivery {
        rule: rule.name.clone(),
        url: rule.url.clone(),
        payload,
        event_id: event_id.to_string(),
        status: "pending".into(),
        attempts: 0,
        max_attempts: if rule.max_attempts == 0 {
            8
        } else {
            rule.max_attempts
        },
        timeout_ms: if rule.timeout_ms == 0 {
            10_000
        } else {
            rule.timeout_ms
        },
        next_attempt_ms: 0,
        created_at: now_ms(),
        updated_at: now_ms(),
        last_error: None,
        response_status: None,
        secret_env: rule.secret_env.clone(),
        headers: rule.headers.clone(),
    };
    let mut doc = serde_json::to_value(delivery).map_err(|e| e.to_string())?;
    doc["_id"] = json!(events::new_event_id());
    submit(
        write_tx,
        WriteReq::Insert {
            coll: DELIVERIES_COLLECTION.into(),
            doc,
            vault: None,
        },
    )?;
    Ok(())
}

fn hmac_hex(secret: &str, body: &[u8]) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    let tag = hmac::sign(&key, body);
    let mut out = String::with_capacity(tag.as_ref().len() * 2);
    for byte in tag.as_ref() {
        use std::fmt::Write;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn deliver(write_tx: &mpsc::Sender<WriteJob>, delivery: &Value) -> Result<(), String> {
    let parsed: WebhookDelivery =
        serde_json::from_value(delivery.clone()).map_err(|e| e.to_string())?;
    let id = delivery
        .get("_id")
        .and_then(Value::as_str)
        .ok_or("delivery has no _id")?;
    let body = serde_json::to_vec(&parsed.payload).map_err(|e| e.to_string())?;
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_millis(parsed.timeout_ms.max(1)))
        .build()
        .map_err(|e| e.to_string())?;
    let mut request = client
        .post(&parsed.url)
        .header("content-type", "application/json")
        .header("idempotency-key", &parsed.event_id)
        .header("x-nonodb-webhook", &parsed.rule)
        .body(body.clone());
    for (name, value) in &parsed.headers {
        request = request.header(name, value);
    }
    let secret_error = if let Some(variable) = parsed.secret_env.as_deref() {
        match std::env::var(variable) {
            Ok(secret) => {
                request = request.header(
                    "x-nonodb-signature",
                    format!("sha256={}", hmac_hex(&secret, &body)),
                );
                None
            }
            Err(_) => Some(format!(
                "webhook secret environment variable `{variable}` is not set"
            )),
        }
    } else {
        None
    };
    let result: Result<u16, String> = if let Some(error) = secret_error {
        Err(error)
    } else {
        request
            .send()
            .map_err(|error| error.to_string())
            .and_then(|response| {
                let status = response.status();
                if status.is_success() {
                    Ok(status.as_u16())
                } else {
                    Err(format!("webhook returned HTTP {}", status.as_u16()))
                }
            })
    };
    let now = now_ms();
    let (status, error, response_status, attempts, next) = match result {
        Ok(code) => (
            "delivered".to_string(),
            None,
            Some(code),
            parsed.attempts,
            0,
        ),
        Err(error) => {
            let attempts = parsed.attempts + 1;
            let dead = attempts >= parsed.max_attempts.max(1);
            let delay = (1u64 << attempts.min(8)).saturating_mul(1000).min(300_000);
            (
                if dead {
                    "dead".to_string()
                } else {
                    "retry".to_string()
                },
                Some(error.to_string()),
                None,
                attempts,
                if dead { 0 } else { now + delay },
            )
        }
    };
    let update = json!({
        "$set": {
            "status": status,
            "attempts": attempts,
            "next_attempt_ms": next,
            "updated_at": now,
            "last_error": error,
            "response_status": response_status,
        }
    });
    submit(
        write_tx,
        WriteReq::Update {
            coll: DELIVERIES_COLLECTION.into(),
            filter: json!({"_id": id}),
            update,
            multi: false,
            upsert: false,
            array_filters: Vec::new(),
            default_id: None,
            vault: None,
        },
    )?;
    Ok(())
}

fn process_deliveries(store: &Store, write_tx: &mpsc::Sender<WriteJob>) -> Result<(), String> {
    if !store
        .collections()
        .map_err(|e| e.to_string())?
        .iter()
        .any(|name| name == DELIVERIES_COLLECTION)
    {
        return Ok(());
    }
    let docs = collection_docs(store, DELIVERIES_COLLECTION)?;
    let now = now_ms();
    let mut sent = 0usize;
    for doc in docs {
        if sent >= MAX_DELIVERIES_PER_PASS {
            break;
        }
        let status = doc.get("status").and_then(Value::as_str).unwrap_or("");
        let next = doc
            .get("next_attempt_ms")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if (status == "pending" || status == "retry") && next <= now {
            if let Err(error) = deliver(write_tx, &doc) {
                tracing::warn!(?error, "webhook delivery failed to update state");
            }
            sent += 1;
        }
    }
    Ok(())
}

fn process_events(
    store: &Store,
    write_tx: &mpsc::Sender<WriteJob>,
    rules: &[WebhookRule],
    state: &mut State,
) -> Result<bool, String> {
    if rules.is_empty() {
        return Ok(false);
    }
    let latest = latest_event_id(store)?;
    for rule in rules {
        let fingerprint = rule_fingerprint(rule);
        let stale = state
            .rules
            .get(&rule.name)
            .is_none_or(|current| current.fingerprint != fingerprint);
        if stale {
            match bootstrap(store, rule) {
                Ok(value) => {
                    state.rules.insert(rule.name.clone(), value);
                }
                Err(error) => {
                    tracing::warn!(rule = %rule.name, ?error, "webhook bootstrap failed");
                }
            }
        }
    }
    let min_cursor = state
        .rules
        .values()
        .map(|item| item.cursor.as_str())
        .min()
        .unwrap_or("")
        .to_string();
    if latest.is_empty() || latest <= min_cursor {
        return Ok(false);
    }
    if !store
        .collections()
        .map_err(|e| e.to_string())?
        .iter()
        .any(|name| name == events::EVENTS_COLLECTION)
    {
        return Ok(false);
    }
    let collection = store
        .collection(events::EVENTS_COLLECTION)
        .map_err(|e| e.to_string())?;
    let events = collection
        .find(&FindQuery {
            filter: if min_cursor.is_empty() {
                json!({})
            } else {
                json!({"_id": {"$gt": min_cursor}})
            },
            projection: None,
            sort: vec![("_id".to_string(), 1)],
            skip: 0,
            limit: Some(MAX_EVENTS_PER_PASS),
            comment: None,
            max_time_ms: None,
            hint: None,
        })
        .map_err(|e| e.to_string())?;
    if events.is_empty() {
        return Ok(false);
    }
    let last_id = events
        .last()
        .and_then(|event| event.get("_id"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let mut changed = false;
    let state_before = state.clone();
    let mut enqueue_failed = false;
    for event in &events {
        let event_id = event.get("_id").and_then(Value::as_str).unwrap_or("");
        let coll = event
            .get("payload")
            .and_then(|payload| payload.get("coll"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let op = event
            .get("payload")
            .and_then(|payload| payload.get("op"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let before = event
            .get("payload")
            .and_then(|payload| payload.get("before"))
            .cloned()
            .unwrap_or(Value::Null);
        let after = event
            .get("payload")
            .and_then(|payload| payload.get("after"))
            .cloned()
            .unwrap_or(Value::Null);
        for rule in rules {
            if rule.coll != coll
                || event_id
                    <= state
                        .rules
                        .get(&rule.name)
                        .map(|s| s.cursor.as_str())
                        .unwrap_or("")
            {
                continue;
            }
            let mut fired = false;
            if let Some(threshold) = rule.effective_threshold() {
                if let Some(rule_state) = state.rules.get_mut(&rule.name) {
                    ingest(rule_state, &threshold, &before, &after);
                    let value = threshold.metric(rule_state.count, rule_state.sum);
                    let condition = threshold.matches(value)
                        && matches_filter(&event_document(event, op), &rule.filter);
                    if rule.once {
                        if !condition {
                            rule_state.fired = false;
                        } else if !rule_state.fired {
                            rule_state.fired = true;
                            fired = true;
                        }
                    } else {
                        let cooldown = rule.cooldown_seconds.saturating_mul(1000);
                        if condition
                            && (cooldown == 0
                                || now_ms().saturating_sub(rule_state.last_sent_ms) >= cooldown)
                        {
                            fired = true;
                        }
                    }
                    if fired {
                        rule_state.last_sent_ms = now_ms();
                        if let Err(error) =
                            enqueue_delivery(write_tx, rule, event, event_id, Some(value))
                        {
                            tracing::warn!(?error, rule = %rule.name, "enqueue webhook failed");
                            enqueue_failed = true;
                        }
                    }
                }
            } else {
                let doc = event_document(event, op);
                let condition = rule.watches_op(op) && matches_filter(&doc, &rule.filter);
                if condition {
                    let cooldown = rule.cooldown_seconds.saturating_mul(1000);
                    if let Some(rule_state) = state.rules.get_mut(&rule.name) {
                        if cooldown == 0
                            || now_ms().saturating_sub(rule_state.last_sent_ms) >= cooldown
                        {
                            rule_state.last_sent_ms = now_ms();
                            if let Err(error) =
                                enqueue_delivery(write_tx, rule, event, event_id, None)
                            {
                                tracing::warn!(?error, rule = %rule.name, "enqueue webhook failed");
                                enqueue_failed = true;
                            }
                        }
                    }
                }
            }
        }
    }
    if enqueue_failed {
        // Keep the cursor and aggregate state at the previous checkpoint so the
        // events are replayed. Deliveries already enqueued are duplicated, which
        // is the expected at-least-once trade-off.
        *state = state_before;
        return Err("webhook delivery enqueue failed; events will be replayed".into());
    }
    for rule in rules {
        if let Some(rule_state) = state.rules.get_mut(&rule.name) {
            if rule_state.cursor < last_id {
                rule_state.cursor = last_id.clone();
                changed = true;
            }
        }
    }
    Ok(changed || !events.is_empty())
}

fn load_state(path: &Path) -> State {
    fs::read(path)
        .ok()
        .and_then(|raw| serde_json::from_slice(&raw).ok())
        .unwrap_or_default()
}

fn save_state(path: &Path, state: &State) -> Result<(), String> {
    let tmp = path.with_extension("tmp");
    let raw = serde_json::to_vec_pretty(state).map_err(|e| e.to_string())?;
    fs::write(&tmp, raw).map_err(|e| e.to_string())?;
    fs::rename(&tmp, path).map_err(|e| e.to_string())
}

pub fn start(
    store: Arc<Store>,
    write_tx: mpsc::Sender<WriteJob>,
    database_path: PathBuf,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("nonodb-webhooks".into())
        .spawn(move || run(store, write_tx, database_path, stop))
        .expect("failed to start webhook worker")
}

fn run(
    store: Arc<Store>,
    write_tx: mpsc::Sender<WriteJob>,
    database_path: PathBuf,
    stop: Arc<AtomicBool>,
) {
    let state_path = database_path.join(STATE_FILE);
    let mut state = load_state(&state_path);
    loop {
        if stop.load(Ordering::Relaxed) {
            let _ = save_state(&state_path, &state);
            return;
        }
        if crate::replicator::writable() {
            let rules = load_rules(&store).unwrap_or_else(|error| {
                tracing::warn!(?error, "webhook rule read failed");
                Vec::new()
            });
            if process_deliveries(&store, &write_tx).is_err() {
                tracing::warn!("webhook delivery pass failed");
            }
            match process_events(&store, &write_tx, &rules, &mut state) {
                Ok(changed) => {
                    if changed {
                        let _ = save_state(&state_path, &state);
                    }
                }
                Err(error) => tracing::warn!(?error, "webhook event pass failed"),
            }
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

pub fn list_rules(store: &Store) -> Result<Vec<Value>, String> {
    if !store
        .collections()
        .map_err(|e| e.to_string())?
        .iter()
        .any(|name| name == WEBHOOKS_COLLECTION)
    {
        return Ok(Vec::new());
    }
    collection_docs(store, WEBHOOKS_COLLECTION)
}

pub fn list_deliveries(store: &Store) -> Result<Vec<Value>, String> {
    if !store
        .collections()
        .map_err(|e| e.to_string())?
        .iter()
        .any(|name| name == DELIVERIES_COLLECTION)
    {
        return Ok(Vec::new());
    }
    let c = store
        .collection(DELIVERIES_COLLECTION)
        .map_err(|e| e.to_string())?;
    c.find(&FindQuery {
        filter: json!({}),
        projection: None,
        sort: vec![("created_at".to_string(), -1)],
        skip: 0,
        limit: Some(1000),
        comment: None,
        max_time_ms: None,
        hint: None,
    })
    .map_err(|e| e.to_string())
}

pub fn ensure_events(
    store: &Store,
    wtxn: &mut nonodb_core::heed::RwTxn<'_>,
    coll: &str,
) -> Result<(), String> {
    let mut config = store
        .collection_events_in_rw(wtxn, coll)
        .map_err(|e| e.to_string())?;
    if !config.enabled
        || !config.filter.is_null()
        || !config.include_before
        || !config.include_after
        || !config.ops.is_empty()
        || !config.fields.is_empty()
        || config.projection.is_some()
    {
        config = EventConfig {
            enabled: true,
            ops: Vec::new(),
            filter: Value::Null,
            fields: Vec::new(),
            include_before: true,
            include_after: true,
            projection: None,
        };
        store
            .set_collection_events_in(wtxn, coll, &config)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn threshold_validation_and_comparisons() {
        let threshold = WebhookThreshold {
            aggregate: "sum".into(),
            field: Some("total".into()),
            gte: Some(10.0),
            ..WebhookThreshold::default()
        };
        threshold.validate().unwrap();
        assert!(threshold.matches(10.0));
        assert!(threshold.matches(11.0));
        assert!(!threshold.matches(9.9));
    }

    #[test]
    fn ingest_tracks_sum_and_count_without_a_writer_query() {
        let threshold = WebhookThreshold {
            aggregate: "sum".into(),
            field: Some("total".into()),
            gt: Some(0.0),
            ..WebhookThreshold::default()
        };
        let mut state = RuleState::default();
        ingest(
            &mut state,
            &threshold,
            &Value::Null,
            &json!({"status": "paid", "total": 10}),
        );
        ingest(
            &mut state,
            &threshold,
            &json!({"status": "paid", "total": 10}),
            &json!({"status": "paid", "total": 15}),
        );
        assert_eq!(state.count, 1);
        assert_eq!(state.sum, 15.0);
    }

    #[test]
    fn threshold_validation_rejects_missing_or_ambiguous_settings() {
        let mut threshold = WebhookThreshold {
            aggregate: "sum".into(),
            gte: Some(1.0),
            ..WebhookThreshold::default()
        };
        assert!(threshold.validate().is_err());
        threshold.field = Some("total".into());
        threshold.gt = Some(2.0);
        assert!(threshold.validate().is_err());
        threshold.aggregate = "median".into();
        threshold.gt = None;
        assert!(threshold.validate().is_err());
    }

    #[test]
    fn ingest_handles_updates_and_deletes() {
        let threshold = WebhookThreshold {
            aggregate: "avg".into(),
            field: Some("score".into()),
            gt: Some(0.0),
            filter: json!({"status": "paid"}),
            ..WebhookThreshold::default()
        };
        let mut state = RuleState::default();
        ingest(
            &mut state,
            &threshold,
            &Value::Null,
            &json!({"status": "paid", "score": 2}),
        );
        ingest(
            &mut state,
            &threshold,
            &json!({"status": "paid", "score": 2}),
            &json!({"status": "paid", "score": 4}),
        );
        assert_eq!(state.count, 1);
        assert_eq!(state.sum, 4.0);
        ingest(
            &mut state,
            &threshold,
            &json!({"status": "paid", "score": 4}),
            &json!({"status": "void", "score": 4}),
        );
        assert_eq!(state.count, 0);
        assert_eq!(state.sum, 0.0);
    }

    #[test]
    fn threshold_filter_inherits_the_rule_filter() {
        let rule = WebhookRule {
            filter: json!({"status": "paid"}),
            threshold: Some(WebhookThreshold {
                aggregate: "count".into(),
                gte: Some(1.0),
                ..WebhookThreshold::default()
            }),
            ..WebhookRule::default()
        };
        assert_eq!(
            rule.effective_threshold().unwrap().filter,
            json!({"status": "paid"})
        );
    }

    #[test]
    fn rule_validation_rejects_bad_secret_and_timeout() {
        let mut rule = WebhookRule {
            name: "hook".into(),
            coll: "orders".into(),
            url: "https://example.test/hook".into(),
            ..WebhookRule::default()
        };
        rule.secret_env = Some("=bad".into());
        assert!(rule.validate().is_err());
        rule.secret_env = None;
        rule.timeout_ms = 300_001;
        assert!(rule.validate().is_err());
    }

    #[test]
    fn writer_does_not_evaluate_webhook_filters_or_aggregates() {
        let source = include_str!("writer.rs");
        for forbidden in [
            "matches_filter(",
            "bootstrap(",
            "process_events(",
            "ingest(",
            "hmac_hex(",
            "reqwest::",
        ] {
            assert!(
                !source.contains(forbidden),
                "writer must not contain webhook evaluation: {forbidden}"
            );
        }
    }

    #[test]
    fn delivery_worker_is_primary_gated() {
        let source = include_str!("webhooks.rs");
        assert!(source.contains("if crate::replicator::writable()"));
        assert!(source.contains("_webhook_deliveries"));
    }

    #[test]
    fn rule_validation_rejects_bad_urls_and_ops() {
        let rule = WebhookRule {
            name: "hook".into(),
            coll: "orders".into(),
            url: "file:///tmp/x".into(),
            ..WebhookRule::default()
        };
        assert!(rule.validate().is_err());
        let rule = WebhookRule {
            name: "hook".into(),
            coll: "orders".into(),
            url: "https://example.test/hook".into(),
            ops: vec!["replace".into()],
            ..WebhookRule::default()
        };
        assert!(rule.validate().is_err());
    }

    #[test]
    fn timeseries_events_trigger_a_threshold_webhook() {
        use crate::protocol::{Durability, WriteReq};
        use crate::writer::{self, WriteJob};
        use nonodb_core::Store;
        use std::io::{Read, Write};
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(Store::open(dir.path()).unwrap());
        let settings = nonodb_core::timeseries::Settings {
            time_field: "ts".into(),
            ..Default::default()
        };
        store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let mut wtxn = store.write_txn().unwrap();
        ensure_events(&store, &mut wtxn, "metrics").unwrap();
        wtxn.commit().unwrap();
        // The daemon registers the internal event log at startup.
        store
            .create_collection(
                events::EVENTS_COLLECTION,
                nonodb_core::CollectionProfile::Standard,
            )
            .unwrap();

        // Local HTTP receiver.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = std::sync::mpsc::channel::<Vec<u8>>();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut raw = Vec::new();
                let mut chunk = [0u8; 4096];
                while let Ok(read) = stream.read(&mut chunk) {
                    if read == 0 {
                        break;
                    }
                    raw.extend_from_slice(&chunk[..read]);
                    if let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        let body_start = end + 4;
                        let headers = String::from_utf8_lossy(&raw[..end]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|value| value.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if raw.len() >= body_start + length {
                            break;
                        }
                    }
                }
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok");
                let _ = request_tx.send(raw);
            }
        });

        let rule = WebhookRule {
            name: "ts_sum".into(),
            coll: "metrics".into(),
            url: format!("http://{address}/hook"),
            ops: vec!["insert".into()],
            threshold: Some(WebhookThreshold {
                aggregate: "sum".into(),
                field: Some("value".into()),
                gte: Some(3.0),
                ..WebhookThreshold::default()
            }),
            enabled: true,
            max_attempts: 1,
            timeout_ms: 5_000,
            ..WebhookRule::default()
        };
        let mut rule_doc = serde_json::to_value(&rule).unwrap();
        rule_doc["_id"] = json!("ts_sum");
        store
            .collection(WEBHOOKS_COLLECTION)
            .unwrap()
            .insert_one(rule_doc)
            .unwrap();

        // Real writer thread so events and delivery bookkeeping happen normally.
        let (write_tx, write_rx) = std::sync::mpsc::channel::<WriteJob>();
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let writer_store = store.clone();
        let cache = std::sync::Arc::new(crate::cache::QueryCache::new(
            Duration::from_secs(60),
            8,
            4096,
        ));
        let writer_stop = stop.clone();
        let writer_thread =
            std::thread::spawn(move || writer::run(writer_store, write_rx, cache, writer_stop));
        let submit = |req| {
            let (resp, out) = tokio::sync::oneshot::channel();
            write_tx
                .send(WriteJob {
                    req,
                    resp,
                    isolated: false,
                    durability: Durability::Full,
                })
                .unwrap();
            out.blocking_recv().unwrap().unwrap()
        };

        let rules = load_rules(&store).unwrap();
        let mut state = State::default();
        // Bootstrap the cursor while the event log is still empty.
        let _ = process_events(&store, &write_tx, &rules, &mut state);

        submit(WriteReq::Insert {
            coll: "metrics".into(),
            doc: json!({"ts": 1_700_000_000_000i64, "value": 1}),
            vault: None,
        });
        submit(WriteReq::Insert {
            coll: "metrics".into(),
            doc: json!({"ts": 1_700_000_001_000i64, "value": 2}),
            vault: None,
        });

        let mut received = None;
        for _ in 0..60 {
            let _ = process_deliveries(&store, &write_tx);
            let _ = process_events(&store, &write_tx, &rules, &mut state);
            if let Ok(raw) = request_rx.try_recv() {
                received = Some(raw);
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let raw = received.expect("webhook receiver should get a request");
        let body_start = raw
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .map(|pos| pos + 4)
            .unwrap();
        let body: Value = serde_json::from_slice(&raw[body_start..]).unwrap();
        assert_eq!(body["webhook"]["rule"], json!("ts_sum"));
        assert_eq!(body["webhook"]["threshold_value"], json!(3.0));
        assert_eq!(body["payload"]["after"]["value"], json!(2));

        stop.store(true, Ordering::Relaxed);
        drop(write_tx);
        writer_thread.join().unwrap();
    }
}
