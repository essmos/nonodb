//! Experimental MongoDB wire-protocol frontend.
//!
//! This is deliberately a small compatibility layer over the existing reader
//! and single-writer paths. It implements handshake, basic CRUD, cursors, and
//! a small aggregate subset; unsupported commands fail with MongoDB-style
//! errors instead of silently behaving differently.

use std::io::{self, BufReader};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine as _;
use bson::{Bson, Document};
use ring::rand::SecureRandom;
use serde_json::{json, Value};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use zeroize::Zeroizing;

use nonodb_auth::{Action, Principal};

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
const SCRAM_SHA256: &str = "SCRAM-SHA-256";
const SCRAM_SHA1: &str = "SCRAM-SHA-1";
const SCRAM_ITERATIONS_DEFAULT: u32 = 15_000;
static NEXT_RESPONSE_ID: AtomicI32 = AtomicI32::new(1);
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

struct Message {
    request_id: i32,
    op_code: i32,
    body: Document,
}

#[derive(Clone, Copy)]
enum ScramMechanism {
    Sha256,
    Sha1,
}

impl ScramMechanism {
    fn parse(name: &str) -> Option<Self> {
        match name {
            SCRAM_SHA256 => Some(Self::Sha256),
            SCRAM_SHA1 => Some(Self::Sha1),
            _ => None,
        }
    }

    fn hash(self, data: &[u8]) -> Vec<u8> {
        let algorithm = match self {
            Self::Sha256 => &ring::digest::SHA256,
            Self::Sha1 => &ring::digest::SHA1_FOR_LEGACY_USE_ONLY,
        };
        ring::digest::digest(algorithm, data).as_ref().to_vec()
    }

    fn hmac(self, key: &[u8], data: &[u8]) -> Vec<u8> {
        let algorithm = match self {
            Self::Sha256 => ring::hmac::HMAC_SHA256,
            Self::Sha1 => ring::hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY,
        };
        let key = ring::hmac::Key::new(algorithm, key);
        ring::hmac::sign(&key, data).as_ref().to_vec()
    }

    fn pbkdf2(self, iterations: NonZeroU32, salt: &[u8], password: &[u8]) -> Vec<u8> {
        let algorithm = match self {
            Self::Sha256 => ring::pbkdf2::PBKDF2_HMAC_SHA256,
            Self::Sha1 => ring::pbkdf2::PBKDF2_HMAC_SHA1,
        };
        let mut out = vec![0u8; self.hash_len()];
        ring::pbkdf2::derive(algorithm, iterations, salt, password, &mut out);
        out
    }

    fn hash_len(self) -> usize {
        match self {
            Self::Sha256 => 32,
            Self::Sha1 => 20,
        }
    }
}

#[derive(Clone)]
struct ScramCredential {
    salt: Vec<u8>,
    iterations: u32,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
}

impl ScramCredential {
    fn new(
        mechanism: ScramMechanism,
        password: &[u8],
        salt: Vec<u8>,
        iterations: u32,
    ) -> Result<Self, String> {
        let iterations = NonZeroU32::new(iterations)
            .filter(|value| value.get() >= 4_096)
            .ok_or_else(|| "SCRAM iteration count is too small".to_string())?;
        let salted = mechanism.pbkdf2(iterations, &salt, password);
        let client_key = mechanism.hmac(&salted, b"Client Key");
        let stored_key = mechanism.hash(&client_key);
        let server_key = mechanism.hmac(&salted, b"Server Key");
        Ok(Self {
            salt,
            iterations: iterations.get(),
            stored_key,
            server_key,
        })
    }
}

#[derive(Clone)]
pub(crate) struct MongoAuth {
    username: String,
    principal: String,
    auth_source: String,
    sha256: ScramCredential,
    sha1: ScramCredential,
}

impl MongoAuth {
    pub(crate) fn from_env() -> Result<Option<Self>, String> {
        let username = std::env::var("NONODB_MONGO_USERNAME").ok();
        let password = std::env::var("NONODB_MONGO_PASSWORD").ok();
        let username = username.filter(|value| !value.is_empty());
        let password = password.filter(|value| !value.is_empty());
        let (username, password) = match (username, password) {
            (None, None) => return Ok(None),
            (Some(username), Some(password)) => (username, Zeroizing::new(password)),
            _ => {
                return Err(
                    "NONODB_MONGO_USERNAME and NONODB_MONGO_PASSWORD must be set together"
                        .to_string(),
                )
            }
        };
        let iterations = std::env::var("NONODB_MONGO_SCRAM_ITERATIONS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|value| (4_096..=1_000_000).contains(value))
            .unwrap_or(SCRAM_ITERATIONS_DEFAULT);
        let mut sha256_salt = [0u8; 16];
        let mut sha1_salt = [0u8; 16];
        let random = ring::rand::SystemRandom::new();
        random
            .fill(&mut sha256_salt)
            .and_then(|_| random.fill(&mut sha1_salt))
            .map_err(|_| "cannot generate MongoDB SCRAM salt".to_string())?;
        let principal = std::env::var("NONODB_MONGO_PRINCIPAL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| username.clone());
        let auth_source = std::env::var("NONODB_MONGO_AUTH_SOURCE")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "admin".to_string());
        Self::new_with_salts(
            username,
            &password,
            sha256_salt.to_vec(),
            sha1_salt.to_vec(),
            iterations,
        )
        .map(|mut auth| {
            auth.principal = principal;
            auth.auth_source = auth_source;
            auth
        })
        .map(Some)
    }

    fn new_with_salts(
        username: String,
        password: &str,
        sha256_salt: Vec<u8>,
        sha1_salt: Vec<u8>,
        iterations: u32,
    ) -> Result<Self, String> {
        let sha256 = ScramCredential::new(
            ScramMechanism::Sha256,
            password.as_bytes(),
            sha256_salt,
            iterations,
        )?;
        let digest = format!("{:x}", md5::compute(format!("{username}:mongo:{password}")));
        let sha1 = ScramCredential::new(
            ScramMechanism::Sha1,
            digest.as_bytes(),
            sha1_salt,
            iterations,
        )?;
        Ok(Self {
            username: username.clone(),
            principal: username,
            auth_source: "admin".to_string(),
            sha256,
            sha1,
        })
    }

    fn credential(&self, mechanism: ScramMechanism) -> &ScramCredential {
        match mechanism {
            ScramMechanism::Sha256 => &self.sha256,
            ScramMechanism::Sha1 => &self.sha1,
        }
    }
}

pub(crate) fn tls_from_env() -> Result<Option<TlsAcceptor>, Box<dyn std::error::Error>> {
    let cert = std::env::var("NONODB_MONGO_TLS_CERT")
        .ok()
        .filter(|value| !value.is_empty());
    let key = std::env::var("NONODB_MONGO_TLS_KEY")
        .ok()
        .filter(|value| !value.is_empty());
    let (cert, key) = match (cert, key) {
        (None, None) => return Ok(None),
        (Some(cert), Some(key)) => (cert, key),
        _ => return Err("provide both NONODB_MONGO_TLS_CERT and NONODB_MONGO_TLS_KEY".into()),
    };
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let mut cert_reader = BufReader::new(std::fs::File::open(&cert)?);
    let certs = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;
    let mut key_reader = BufReader::new(std::fs::File::open(&key)?);
    let key = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or("no private key in NONODB_MONGO_TLS_KEY")?;
    let config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    Ok(Some(TlsAcceptor::from(Arc::new(config))))
}

#[derive(Default)]
struct WireSession {
    authenticated: bool,
    user: Option<String>,
    principal: Option<Principal>,
    scram: Option<ScramChallenge>,
}

struct ScramChallenge {
    conversation_id: i32,
    mechanism: ScramMechanism,
    client_first_bare: String,
    server_first: String,
    nonce: String,
}

/// Starts the experimental MongoDB wire listener.
pub async fn serve(
    addr: &str,
    state: super::AppState,
    auth: Option<MongoAuth>,
    tls: Option<TlsAcceptor>,
    mut shutdown: watch::Receiver<bool>,
    drain_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?.to_string();
    let tls_enabled = tls.is_some();
    if auth.is_some() {
        tracing::warn!(
            %addr,
            bound = %bound,
            mechanism = "SCRAM-SHA-256,SCRAM-SHA-1",
            tls = tls_enabled,
            "experimental MongoDB wire listener enabled with SCRAM authentication"
        );
    } else {
        tracing::warn!(
            %addr,
            bound = %bound,
            tls = tls_enabled,
            "experimental unauthenticated MongoDB wire listener enabled; handshake plus insert/find/getMore/killCursors only"
        );
    }
    let auth = auth.map(Arc::new);
    let tls = tls.map(Arc::new);

    let mut connections = JoinSet::new();
    loop {
        let (stream, peer) = tokio::select! {
            _ = super::wait_for_shutdown(&mut shutdown) => break,
            accepted = listener.accept() => accepted?,
        };
        let shutdown = shutdown.clone();
        let state = state.clone();
        let auth = auth.clone();
        let tls = tls.clone();
        connections.spawn(async move {
            let result = match tls {
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(stream) => handle_connection(stream, state, auth, shutdown).await,
                    Err(error) => Err(error),
                },
                None => handle_connection(stream, state, auth, shutdown).await,
            };
            if let Err(error) = result {
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
    auth: Option<Arc<MongoAuth>>,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
    let mut session = WireSession::default();
    loop {
        let message = tokio::select! {
            _ = super::wait_for_shutdown(&mut shutdown) => return Ok(()),
            message = read_message(&mut stream) => message?,
        };
        let Some(message) = message else {
            return Ok(());
        };
        let response = dispatch_command(
            &state,
            connection_id,
            auth.as_deref(),
            &mut session,
            &message.body,
        )
        .await;
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
    auth: Option<&MongoAuth>,
    session: &mut WireSession,
    body: &Document,
) -> Document {
    let command = body
        .keys()
        .find(|key| !key.starts_with('$'))
        .map(String::as_str);
    let unauthenticated = matches!(
        command,
        Some("hello")
            | Some("isMaster")
            | Some("ismaster")
            | Some("ping")
            | Some("buildInfo")
            | Some("buildinfo")
            | Some("getParameter")
            | Some("connectionStatus")
            | Some("saslStart")
            | Some("saslContinue")
            | Some("logout")
            | Some("endSessions")
            | Some("refreshSessions")
    );
    if auth.is_some() && !session.authenticated && !unauthenticated {
        return mongo_error(&ApiError {
            code: "unauthorized".to_string(),
            message: "Authentication required".to_string(),
        });
    }
    if let Some(principal) = &session.principal {
        if let Err(error) = authorize_mongo_command(principal, command.unwrap_or(""), body) {
            return mongo_error(&error);
        }
    }
    let result = match command {
        Some("saslStart") => sasl_start_command(auth, session, body),
        Some("saslContinue") => sasl_continue_command(auth, state.auth.as_deref(), session, body),
        Some("logout") => {
            session.authenticated = false;
            session.user = None;
            session.principal = None;
            session.scram = None;
            Ok(doc_ok())
        }
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
        Some("create") => create_collection_command(state, body).await,
        Some("drop") => drop_collection_command(state, body).await,
        Some("listCollections") => {
            list_collections_command(state, body, session.principal.as_ref()).await
        }
        Some("listDatabases") => list_databases_command(state, session.principal.as_ref()).await,
        Some("dropDatabase") => drop_database_command(state, body).await,
        Some("endSessions") | Some("refreshSessions") => Ok(doc_ok()),
        Some("ping")
        | Some("hello")
        | Some("isMaster")
        | Some("ismaster")
        | Some("buildInfo")
        | Some("getParameter")
        | Some("connectionStatus") => return command_reply(body, auth.is_some(), session),
        _ => return command_not_found(command.unwrap_or("<missing>")),
    };
    match result {
        Ok(reply) => reply,
        Err(error) => mongo_error(&error),
    }
}

fn sasl_start_command(
    auth: Option<&MongoAuth>,
    session: &mut WireSession,
    body: &Document,
) -> Result<Document, ApiError> {
    let Some(auth) = auth else {
        return Err(authentication_failed());
    };
    let mechanism = body
        .get_str("mechanism")
        .map_err(|_| bad_value("saslStart requires a mechanism"))?;
    let Some(mechanism) = ScramMechanism::parse(mechanism) else {
        return Err(authentication_failed());
    };
    if body
        .get_str("$db")
        .ok()
        .is_some_and(|database| database != auth.auth_source)
    {
        return Err(authentication_failed());
    }
    let credential = auth.credential(mechanism);
    let payload = payload_bytes(body)?;
    let message = std::str::from_utf8(&payload).map_err(|_| authentication_failed())?;
    let mut parts = message.splitn(3, ',');
    let channel = parts.next().ok_or_else(authentication_failed)?;
    let authzid = parts.next().ok_or_else(authentication_failed)?;
    let bare = parts.next().ok_or_else(authentication_failed)?;
    if channel == "p" {
        return Err(authentication_failed());
    }
    if !authzid.is_empty() && !authzid.starts_with("a=") {
        return Err(authentication_failed());
    }
    let attrs = scram_attrs(bare)?;
    let username = scram_get(&attrs, "n")
        .ok_or_else(authentication_failed)
        .and_then(scram_unescape)?;
    let client_nonce = scram_get(&attrs, "r").ok_or_else(authentication_failed)?;
    if scram_get(&attrs, "m").is_some()
        || !bool::from(username.as_bytes().ct_eq(auth.username.as_bytes()))
    {
        return Err(authentication_failed());
    }
    let mut random = [0u8; 18];
    ring::rand::SystemRandom::new()
        .fill(&mut random)
        .map_err(|_| ApiError::internal("cannot generate SCRAM nonce"))?;
    let nonce = format!("{client_nonce}{}", URL_SAFE_NO_PAD.encode(random));
    let server_first = format!(
        "r={nonce},s={},i={}",
        STANDARD.encode(&credential.salt),
        credential.iterations
    );
    let conversation_id = session
        .scram
        .as_ref()
        .map(|challenge| challenge.conversation_id)
        .unwrap_or(1);
    session.scram = Some(ScramChallenge {
        conversation_id,
        mechanism,
        client_first_bare: bare.to_string(),
        server_first: server_first.clone(),
        nonce,
    });
    Ok(sasl_reply(conversation_id, false, server_first.as_bytes()))
}

fn sasl_continue_command(
    auth: Option<&MongoAuth>,
    auth_state: Option<&crate::auth::AuthState>,
    session: &mut WireSession,
    body: &Document,
) -> Result<Document, ApiError> {
    let Some(auth) = auth else {
        return Err(authentication_failed());
    };
    let Some(challenge) = session.scram.take() else {
        return Err(authentication_failed());
    };
    let mechanism = challenge.mechanism;
    let credential = auth.credential(mechanism);
    let conversation_id = int_field(body, "conversationId").unwrap_or(0) as i32;
    if conversation_id != challenge.conversation_id {
        return Err(authentication_failed());
    }
    let payload = payload_bytes(body)?;
    let message = std::str::from_utf8(&payload).map_err(|_| authentication_failed())?;
    let proof_at = message.rfind(",p=").ok_or_else(authentication_failed)?;
    let without_proof = &message[..proof_at];
    let proof = STANDARD
        .decode(&message[proof_at + 3..])
        .map_err(|_| authentication_failed())?;
    if proof.len() != mechanism.hash_len() {
        return Err(authentication_failed());
    }
    let attrs = scram_attrs(without_proof)?;
    let channel = scram_get(&attrs, "c").ok_or_else(authentication_failed)?;
    if channel != "biws" && channel != "eSws" {
        return Err(authentication_failed());
    }
    let nonce = scram_get(&attrs, "r").ok_or_else(authentication_failed)?;
    if nonce != challenge.nonce {
        return Err(authentication_failed());
    }
    let auth_message = format!(
        "{},{},{}",
        challenge.client_first_bare, challenge.server_first, without_proof
    );
    let client_signature = mechanism.hmac(&credential.stored_key, auth_message.as_bytes());
    let mut client_key = vec![0u8; mechanism.hash_len()];
    for (value, (proof, signature)) in client_key
        .iter_mut()
        .zip(proof.iter().zip(client_signature.iter()))
    {
        *value = proof ^ signature;
    }
    let recovered = mechanism.hash(&client_key);
    if !bool::from(recovered.ct_eq(&credential.stored_key)) {
        return Err(authentication_failed());
    }
    let server_signature = mechanism.hmac(&credential.server_key, auth_message.as_bytes());
    let principal = match auth_state {
        Some(auth_state) => auth_state
            .registry()
            .principal(&auth.principal)
            .cloned()
            .ok_or_else(authentication_failed)?,
        None => Principal::local_admin(),
    };
    session.authenticated = true;
    session.user = Some(auth.username.clone());
    session.principal = Some(principal);
    let server_final = format!("v={}", STANDARD.encode(server_signature));
    Ok(sasl_reply(
        challenge.conversation_id,
        true,
        server_final.as_bytes(),
    ))
}

fn sasl_reply(conversation_id: i32, done: bool, payload: &[u8]) -> Document {
    bson::doc! {
        "conversationId": conversation_id,
        "done": done,
        "payload": Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: payload.to_vec(),
        }),
        "ok": 1.0,
    }
}

fn payload_bytes(body: &Document) -> Result<Vec<u8>, ApiError> {
    match body.get("payload") {
        Some(Bson::Binary(binary)) => Ok(binary.bytes.clone()),
        Some(Bson::String(value)) => Ok(value.as_bytes().to_vec()),
        _ => Err(bad_value("SASL payload must be binary")),
    }
}

fn scram_attrs(input: &str) -> Result<Vec<(&str, &str)>, ApiError> {
    let mut attrs = Vec::new();
    for part in input.split(',') {
        let (key, value) = part.split_once('=').ok_or_else(authentication_failed)?;
        if key.is_empty() {
            return Err(authentication_failed());
        }
        attrs.push((key, value));
    }
    Ok(attrs)
}

fn scram_get<'a>(attrs: &'a [(&'a str, &'a str)], key: &str) -> Option<&'a str> {
    attrs
        .iter()
        .find_map(|(name, value)| (*name == key).then_some(*value))
}

fn scram_unescape(value: &str) -> Result<String, ApiError> {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch == '=' {
            let escaped = match (chars.next(), chars.next()) {
                (Some('2'), Some('C')) => ',',
                (Some('3'), Some('D')) => '=',
                _ => return Err(authentication_failed()),
            };
            out.push(escaped);
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}

fn authentication_failed() -> ApiError {
    ApiError {
        code: "authentication_failed".to_string(),
        message: "Authentication failed".to_string(),
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

async fn create_collection_command(
    state: &super::AppState,
    body: &Document,
) -> Result<Document, ApiError> {
    let collection = body
        .get_str("create")
        .map_err(|_| bad_value("create must name a collection"))?
        .to_string();
    let database = database_name(body);
    for unsupported in [
        "capped",
        "size",
        "max",
        "validator",
        "validationLevel",
        "validationAction",
        "viewOn",
        "pipeline",
        "collation",
        "timeseries",
        "expireAfterSeconds",
        "storageEngine",
        "indexOptionDefaults",
        "changeStreamPreAndPostImages",
        "clusteredIndex",
    ] {
        if body.contains_key(unsupported) {
            return Err(bad_value(format!(
                "collection option `{unsupported}` is not supported by NonoDB"
            )));
        }
    }
    let runtime = database_for_write(state, &database).await?;
    let (_, response) = super::submit_write(
        &runtime,
        WriteReq::CreateCollection {
            coll: collection,
            profile: nonodb_core::CollectionProfile::Standard,
            ttl_seconds: None,
            ttl_purge_history: None,
        },
        Durability::Full,
    )
    .await;
    let api = response.0;
    if let Some(error) = api.error {
        return Err(error);
    }
    Ok(doc_ok())
}

async fn drop_collection_command(
    state: &super::AppState,
    body: &Document,
) -> Result<Document, ApiError> {
    let collection = body
        .get_str("drop")
        .map_err(|_| bad_value("drop must name a collection"))?
        .to_string();
    let database = database_name(body);
    let runtime = database_for_write(state, &database).await?;
    let (_, response) = super::submit_write(
        &runtime,
        WriteReq::DropCollection {
            coll: collection.clone(),
        },
        Durability::Full,
    )
    .await;
    let api = response.0;
    if let Some(error) = api.error {
        return Err(error);
    }
    Ok(bson::doc! {
        "ns": format!("{database}.{collection}"),
        "ok": 1.0,
    })
}

async fn list_collections_command(
    state: &super::AppState,
    body: &Document,
    principal: Option<&Principal>,
) -> Result<Document, ApiError> {
    let database = database_name(body);
    let namespace = format!("{database}.$cmd.listCollections");
    let Some(runtime) = database_for_read(state, &database).await else {
        return Ok(cursor_reply(0, &namespace, Vec::new(), true));
    };
    let store = runtime.store.clone();
    let names = tokio::task::spawn_blocking(move || {
        let rtxn = store.read_txn()?;
        let names = store.collections_in(&rtxn)?;
        rtxn.commit()?;
        Ok::<Vec<String>, nonodb_core::Error>(names)
    })
    .await
    .map_err(|error| ApiError::internal(format!("listCollections join error: {error}")))??;
    let name_only = body.get_bool("nameOnly").unwrap_or(false);
    let mut batch = Vec::with_capacity(names.len());
    for name in names {
        if name.starts_with('\u{1}') || name == crate::databases::DEFAULT_DATABASE {
            continue;
        }
        if principal.is_some_and(|principal| !principal.can_see_collection(&database, &name)) {
            continue;
        }
        let mut doc = bson::doc! {
            "name": name.clone(),
            "type": "collection",
        };
        if !name_only {
            doc.insert("options", Bson::Document(Document::new()));
            doc.insert("info", Bson::Document(bson::doc! {"readOnly": false}));
            doc.insert(
                "idIndex",
                Bson::Document(bson::doc! {
                    "v": 2i32,
                    "key": Bson::Document(bson::doc! {"_id": 1i32}),
                    "name": "_id_",
                }),
            );
        }
        batch.push(Bson::Document(doc));
    }
    Ok(cursor_reply(0, &namespace, batch, true))
}

async fn drop_database_command(
    state: &super::AppState,
    body: &Document,
) -> Result<Document, ApiError> {
    let database = database_name(body);
    match state.databases.drop_database(&database).await {
        Ok(_) => Ok(bson::doc! {
            "dropped": database,
            "ok": 1.0,
        }),
        Err(error) => Err(error),
    }
}

async fn list_databases_command(
    state: &super::AppState,
    principal: Option<&Principal>,
) -> Result<Document, ApiError> {
    let names = state.databases.names().await;
    let mut databases = Vec::with_capacity(names.len());
    for name in names {
        if principal.is_some_and(|principal| !principal.can_see_database(&name)) {
            continue;
        }
        databases.push(Bson::Document(bson::doc! {
            "name": name,
            "sizeOnDisk": 0i64,
            "empty": false,
        }));
    }
    Ok(bson::doc! {
        "databases": Bson::Array(databases),
        "totalSize": 0i64,
        "ok": 1.0,
    })
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
    let sort = sort_fields(body)?;
    let ascending = id_sort_ascending(&sort)?;
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
            sort,
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

fn id_sort_ascending(sort: &[(String, i32)]) -> Result<bool, ApiError> {
    match sort {
        [] => Ok(true),
        [(field, direction)] if matches!(field.as_str(), "_id" | "$natural") => Ok(*direction > 0),
        _ => Ok(true),
    }
}

fn values_to_bson(values: Vec<Value>) -> Result<Vec<Bson>, ApiError> {
    values
        .into_iter()
        .map(|value| bson_map::value_to_bson(&value).map_err(ApiError::internal))
        .collect()
}

fn authorize_mongo_command(
    principal: &Principal,
    command: &str,
    body: &Document,
) -> Result<(), ApiError> {
    let database = database_name(body);
    let collection = |name: &str| {
        body.get_str(name)
            .map_err(|_| bad_value(format!("{command} must name a collection")))
    };
    match command {
        "insert" => mongo_allow(principal, Action::Write, &database, collection("insert")?),
        "update" => mongo_allow(principal, Action::Write, &database, collection("update")?),
        "delete" => mongo_allow(principal, Action::Write, &database, collection("delete")?),
        "findAndModify" | "findandmodify" => mongo_allow(
            principal,
            Action::Write,
            &database,
            body.get_str("findAndModify")
                .or_else(|_| body.get_str("findandmodify"))
                .map_err(|_| bad_value("findAndModify must name a collection"))?,
        ),
        "find" => mongo_allow(principal, Action::Read, &database, collection("find")?),
        "count" => mongo_allow(principal, Action::Read, &database, collection("count")?),
        "distinct" => mongo_allow(principal, Action::Read, &database, collection("distinct")?),
        "listIndexes" => mongo_allow(
            principal,
            Action::Read,
            &database,
            collection("listIndexes")?,
        ),
        "createIndexes" => mongo_allow(
            principal,
            Action::AdminCollection,
            &database,
            collection("createIndexes")?,
        ),
        "dropIndexes" => mongo_allow(
            principal,
            Action::AdminCollection,
            &database,
            collection("dropIndexes")?,
        ),
        "create" => mongo_allow(
            principal,
            Action::AdminCollection,
            &database,
            collection("create")?,
        ),
        "drop" => mongo_allow(
            principal,
            Action::AdminCollection,
            &database,
            collection("drop")?,
        ),
        "aggregate" => {
            let coll = collection("aggregate")?;
            if coll == "1" {
                return Err(bad_value("database-level aggregate is not supported"));
            }
            mongo_allow(principal, Action::Read, &database, coll)?;
            let Ok(stages) = body.get_array("pipeline") else {
                return Ok(());
            };
            let mut pipeline = Vec::with_capacity(stages.len());
            for stage in stages {
                let Bson::Document(document) = stage else {
                    return Ok(());
                };
                let Ok(value) = bson_map::document_to_value(document) else {
                    return Ok(());
                };
                pipeline.push(value);
            }
            let Ok(Some(spec)) = nonodb_core::aggregate::write_stage_spec(&pipeline) else {
                return Ok(());
            };
            if let Some(target_database) = &spec.database {
                if target_database != &database {
                    return Err(ApiError {
                        code: "invalid_query".into(),
                        message: "cross-database $out/$merge is not supported".into(),
                    });
                }
            }
            mongo_allow(principal, Action::Write, &database, &spec.target)
        }
        "dropDatabase" => {
            if principal.allows_admin_database() {
                Ok(())
            } else {
                Err(ApiError::forbidden())
            }
        }
        "getMore" | "killCursors" | "listCollections" | "listDatabases" | "endSessions"
        | "refreshSessions" => Ok(()),
        _ => Ok(()),
    }
}

fn mongo_allow(
    principal: &Principal,
    action: Action,
    database: &str,
    collection: &str,
) -> Result<(), ApiError> {
    if principal.allows(action, database, collection) {
        Ok(())
    } else {
        Err(ApiError::forbidden())
    }
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
        "unauthorized" | "forbidden" => (13, "Unauthorized"),
        "authentication_failed" => (18, "AuthenticationFailed"),
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

fn command_reply(body: &Document, auth_enabled: bool, session: &WireSession) -> Document {
    let command = body.keys().next().map(String::as_str).unwrap_or("");
    match command {
        "hello" | "isMaster" | "ismaster" => hello_reply(auth_enabled),
        "ping" => doc_ok(),
        "buildInfo" | "buildinfo" => build_info(),
        "getParameter" => get_parameter(),
        "connectionStatus" => connection_status(session),
        _ => command_not_found(command),
    }
}

fn doc_ok() -> Document {
    let mut doc = Document::new();
    doc.insert("ok", 1.0);
    doc
}

fn hello_reply(auth_enabled: bool) -> Document {
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
    if auth_enabled {
        doc.insert(
            "saslSupportedMechs",
            Bson::Array(vec![
                Bson::String(SCRAM_SHA256.to_string()),
                Bson::String(SCRAM_SHA1.to_string()),
            ]),
        );
    }
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

fn connection_status(session: &WireSession) -> Document {
    let mut doc = doc_ok();
    let mut auth_info = Document::new();
    if let Some(user) = &session.user {
        auth_info.insert(
            "authenticatedUsers",
            Bson::Array(vec![Bson::Document(bson::doc! {
                "user": user.clone(),
                "db": "admin",
            })]),
        );
        let roles = if session
            .principal
            .as_ref()
            .is_some_and(Principal::allows_admin_database)
        {
            vec![Bson::Document(bson::doc! {
                "role": "root",
                "db": "admin",
            })]
        } else {
            Vec::new()
        };
        auth_info.insert("authenticatedUserRoles", Bson::Array(roles));
    } else {
        auth_info.insert("authenticatedUsers", Bson::Array(Vec::new()));
        auth_info.insert("authenticatedUserRoles", Bson::Array(Vec::new()));
    }
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

    fn scram_payload(reply: &Document) -> String {
        match reply.get("payload").unwrap() {
            Bson::Binary(binary) => String::from_utf8(binary.bytes.clone()).unwrap(),
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    fn scram_continue(
        auth: &MongoAuth,
        mechanism: ScramMechanism,
        password: &str,
        client_first: &str,
        server_first: &str,
        nonce: &str,
    ) -> Document {
        let bare = client_first.splitn(3, ',').nth(2).unwrap();
        let attrs = scram_attrs(server_first).unwrap();
        let salt = STANDARD.decode(scram_get(&attrs, "s").unwrap()).unwrap();
        let iterations = scram_get(&attrs, "i").unwrap().parse::<u32>().unwrap();
        let salted = mechanism.pbkdf2(
            NonZeroU32::new(iterations).unwrap(),
            &salt,
            password.as_bytes(),
        );
        let client_key = mechanism.hmac(&salted, b"Client Key");
        let stored_key = mechanism.hash(&client_key);
        let without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!("{bare},{server_first},{without_proof}");
        let signature = mechanism.hmac(&stored_key, auth_message.as_bytes());
        let mut proof = vec![0u8; mechanism.hash_len()];
        for (value, (client, signature)) in proof
            .iter_mut()
            .zip(client_key.iter().zip(signature.iter()))
        {
            *value = client ^ signature;
        }
        let payload = format!("{without_proof},p={}", STANDARD.encode(proof));
        let body = bson::doc! {
            "saslContinue": 1,
            "conversationId": 1i32,
            "payload": Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: payload.into_bytes(),
            }),
        };
        let mut session = WireSession {
            scram: Some(ScramChallenge {
                conversation_id: 1,
                mechanism,
                client_first_bare: bare.to_string(),
                server_first: server_first.to_string(),
                nonce: nonce.to_string(),
            }),
            ..WireSession::default()
        };
        sasl_continue_command(Some(auth), None, &mut session, &body).unwrap()
    }

    fn test_auth() -> MongoAuth {
        MongoAuth::new_with_salts(
            "alice".into(),
            "pencil",
            vec![1, 2, 3, 4],
            vec![5, 6, 7, 8],
            4_096,
        )
        .unwrap()
    }

    fn scram_start(auth: &MongoAuth, mechanism: &str, client_first: &str) -> Document {
        let mut session = WireSession::default();
        let start = bson::doc! {
            "saslStart": 1,
            "mechanism": mechanism,
            "payload": Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: client_first.as_bytes().to_vec(),
            }),
        };
        sasl_start_command(Some(auth), &mut session, &start).unwrap()
    }

    #[test]
    fn scram_sha256_round_trip() {
        let auth = test_auth();
        let client_first = "n,,n=alice,r=clientnonce";
        let reply = scram_start(&auth, SCRAM_SHA256, client_first);
        assert!(!reply.get_bool("done").unwrap());
        let server_first = scram_payload(&reply);
        let nonce = scram_get(&scram_attrs(&server_first).unwrap(), "r")
            .unwrap()
            .to_string();
        let final_reply = scram_continue(
            &auth,
            ScramMechanism::Sha256,
            "pencil",
            client_first,
            &server_first,
            &nonce,
        );
        assert!(final_reply.get_bool("done").unwrap());
        assert!(scram_payload(&final_reply).starts_with("v="));
    }

    #[test]
    fn scram_sha1_round_trip() {
        let auth = test_auth();
        let client_first = "n,,n=alice,r=clientnonce";
        let reply = scram_start(&auth, SCRAM_SHA1, client_first);
        let server_first = scram_payload(&reply);
        let nonce = scram_get(&scram_attrs(&server_first).unwrap(), "r")
            .unwrap()
            .to_string();
        let digest = format!("{:x}", md5::compute("alice:mongo:pencil"));
        let final_reply = scram_continue(
            &auth,
            ScramMechanism::Sha1,
            &digest,
            client_first,
            &server_first,
            &nonce,
        );
        assert!(final_reply.get_bool("done").unwrap());
        assert!(scram_payload(&final_reply).starts_with("v="));
    }

    #[test]
    fn scram_sha256_rejects_wrong_password() {
        let auth = test_auth();
        let mut session = WireSession::default();
        let client_first = "n,,n=alice,r=clientnonce";
        let start = bson::doc! {
            "saslStart": 1,
            "mechanism": SCRAM_SHA256,
            "payload": Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: client_first.as_bytes().to_vec(),
            }),
        };
        let reply = sasl_start_command(Some(&auth), &mut session, &start).unwrap();
        let server_first = scram_payload(&reply);
        let nonce = scram_get(&scram_attrs(&server_first).unwrap(), "r")
            .unwrap()
            .to_string();
        let bare = "n=alice,r=clientnonce";
        let attrs = scram_attrs(&server_first).unwrap();
        let salt = STANDARD.decode(scram_get(&attrs, "s").unwrap()).unwrap();
        let iterations = scram_get(&attrs, "i").unwrap().parse::<u32>().unwrap();
        let mechanism = ScramMechanism::Sha256;
        let salted = mechanism.pbkdf2(NonZeroU32::new(iterations).unwrap(), &salt, b"wrong");
        let client_key = mechanism.hmac(&salted, b"Client Key");
        let stored_key = mechanism.hash(&client_key);
        let without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!("{bare},{server_first},{without_proof}");
        let signature = mechanism.hmac(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(signature.iter())
            .map(|(client, sig)| client ^ sig)
            .collect();
        let payload = format!("{without_proof},p={}", STANDARD.encode(proof));
        let body = bson::doc! {
            "saslContinue": 1,
            "conversationId": 1i32,
            "payload": Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: payload.into_bytes(),
            }),
        };
        let mut challenge_session = WireSession {
            scram: Some(ScramChallenge {
                conversation_id: 1,
                mechanism,
                client_first_bare: bare.to_string(),
                server_first: server_first.to_string(),
                nonce,
            }),
            ..WireSession::default()
        };
        let error =
            sasl_continue_command(Some(&auth), None, &mut challenge_session, &body).unwrap_err();
        assert_eq!(error.code, "authentication_failed");
        assert!(!challenge_session.authenticated);
    }

    #[test]
    fn scram_start_checks_the_configured_auth_source() {
        let mut auth = test_auth();
        auth.auth_source = "app".into();
        let mut session = WireSession::default();
        let start = bson::doc! {
            "saslStart": 1,
            "mechanism": SCRAM_SHA256,
            "$db": "admin",
            "payload": Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: b"n,,n=alice,r=clientnonce".to_vec(),
            }),
        };
        let error = sasl_start_command(Some(&auth), &mut session, &start).unwrap_err();
        assert_eq!(error.code, "authentication_failed");

        let start = bson::doc! {
            "saslStart": 1,
            "mechanism": SCRAM_SHA256,
            "$db": "app",
            "payload": Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: b"n,,n=alice,r=clientnonce".to_vec(),
            }),
        };
        assert!(sasl_start_command(Some(&auth), &mut session, &start).is_ok());
    }

    #[test]
    fn principal_scopes_authorize_wire_commands() {
        let principal = Principal::new(
            "app",
            &["read:app/orders".into(), "write:app/orders".into()],
        )
        .unwrap();
        assert!(authorize_mongo_command(
            &principal,
            "find",
            &bson::doc! { "find": "orders", "$db": "app" }
        )
        .is_ok());
        assert!(authorize_mongo_command(
            &principal,
            "insert",
            &bson::doc! { "insert": "orders", "$db": "app" }
        )
        .is_ok());
        assert_eq!(
            authorize_mongo_command(
                &principal,
                "insert",
                &bson::doc! { "insert": "customers", "$db": "app" }
            )
            .unwrap_err()
            .code,
            "forbidden"
        );
        assert_eq!(
            authorize_mongo_command(
                &principal,
                "dropDatabase",
                &bson::doc! { "dropDatabase": 1, "$db": "app" }
            )
            .unwrap_err()
            .code,
            "forbidden"
        );
        assert!(principal.can_see_database("app"));
        assert!(!principal.can_see_database("other"));
    }

    #[test]
    fn find_sort_support_is_explicit() {
        assert!(id_sort_ascending(&[("_id".into(), 1)]).unwrap());
        assert!(!id_sort_ascending(&[("_id".into(), -1)]).unwrap());
        assert!(id_sort_ascending(&[("name".into(), 1)]).unwrap());
        assert_eq!(
            sort_fields(&bson::doc! { "sort": { "name": 1, "_id": -1 } }).unwrap(),
            vec![("name".into(), 1), ("_id".into(), -1)]
        );
    }

    #[test]
    fn hello_reply_advertises_wire_contract() {
        let reply = command_reply(&bson::doc! { "hello": 1 }, false, &WireSession::default());
        assert_eq!(reply.get_f64("ok").unwrap(), 1.0);
        assert_eq!(reply.get_i32("maxWireVersion").unwrap(), MAX_WIRE_VERSION);
        assert_eq!(reply.get_i32("maxBsonObjectSize").unwrap(), MAX_BSON_BYTES);
        assert!(reply.get_bool("isWritablePrimary").unwrap());
    }

    #[test]
    fn ping_and_unknown_commands_have_mongodb_errors() {
        assert_eq!(
            command_reply(&bson::doc! { "ping": 1 }, false, &WireSession::default())
                .get_f64("ok")
                .unwrap(),
            1.0
        );
        let reply = command_reply(
            &bson::doc! { "notACommand": 1 },
            false,
            &WireSession::default(),
        );
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
