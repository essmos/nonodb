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
use tokio::sync::{watch, Semaphore};
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
const DEFAULT_MAX_CONNECTIONS: usize = 1_024;
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
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

#[derive(Clone, Copy)]
pub(crate) struct WireLimits {
    max_connections: usize,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    handshake_timeout: Option<Duration>,
}

impl WireLimits {
    pub(crate) fn from_env() -> Result<Self, String> {
        Ok(Self {
            max_connections: parse_limit("NONODB_MONGO_MAX_CONNECTIONS", DEFAULT_MAX_CONNECTIONS)?,
            read_timeout: parse_timeout("NONODB_MONGO_READ_TIMEOUT_SECS", DEFAULT_READ_TIMEOUT)?,
            write_timeout: parse_timeout("NONODB_MONGO_WRITE_TIMEOUT_SECS", DEFAULT_WRITE_TIMEOUT)?,
            handshake_timeout: parse_timeout(
                "NONODB_MONGO_HANDSHAKE_TIMEOUT_SECS",
                DEFAULT_HANDSHAKE_TIMEOUT,
            )?,
        })
    }
}

fn parse_limit(name: &str, default: usize) -> Result<usize, String> {
    match std::env::var(name) {
        Ok(value) => match value.trim().parse::<usize>() {
            Ok(limit) if limit > 0 => Ok(limit),
            _ => Err(format!("{name} must be a positive integer")),
        },
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("cannot read {name}: {error}")),
    }
}

fn parse_timeout(name: &str, default: Duration) -> Result<Option<Duration>, String> {
    match std::env::var(name) {
        Ok(value) if value.trim() == "0" => Ok(None),
        Ok(value) => match value.trim().parse::<u64>() {
            Ok(seconds) if seconds > 0 => Ok(Some(Duration::from_secs(seconds))),
            _ => Err(format!(
                "{name} must be 0 or a positive integer number of seconds"
            )),
        },
        Err(std::env::VarError::NotPresent) => Ok(Some(default)),
        Err(error) => Err(format!("cannot read {name}: {error}")),
    }
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
    limits: WireLimits,
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
            max_connections = limits.max_connections,
            "experimental MongoDB wire listener enabled with SCRAM authentication"
        );
    } else {
        tracing::warn!(
            %addr,
            bound = %bound,
            tls = tls_enabled,
            max_connections = limits.max_connections,
            "experimental unauthenticated MongoDB wire listener enabled; documented MongoDB-compatible subset only"
        );
    }
    let auth = auth.map(Arc::new);
    let tls = tls.map(Arc::new);
    let connection_limit = Arc::new(Semaphore::new(limits.max_connections));

    let mut connections = JoinSet::new();
    loop {
        let (stream, peer) = tokio::select! {
            _ = super::wait_for_shutdown(&mut shutdown) => break,
            accepted = listener.accept() => accepted?,
        };
        let permit = match connection_limit.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                tracing::warn!(
                    %peer,
                    max_connections = limits.max_connections,
                    "Mongo wire connection limit reached; rejecting connection"
                );
                drop(stream);
                continue;
            }
        };
        let shutdown = shutdown.clone();
        let state = state.clone();
        let auth = auth.clone();
        let tls = tls.clone();
        connections.spawn(async move {
            let _permit = permit;
            let result = match tls {
                Some(acceptor) => match timeout_opt(
                    limits.handshake_timeout,
                    acceptor.accept(stream),
                    "Mongo TLS handshake timed out",
                )
                .await
                {
                    Ok(stream) => {
                        handle_connection(
                            stream,
                            state,
                            auth,
                            shutdown,
                            limits.read_timeout,
                            limits.write_timeout,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                },
                None => {
                    handle_connection(
                        stream,
                        state,
                        auth,
                        shutdown,
                        limits.read_timeout,
                        limits.write_timeout,
                    )
                    .await
                }
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
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
    let mut session = WireSession::default();
    loop {
        let message = tokio::select! {
            _ = super::wait_for_shutdown(&mut shutdown) => return Ok(()),
            message = timeout_opt(
                read_timeout,
                read_message(&mut stream),
                "Mongo wire read timed out",
            ) => message?,
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
        let write = async {
            stream.write_all(&bytes).await?;
            stream.flush().await
        };
        timeout_opt(write_timeout, write, "Mongo wire write timed out").await?;
    }
}

async fn timeout_opt<T>(
    limit: Option<Duration>,
    future: impl std::future::Future<Output = io::Result<T>>,
    message: &'static str,
) -> io::Result<T> {
    match limit {
        Some(limit) => match tokio::time::timeout(limit, future).await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, message)),
        },
        None => future.await,
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
    if let Some(command) = command {
        if let Err(error) = validate_mongo_options(command, body) {
            return mongo_error(&error);
        }
        if let Err(error) = validate_mongo_bson_values(command, body) {
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
        Some("aggregate") => aggregate_command(state, connection_id, body).await,
        Some("createIndexes") => create_indexes_command(state, body).await,
        Some("listIndexes") => list_indexes_command(state, body).await,
        Some("dropIndexes") => drop_indexes_command(state, body).await,
        Some("create") => create_collection_command(state, body).await,
        Some("drop") => drop_collection_command(state, body).await,
        Some("listCollections") => {
            list_collections_command(state, body, session.principal.as_ref()).await
        }
        Some("listDatabases") => {
            list_databases_command(state, body, session.principal.as_ref()).await
        }
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
        let upsert = spec.get_bool("upsert").unwrap_or(false);
        let (_, response) = super::submit_write(
            &runtime,
            WriteReq::Update {
                coll: collection.clone(),
                filter,
                update,
                multi: spec.get_bool("multi").unwrap_or(false),
                upsert,
                array_filters,
                default_id: upsert.then(new_object_id),
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
        if spec.contains_key("collation") || spec.contains_key("hint") || spec.contains_key("let") {
            errors.push(write_error(
                index,
                &bad_value("collation, hint, and let are not supported by the wire delete command"),
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
    let upsert = body.get_bool("upsert").unwrap_or(false);
    let (_, response) = super::submit_write(
        &runtime,
        WriteReq::FindAndModify {
            coll: collection.clone(),
            filter,
            update,
            sort,
            new: body.get_bool("new").unwrap_or(false),
            upsert,
            remove,
            array_filters,
            projection,
            default_id: (!remove && upsert).then(new_object_id),
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
    let raw_filter = document_field(body, "query")?.unwrap_or_else(|| json!({}));
    let (text_query, filter) = extract_text_filter(&raw_filter)?;
    let skip = non_negative(body, "skip")?.unwrap_or(0);
    let limit = non_negative(body, "limit")?.unwrap_or(0);
    let max_time_ms = int_field(body, "maxTimeMS")
        .filter(|value| *value > 0)
        .map(|value| value as u64);
    if text_query.is_some() && max_time_ms.is_some() {
        return Err(bad_value("maxTimeMS is not supported with $text by NonoDB"));
    }
    let Some(runtime) = database_for_read(state, &database).await else {
        return Ok(bson::doc! {"n": 0i64, "ok": 1.0});
    };
    let store = runtime.store.clone();
    let coll = collection.clone();
    let count = tokio::task::spawn_blocking(move || {
        if let Some(query) = text_query {
            let rtxn = store.read_txn()?;
            let Some(c) = store.collection_at(&rtxn, &coll)? else {
                rtxn.commit()?;
                return Ok::<usize, nonodb_core::Error>(0);
            };
            let Some(def) = c
                .search_index_defs_in(&rtxn)?
                .into_iter()
                .find(|def| def.kind == nonodb_core::SearchKind::Text)
            else {
                return Err(nonodb_core::Error::Query(
                    "text index required for $text query".into(),
                ));
            };
            let docs = c.search_text_index_in(
                &rtxn,
                &def.name,
                &query,
                &filter,
                0,
                None,
                nonodb_core::TextQueryMode::Mongo,
            )?;
            rtxn.commit()?;
            Ok(docs.len())
        } else {
            let c = store.collection(&coll)?;
            c.count_with_options(&filter, max_time_ms)
        }
    })
    .await
    .map_err(|error| ApiError::internal(format!("count join error: {error}")))??;
    let mut count = count.saturating_sub(skip);
    if limit != 0 {
        count = count.min(limit);
    }
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
    let raw_filter = document_field(body, "query")?.unwrap_or_else(|| json!({}));
    let (text_query, filter) = extract_text_filter(&raw_filter)?;
    let max_time_ms = int_field(body, "maxTimeMS")
        .filter(|value| *value > 0)
        .map(|value| value as u64);
    if text_query.is_some() && max_time_ms.is_some() {
        return Err(bad_value("maxTimeMS is not supported with $text by NonoDB"));
    }
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
        if let Some(query) = text_query {
            let rtxn = store.read_txn()?;
            let Some(c) = store.collection_at(&rtxn, &coll)? else {
                rtxn.commit()?;
                return Ok::<Vec<Value>, nonodb_core::Error>(Vec::new());
            };
            let Some(def) = c
                .search_index_defs_in(&rtxn)?
                .into_iter()
                .find(|def| def.kind == nonodb_core::SearchKind::Text)
            else {
                return Err(nonodb_core::Error::Query(
                    "text index required for $text query".into(),
                ));
            };
            let docs = c.search_text_index_in(
                &rtxn,
                &def.name,
                &query,
                &filter,
                0,
                None,
                nonodb_core::TextQueryMode::Mongo,
            )?;
            rtxn.commit()?;
            let mut values = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for doc in docs {
                let Some(value) = nonodb_core::query::path::get(&doc, &key) else {
                    continue;
                };
                let items: Vec<Value> = match value {
                    Value::Array(items) => items.clone(),
                    value => vec![value.clone()],
                };
                for value in items {
                    let marker = serde_json::to_string(&value)?;
                    if seen.insert(marker) {
                        values.push(value);
                    }
                }
            }
            return Ok(values);
        }
        let c = store.collection(&coll)?;
        let docs = c.aggregate_with_options(&pipeline, max_time_ms)?;
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

async fn aggregate_command(
    state: &super::AppState,
    connection_id: u64,
    body: &Document,
) -> Result<Document, ApiError> {
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
    let comment = body.get_str("comment").ok().map(str::to_string);
    let cursor_options = aggregate_cursor_options(body)?;
    let raw_batch_size = cursor_options
        .batch_size
        .unwrap_or(cursor::DEFAULT_BATCH_SIZE);
    let empty_first_batch = cursor_options.batch_size == Some(0);
    let batch_size = if raw_batch_size == 0 {
        cursor::DEFAULT_BATCH_SIZE
    } else {
        cursor::normalize_batch(Some(raw_batch_size))
    };
    let mut remaining = pipeline.clone();
    let mut text_query = None;
    let mut text_residual = json!({});
    if let Some(filter) = pipeline.first().and_then(|stage| stage.get("$match")) {
        let (found, residual) = extract_text_filter(filter)?;
        if found.is_some() {
            text_query = found;
            text_residual = residual.clone();
            remaining = Vec::new();
            if residual != json!({}) {
                remaining.push(json!({"$match": residual}));
            }
            remaining.extend(pipeline.iter().skip(1).cloned());
        }
    }
    if text_query.is_none() && pipeline.iter().any(value_contains_text) {
        return Err(bad_value(
            "$text is only supported in a leading aggregate $match by NonoDB",
        ));
    }
    if let Some(query) = text_query {
        if max_time_ms.is_some() {
            return Err(bad_value("maxTimeMS is not supported with $text by NonoDB"));
        }
        if nonodb_core::aggregate::write_stage_spec(&remaining)?.is_some() {
            return Err(bad_value("$out and $merge are not supported with $text"));
        }
        let namespace = format!("{database}.{collection}");
        let Some(runtime) = database_for_read(state, &database).await else {
            return Ok(cursor_reply(0, &namespace, Vec::new(), true));
        };
        let store = runtime.store.clone();
        let coll = collection.clone();
        let docs = tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;
            let Some(c) = store.collection_at(&rtxn, &coll)? else {
                rtxn.commit()?;
                return Ok::<Vec<Value>, nonodb_core::Error>(Vec::new());
            };
            let Some(def) = c
                .search_index_defs_in(&rtxn)?
                .into_iter()
                .find(|def| def.kind == nonodb_core::SearchKind::Text)
            else {
                return Err(nonodb_core::Error::Query(
                    "text index required for $text query".into(),
                ));
            };
            let docs = c.search_text_index_in(
                &rtxn,
                &def.name,
                &query,
                &text_residual,
                0,
                None,
                nonodb_core::TextQueryMode::Mongo,
            )?;
            rtxn.commit()?;
            Ok(docs)
        })
        .await
        .map_err(|error| ApiError::internal(format!("aggregate $text join error: {error}")))??;
        let docs = if remaining.is_empty() {
            docs
        } else {
            nonodb_core::aggregate::run(
                docs,
                &remaining,
                nonodb_core::aggregate::MAX_AGGREGATE_DOCS,
            )?
        };
        if !cursor_options.requested {
            return Ok(cursor_reply(0, &namespace, values_to_bson(docs)?, true));
        }
        let cursor_state = cursor::State::new(
            format!("mongo:{connection_id}"),
            database,
            cursor::Kind::MaterializedAggregate {
                coll: collection,
                docs: Arc::new(docs),
                offset: 0,
            },
            None,
            Some(batch_size),
        );
        return aggregate_cursor_page(state, &runtime, &namespace, cursor_state, empty_first_batch)
            .await;
    }
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
    let namespace = format!("{database}.{collection}");
    let Some(runtime) = database_for_read(state, &database).await else {
        return Ok(cursor_reply(0, &namespace, Vec::new(), true));
    };
    let owner = format!("mongo:{connection_id}");
    if cursor_options.requested {
        if let Some((filter, stages)) = streamable_aggregate_pipeline(&pipeline) {
            let cursor_state = cursor::State::new(
                owner,
                database.clone(),
                cursor::Kind::Aggregate {
                    coll: collection,
                    filter,
                    stages,
                    comment,
                    max_time_ms,
                },
                None,
                Some(batch_size),
            );
            return aggregate_cursor_page(
                state,
                &runtime,
                &namespace,
                cursor_state,
                empty_first_batch,
            )
            .await;
        }
    }
    let store = runtime.store.clone();
    let coll = collection.clone();
    let docs = tokio::task::spawn_blocking(move || {
        let c = store.collection(&coll)?;
        c.aggregate_with_options(&pipeline, max_time_ms)
    })
    .await
    .map_err(|error| ApiError::internal(format!("aggregate join error: {error}")))??;
    if !cursor_options.requested {
        let docs = values_to_bson(docs)?;
        return Ok(cursor_reply(0, &namespace, docs, true));
    }
    let cursor_state = cursor::State::new(
        owner,
        database,
        cursor::Kind::MaterializedAggregate {
            coll: collection,
            docs: Arc::new(docs),
            offset: 0,
        },
        None,
        Some(batch_size),
    );
    aggregate_cursor_page(state, &runtime, &namespace, cursor_state, empty_first_batch).await
}

async fn aggregate_cursor_page(
    state: &super::AppState,
    runtime: &super::AppState,
    namespace: &str,
    mut cursor_state: cursor::State,
    empty_first_batch: bool,
) -> Result<Document, ApiError> {
    if empty_first_batch {
        let id = state
            .cursors
            .open(cursor_state)
            .map_err(|message| ApiError {
                code: "cursor".into(),
                message,
            })?;
        return Ok(cursor_reply(id as i64, namespace, Vec::new(), true));
    }
    let store = runtime.store.clone();
    let page = tokio::task::spawn_blocking(move || {
        super::cursor_page(&store, None, &mut cursor_state, None)
            .map(|(docs, has_more)| (docs, has_more, cursor_state))
    })
    .await
    .map_err(|error| ApiError::internal(format!("aggregate cursor join error: {error}")))??;
    let (docs, has_more, cursor_state) = page;
    let cursor_id = if has_more {
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
    Ok(cursor_reply(cursor_id, namespace, docs, true))
}

struct AggregateCursorOptions {
    requested: bool,
    batch_size: Option<usize>,
}

fn aggregate_cursor_options(body: &Document) -> Result<AggregateCursorOptions, ApiError> {
    match body.get("cursor") {
        None | Some(Bson::Null) => Ok(AggregateCursorOptions {
            requested: false,
            batch_size: None,
        }),
        Some(Bson::Document(cursor)) => {
            for key in cursor.keys() {
                if key != "batchSize" {
                    return Err(bad_value(format!(
                        "aggregate cursor option `{key}` is not supported"
                    )));
                }
            }
            let batch_size = match cursor.get("batchSize") {
                None => None,
                Some(value) => {
                    Some(bson_int(value).filter(|value| *value >= 0).ok_or_else(|| {
                        bad_value("aggregate cursor.batchSize must be a non-negative integer")
                    })? as usize)
                }
            };
            Ok(AggregateCursorOptions {
                requested: true,
                batch_size,
            })
        }
        Some(_) => Err(bad_value("aggregate cursor option must be a document")),
    }
}

fn streamable_aggregate_pipeline(pipeline: &[Value]) -> Option<(Value, Vec<Value>)> {
    let (filter, stages) = match pipeline.first().and_then(|stage| {
        stage
            .as_object()
            .filter(|object| object.len() == 1)
            .and_then(|object| object.get("$match"))
    }) {
        Some(filter) if filter.is_null() => (json!({}), &pipeline[1..]),
        Some(filter) => (filter.clone(), &pipeline[1..]),
        None => (json!({}), pipeline),
    };
    for stage in stages {
        let object = stage.as_object()?;
        if object.len() != 1 {
            return None;
        }
        let name = object.keys().next()?;
        if !matches!(name.as_str(), "$project" | "$addFields" | "$set" | "$unset") {
            return None;
        }
    }
    Some((filter, stages.to_vec()))
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
    let filter = document_field(body, "filter")?;
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
            "options": Bson::Document(Document::new()),
            "info": Bson::Document(bson::doc! {"readOnly": false}),
            "idIndex": Bson::Document(bson::doc! {
                "v": 2i32,
                "key": Bson::Document(bson::doc! {"_id": 1i32}),
                "name": "_id_",
            }),
        };
        if let Some(filter) = &filter {
            let value = bson_map::document_to_value(&doc).map_err(bad_value)?;
            if !nonodb_core::query::matches(&value, filter) {
                continue;
            }
        }
        if name_only {
            doc.remove("options");
            doc.remove("info");
            doc.remove("idIndex");
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
    body: &Document,
    principal: Option<&Principal>,
) -> Result<Document, ApiError> {
    let names = state.databases.names().await;
    let name_only = body.get_bool("nameOnly").unwrap_or(false);
    let filter = document_field(body, "filter")?;
    let mut databases = Vec::with_capacity(names.len());
    for name in names {
        if principal.is_some_and(|principal| !principal.can_see_database(&name)) {
            continue;
        }
        let mut doc = bson::doc! {
            "name": name,
            "sizeOnDisk": 0i64,
            "empty": false,
        };
        if let Some(filter) = &filter {
            let value = bson_map::document_to_value(&doc).map_err(bad_value)?;
            if !nonodb_core::query::matches(&value, filter) {
                continue;
            }
        }
        if name_only {
            doc.remove("sizeOnDisk");
            doc.remove("empty");
        }
        databases.push(Bson::Document(doc));
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
    let mut text_specs: Vec<nonodb_core::SearchIndexDef> = Vec::new();
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
        let text_fields: Vec<&String> = key
            .iter()
            .filter(|(_, direction)| matches!(direction, Bson::String(value) if value == "text"))
            .map(|(field, _)| field)
            .collect();
        if !text_fields.is_empty() {
            if text_fields.len() != key.len() {
                return Err(bad_value(
                    "NonoDB text indexes cannot mix text and non-text fields",
                ));
            }
            if !text_specs.is_empty() {
                return Err(bad_value(
                    "createIndexes can create at most one text index at a time",
                ));
            }
            for unsupported in [
                "partialFilterExpression",
                "collation",
                "expireAfterSeconds",
                "wildcardProjection",
                "language_override",
            ] {
                if spec.contains_key(unsupported) {
                    return Err(bad_value(format!(
                        "text index option `{unsupported}` is not supported by NonoDB"
                    )));
                }
            }
            if let Some(language) = spec.get("default_language") {
                if language.as_str() != Some("none") {
                    return Err(bad_value(
                        "only default_language: \"none\" is supported for text indexes",
                    ));
                }
            }
            if spec.get_bool("unique").unwrap_or(false) {
                return Err(bad_value("text indexes cannot be unique"));
            }
            if spec.get_bool("sparse").unwrap_or(false) {
                return Err(bad_value("sparse indexes are not supported by NonoDB"));
            }
            if spec.get_bool("hidden").unwrap_or(false) {
                return Err(bad_value("hidden indexes are not supported by NonoDB"));
            }
            let weights = match spec.get("weights") {
                Some(Bson::Document(weights)) => Some(weights),
                Some(_) => return Err(bad_value("text index weights must be a document")),
                None => None,
            };
            let mut fields = Vec::with_capacity(key.len());
            for field in &text_fields {
                let weight = match weights.and_then(|weights| weights.get(*field)) {
                    Some(value) => {
                        let Some(weight) = bson_number(value) else {
                            return Err(bad_value(format!(
                                "text index weight for `{field}` must be a number"
                            )));
                        };
                        if !weight.is_finite() || weight <= 0.0 {
                            return Err(bad_value(format!(
                                "text index weight for `{field}` must be positive and finite"
                            )));
                        }
                        weight
                    }
                    None => 1.0,
                };
                fields.push(nonodb_core::SearchField {
                    path: (*field).clone(),
                    weight,
                });
            }
            if let Some(weights) = weights {
                for field in weights.keys() {
                    if !key.contains_key(field) {
                        return Err(bad_value(format!(
                            "text index weight refers to unknown field `{field}`"
                        )));
                    }
                }
            }
            let default_name = text_fields
                .iter()
                .map(|field| format!("{field}_text"))
                .collect::<Vec<_>>()
                .join("_");
            let name = spec.get_str("name").unwrap_or(&default_name);
            if name.is_empty() {
                return Err(bad_value("index name cannot be empty"));
            }
            if name.len() > 127 || name.contains('\0') {
                return Err(bad_value(
                    "index name must be at most 127 bytes and must not contain NUL",
                ));
            }
            text_specs.push(nonodb_core::SearchIndexDef {
                name: name.to_string(),
                kind: nonodb_core::SearchKind::Text,
                fields,
            });
            continue;
        }

        let mut fields = Vec::with_capacity(key.len());
        let mut directions = Vec::with_capacity(key.len());
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
                Bson::String(value) if value == "text" => {
                    return Err(bad_value("text index fields must all use the text type"))
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
            directions.push(direction as i8);
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
        let default_name = default_parts.join("_");
        let name = spec.get_str("name").unwrap_or(&default_name);
        if name.is_empty() {
            return Err(bad_value("index name cannot be empty"));
        }
        if name.len() > 127 || name.contains('\0') {
            return Err(bad_value(
                "index name must be at most 127 bytes and must not contain NUL",
            ));
        }
        specs.push(nonodb_core::IndexSpec {
            fields,
            name: name.to_string(),
            directions,
            unique: spec.get_bool("unique").unwrap_or(false),
            include: Vec::new(),
            geo,
        });
    }
    if !text_specs.is_empty() && !specs.is_empty() {
        return Err(bad_value(
            "NonoDB cannot create text and non-text indexes in one createIndexes call",
        ));
    }
    let runtime = database_for_write(state, &database).await?;
    if let Some(def) = text_specs.pop() {
        let store = runtime.store.clone();
        let coll = collection.clone();
        let name = def.name.clone();
        let existing = tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;
            let Some(c) = store.collection_at(&rtxn, &coll)? else {
                rtxn.commit()?;
                return Ok::<Vec<nonodb_core::SearchIndexDef>, nonodb_core::Error>(Vec::new());
            };
            let defs = c.search_index_defs_in(&rtxn)?;
            rtxn.commit()?;
            Ok(defs
                .into_iter()
                .filter(|def| def.kind == nonodb_core::SearchKind::Text)
                .collect())
        })
        .await
        .map_err(|error| ApiError::internal(format!("createIndexes join error: {error}")))??;
        if existing.iter().any(|index| index.name != name) {
            return Err(bad_value(
                "only one text index is supported per collection; drop the existing text index first",
            ));
        }
        let (_, response) = super::submit_write(
            &runtime,
            WriteReq::CreateSearchIndex {
                coll: collection,
                index: def,
            },
            Durability::Full,
        )
        .await;
        let api = response.0;
        if let Some(error) = api.error {
            return Err(error);
        }
    } else {
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
    }
    Ok(bson::doc! {
        "createdCollectionAutomatically": false,
        "numIndexesBefore": 1i32,
        "numIndexesAfter": 1i32,
        "ok": 1.0,
    })
}

fn bson_number(value: &Bson) -> Option<f64> {
    match value {
        Bson::Int32(value) => Some(f64::from(*value)),
        Bson::Int64(value) => Some(*value as f64),
        Bson::Double(value) => Some(*value),
        _ => None,
    }
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
            return Ok::<
                Option<(Vec<nonodb_core::IndexDef>, Vec<nonodb_core::SearchIndexDef>)>,
                nonodb_core::Error,
            >(None);
        };
        let defs = c.index_defs_in(&rtxn)?;
        let search = c.search_index_defs_in(&rtxn)?;
        rtxn.commit()?;
        Ok(Some((defs, search)))
    })
    .await
    .map_err(|error| ApiError::internal(format!("listIndexes join error: {error}")))??;
    let Some((defs, search_defs)) = defs else {
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
        for (field, direction) in def.effective_fields().into_iter().zip(def.directions()) {
            if def.geo {
                key.insert(field, "2dsphere");
            } else {
                key.insert(field, i32::from(direction));
            }
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
    for def in search_defs
        .into_iter()
        .filter(|def| def.kind == nonodb_core::SearchKind::Text)
    {
        let mut key = Document::new();
        let mut weights = Document::new();
        let mut custom_weights = false;
        for field in &def.fields {
            key.insert(field.path.clone(), "text");
            weights.insert(field.path.clone(), field.weight);
            custom_weights |= (field.weight - 1.0).abs() > f64::EPSILON;
        }
        let mut doc = bson::doc! {
            "v": 2i32,
            "key": Bson::Document(key),
            "name": def.name,
            "ns": namespace.clone(),
            "default_language": "none",
            "textIndexVersion": 3i32,
        };
        if custom_weights {
            doc.insert("weights", Bson::Document(weights));
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
            return Ok::<
                Option<(Vec<nonodb_core::IndexDef>, Vec<nonodb_core::SearchIndexDef>)>,
                nonodb_core::Error,
            >(None);
        };
        let defs = c.index_defs_in(&rtxn)?;
        let search = c.search_index_defs_in(&rtxn)?;
        rtxn.commit()?;
        Ok(Some((defs, search)))
    })
    .await
    .map_err(|error| ApiError::internal(format!("dropIndexes join error: {error}")))??;
    let Some((defs, search_defs)) = defs else {
        return Err(ApiError {
            code: "not_found".into(),
            message: format!("ns not found: {database}.{collection}"),
        });
    };
    let text_defs: Vec<_> = search_defs
        .into_iter()
        .filter(|def| def.kind == nonodb_core::SearchKind::Text)
        .collect();
    let before = defs.len() as i32 + text_defs.len() as i32 + 1;
    let target = body
        .get("index")
        .ok_or_else(|| bad_value("dropIndexes requires an index name, key document, or `*`"))?;
    #[derive(Clone)]
    enum DropTarget {
        Secondary(String),
        Text(String),
    }
    let targets: Vec<DropTarget> = match target {
        Bson::String(name) if name == "*" => defs
            .iter()
            .map(|def| DropTarget::Secondary(def.name()))
            .chain(
                text_defs
                    .iter()
                    .map(|def| DropTarget::Text(def.name.clone())),
            )
            .collect(),
        Bson::String(name) => {
            if name == "_id_" {
                return Err(bad_value("the _id index cannot be dropped"));
            }
            if let Some(def) = defs.iter().find(|def| def.name() == *name) {
                vec![DropTarget::Secondary(def.name())]
            } else if let Some(def) = text_defs.iter().find(|def| def.name == *name) {
                vec![DropTarget::Text(def.name.clone())]
            } else {
                return Err(ApiError {
                    code: "index_not_found".into(),
                    message: format!("index not found with name [{name}]"),
                });
            }
        }
        Bson::Document(key) => {
            let mut fields = Vec::new();
            let mut text = false;
            for (field, direction) in key {
                match direction {
                    Bson::String(value) if value == "text" => text = true,
                    Bson::Int32(1)
                    | Bson::Int32(-1)
                    | Bson::Int64(1)
                    | Bson::Int64(-1)
                    | Bson::Double(1.0)
                    | Bson::Double(-1.0) => {}
                    Bson::String(value) if value == "2dsphere" => {}
                    _ => {
                        return Err(bad_value(
                            "index key direction must be 1, -1, 2dsphere, or text",
                        ))
                    }
                }
                fields.push(field.clone());
            }
            if text {
                let found = text_defs
                    .iter()
                    .find(|def| def.field_paths() == fields)
                    .ok_or_else(|| ApiError {
                        code: "index_not_found".into(),
                        message: "index not found".into(),
                    })?;
                vec![DropTarget::Text(found.name.clone())]
            } else {
                let found = defs
                    .iter()
                    .find(|def| def.effective_fields() == fields)
                    .ok_or_else(|| ApiError {
                        code: "index_not_found".into(),
                        message: "index not found".into(),
                    })?;
                vec![DropTarget::Secondary(found.name())]
            }
        }
        _ => return Err(bad_value("dropIndexes index must be a name, key, or `*`")),
    };
    for target in &targets {
        let request = match target {
            DropTarget::Secondary(name) => WriteReq::DropIndex {
                coll: collection.clone(),
                field: name.clone(),
            },
            DropTarget::Text(name) => WriteReq::DropSearchIndex {
                coll: collection.clone(),
                kind: nonodb_core::SearchKind::Text,
                name: name.clone(),
            },
        };
        let (_, response) = super::submit_write(&runtime, request, Durability::Full).await;
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

fn value_contains_text(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            map.keys().any(|key| key == "$text") || map.values().any(value_contains_text)
        }
        Value::Array(items) => items.iter().any(value_contains_text),
        _ => false,
    }
}

fn parse_text_spec(value: &Value) -> Result<String, ApiError> {
    let Some(spec) = value.as_object() else {
        return Err(bad_value("$text must be a document"));
    };
    let Some(search) = spec.get("$search").and_then(Value::as_str) else {
        return Err(bad_value("$text requires a string $search"));
    };
    for (key, value) in spec {
        match key.as_str() {
            "$search" => {}
            "$language" => {
                if value.as_str() != Some("none") {
                    return Err(bad_value("only $language: \"none\" is supported for $text"));
                }
            }
            "$caseSensitive" | "$diacriticSensitive" => {
                if value.as_bool() != Some(false) {
                    return Err(bad_value(format!(
                        "$text option `{key}` can only be false in NonoDB"
                    )));
                }
            }
            _ => return Err(bad_value(format!("unsupported $text option `{key}`"))),
        }
    }
    Ok(search.to_string())
}

fn extract_text_filter(filter: &Value) -> Result<(Option<String>, Value), ApiError> {
    let Value::Object(object) = filter else {
        return Ok((None, filter.clone()));
    };
    let mut residual = serde_json::Map::new();
    let mut text = None;
    for (key, value) in object {
        match key.as_str() {
            "$text" => {
                if text.is_some() {
                    return Err(bad_value("$text can appear only once in a filter"));
                }
                text = Some(parse_text_spec(value)?);
            }
            "$and" => {
                let Value::Array(items) = value else {
                    return Err(bad_value("$and must be an array"));
                };
                let mut residuals = Vec::with_capacity(items.len());
                for item in items {
                    let (found, residual) = extract_text_filter(item)?;
                    if let Some(found) = found {
                        if text.is_some() {
                            return Err(bad_value("$text can appear only once in a filter"));
                        }
                        text = Some(found);
                    }
                    residuals.push(residual);
                }
                residual.insert("$and".to_string(), Value::Array(residuals));
            }
            "$or" | "$nor" => {
                if value_contains_text(value) {
                    return Err(bad_value(
                        "$text is not supported inside $or or $nor by NonoDB",
                    ));
                }
                residual.insert(key.clone(), value.clone());
            }
            _ => {
                if value_contains_text(value) {
                    return Err(bad_value(
                        "$text is only supported at the filter top level or inside $and",
                    ));
                }
                residual.insert(key.clone(), value.clone());
            }
        }
    }
    Ok((text, Value::Object(residual)))
}

fn validate_text_sort(body: &Document) -> Result<(), ApiError> {
    let Some(value) = body.get("sort") else {
        return Ok(());
    };
    let Bson::Document(sort) = value else {
        return Err(bad_value("sort must be a document"));
    };
    for (field, direction) in sort {
        let Bson::Document(meta) = direction else {
            return Err(bad_value(format!(
                "text queries can only sort by $meta: \"textScore\" (field `{field}`)"
            )));
        };
        if meta.get_str("$meta").ok() != Some("textScore") {
            return Err(bad_value(format!(
                "text queries can only sort by $meta: \"textScore\" (field `{field}`)"
            )));
        }
    }
    Ok(())
}

fn projection_uses_text_score(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            if map.get("$meta").and_then(Value::as_str) == Some("textScore") {
                return true;
            }
            map.values().any(projection_uses_text_score)
        }
        Value::Array(items) => items.iter().any(projection_uses_text_score),
        _ => false,
    }
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
    let raw_filter = document_field(body, "filter")?.unwrap_or_else(|| json!({}));
    let (text_query, filter) = extract_text_filter(&raw_filter)?;
    let projection = document_field(body, "projection")?;
    let hint = if text_query.is_some() {
        if body.contains_key("hint") {
            return Err(bad_value("$text queries do not support hint in NonoDB"));
        }
        None
    } else if let Some(value) = body.get("hint") {
        Some(bson_map::bson_to_value(value).map_err(bad_value)?)
    } else {
        None
    };
    let (sort, ascending) = if text_query.is_some() {
        validate_text_sort(body)?;
        (Vec::new(), true)
    } else {
        let sort = sort_fields(body)?;
        let ascending = id_sort_ascending(&sort)?;
        (sort, ascending)
    };
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
    if text_query.is_some() && max_time_ms.is_some() {
        return Err(bad_value("maxTimeMS is not supported with $text by NonoDB"));
    }
    let namespace = format!("{database}.{collection}");
    let owner = format!("mongo:{connection_id}");
    let Some(runtime) = database_for_read(state, &database).await else {
        return Ok(cursor_reply(0, &namespace, Vec::new(), true));
    };
    let store = runtime.store.clone();
    if let Some(query) = text_query {
        let coll = collection.clone();
        let search_store = store.clone();
        let docs = tokio::task::spawn_blocking(move || {
            let rtxn = search_store.read_txn()?;
            let Some(c) = search_store.collection_at(&rtxn, &coll)? else {
                rtxn.commit()?;
                return Ok::<Vec<Value>, nonodb_core::Error>(Vec::new());
            };
            let Some(def) = c
                .search_index_defs_in(&rtxn)?
                .into_iter()
                .find(|def| def.kind == nonodb_core::SearchKind::Text)
            else {
                return Err(nonodb_core::Error::Query(
                    "text index required for $text query".into(),
                ));
            };
            let docs = c.search_text_index_in(
                &rtxn,
                &def.name,
                &query,
                &filter,
                0,
                None,
                nonodb_core::TextQueryMode::Mongo,
            )?;
            rtxn.commit()?;
            Ok(docs)
        })
        .await
        .map_err(|error| ApiError::internal(format!("find $text join error: {error}")))??;
        let uses_meta = projection.as_ref().is_some_and(projection_uses_text_score);
        let docs: Vec<Value> = docs
            .into_iter()
            .map(|mut doc| {
                let score = doc.get("_score").cloned();
                if let Some(object) = doc.as_object_mut() {
                    object.remove("_score");
                }
                if let Some(projection) = projection.as_ref() {
                    if uses_meta {
                        if let (Some(score), Some(object)) = (score, doc.as_object_mut()) {
                            object.insert("_score".to_string(), score);
                        }
                    }
                    doc = nonodb_core::project_document(&doc, Some(projection));
                }
                if let Some(object) = doc.as_object_mut() {
                    object.remove("_score");
                }
                doc
            })
            .skip(skip)
            .take(limit.unwrap_or(usize::MAX))
            .collect();
        let docs = if single_batch {
            docs.into_iter().take(batch_size).collect()
        } else {
            docs
        };
        let mut cursor_state = cursor::State::new(
            owner,
            database,
            cursor::Kind::MaterializedAggregate {
                coll: collection,
                docs: Arc::new(docs),
                offset: 0,
            },
            None,
            Some(batch_size),
        );
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
        let page = tokio::task::spawn_blocking(move || {
            super::cursor_page(&store, None, &mut cursor_state, None)
                .map(|(docs, has_more)| (docs, has_more, cursor_state))
        })
        .await
        .map_err(|error| ApiError::internal(format!("find $text page join error: {error}")))??;
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
        return Ok(cursor_reply(
            cursor_id,
            &namespace,
            values_to_bson(docs)?,
            true,
        ));
    }

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

/// MongoDB drivers send a regex literal as a BSON regular-expression value.
/// NonoDB supports those values as read predicates, but does not store them, so
/// reject them explicitly in documents, updates, projections, and non-filter
/// aggregation positions.
fn is_bson_regex(value: &Value) -> bool {
    value.as_object().is_some_and(|object| {
        object.len() == 1
            && object
                .get("$regularExpression")
                .is_some_and(Value::is_object)
    })
}

fn reject_stored_bson_regex(value: &Value) -> Result<(), ApiError> {
    if is_bson_regex(value) {
        return Err(bad_value(
            "stored BSON regular-expression values are not supported; use the $regex operator",
        ));
    }
    match value {
        Value::Array(items) => {
            for item in items {
                reject_stored_bson_regex(item)?;
            }
        }
        Value::Object(object) => {
            for item in object.values() {
                reject_stored_bson_regex(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn reject_aggregate_stored_regex(value: &Value) -> Result<(), ApiError> {
    if is_bson_regex(value) {
        return Err(bad_value(
            "stored BSON regular-expression values are not supported; use the $regex operator",
        ));
    }
    match value {
        Value::Array(items) => {
            for item in items {
                reject_aggregate_stored_regex(item)?;
            }
        }
        Value::Object(object) => {
            for (key, item) in object {
                if key != "$match" {
                    reject_aggregate_stored_regex(item)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn command_field(body: &Document, key: &str) -> Result<Option<Value>, ApiError> {
    body.get(key)
        .map(|value| bson_map::bson_to_value(value).map_err(bad_value))
        .transpose()
}

fn validate_mongo_bson_values(command: &str, body: &Document) -> Result<(), ApiError> {
    match command {
        "insert" => {
            if let Some(Value::Array(documents)) = command_field(body, "documents")? {
                for document in &documents {
                    reject_stored_bson_regex(document)?;
                }
            }
        }
        "update" => {
            if let Some(Value::Array(updates)) = command_field(body, "updates")? {
                for update in &updates {
                    let Some(spec) = update.as_object() else {
                        continue;
                    };
                    if let Some(update) = spec.get("u") {
                        reject_stored_bson_regex(update)?;
                    }
                }
            }
        }
        "find" => {
            if let Some(projection) = command_field(body, "projection")? {
                reject_stored_bson_regex(&projection)?;
            }
        }
        "findAndModify" | "findandmodify" => {
            if let Some(update) = command_field(body, "update")? {
                reject_stored_bson_regex(&update)?;
            }
            if let Some(fields) = command_field(body, "fields")? {
                reject_stored_bson_regex(&fields)?;
            }
        }
        "aggregate" => {
            if let Some(pipeline) = command_field(body, "pipeline")? {
                reject_aggregate_stored_regex(&pipeline)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_mongo_options(command: &str, body: &Document) -> Result<(), ApiError> {
    match command {
        "find" => {
            reject_options(
                body,
                &["collation", "let", "min", "max"],
                "not supported by the wire find command",
            )?;
            reject_truthy_options(
                body,
                &[
                    "returnKey",
                    "showRecordId",
                    "tailable",
                    "awaitData",
                    "noCursorTimeout",
                    "oplogReplay",
                ],
                "not supported by the wire find command",
            )?;
            validate_concerns(body)
        }
        "aggregate" => {
            reject_options(
                body,
                &["collation", "let", "hint"],
                "not supported by the wire aggregate command",
            )?;
            reject_truthy_options(
                body,
                &["explain"],
                "not supported by the wire aggregate command",
            )?;
            validate_concerns(body)
        }
        "count" => {
            reject_options(
                body,
                &["collation", "hint", "let"],
                "not supported by the wire count command",
            )?;
            validate_concerns(body)
        }
        "distinct" => {
            reject_options(
                body,
                &["collation", "let"],
                "not supported by the wire distinct command",
            )?;
            validate_concerns(body)
        }
        "insert" | "update" | "delete" => {
            reject_truthy_options(
                body,
                &["bypassDocumentValidation"],
                "not supported by the wire write commands",
            )?;
            validate_concerns(body)
        }
        "findAndModify" | "findandmodify" => {
            reject_options(
                body,
                &["collation", "hint", "let"],
                "not supported by the wire findAndModify command",
            )?;
            reject_truthy_options(
                body,
                &["bypassDocumentValidation"],
                "not supported by the wire findAndModify command",
            )?;
            validate_concerns(body)
        }
        "createIndexes" | "dropIndexes" => {
            reject_options(
                body,
                &["commitQuorum", "maxTimeMS"],
                "not supported by the wire index commands",
            )?;
            validate_concerns(body)
        }
        "listIndexes" | "listCollections" => {
            reject_cursor_batching(body, command)?;
            validate_concerns(body)
        }
        "listDatabases" => validate_concerns(body),
        "create" | "drop" | "dropDatabase" => validate_concerns(body),
        "getMore" | "killCursors" | "endSessions" | "refreshSessions" => Ok(()),
        _ => Ok(()),
    }
}

fn reject_options(body: &Document, names: &[&str], reason: &str) -> Result<(), ApiError> {
    for name in names {
        if body.contains_key(*name) {
            return Err(bad_value(format!("option `{name}` is {reason}")));
        }
    }
    Ok(())
}

fn reject_truthy_options(body: &Document, names: &[&str], reason: &str) -> Result<(), ApiError> {
    for name in names {
        let Some(value) = body.get(*name) else {
            continue;
        };
        let truthy = match value {
            Bson::Boolean(value) => *value,
            Bson::Int32(value) => *value != 0,
            Bson::Int64(value) => *value != 0,
            Bson::Double(value) => *value != 0.0,
            Bson::Null => false,
            _ => true,
        };
        if truthy {
            return Err(bad_value(format!("option `{name}` is {reason}")));
        }
    }
    Ok(())
}

fn reject_cursor_batching(body: &Document, command: &str) -> Result<(), ApiError> {
    let Some(Bson::Document(cursor)) = body.get("cursor") else {
        if body.contains_key("cursor") {
            return Err(bad_value("cursor must be a document"));
        }
        return Ok(());
    };
    if let Some(batch_size) = cursor.get("batchSize").and_then(bson_int) {
        if batch_size > 0 {
            return Err(bad_value(format!(
                "cursor.batchSize is not supported by the wire {command} command"
            )));
        }
    }
    Ok(())
}

fn validate_concerns(body: &Document) -> Result<(), ApiError> {
    if let Some(Bson::Document(concern)) = body.get("readConcern") {
        if let Ok(level) = concern.get_str("level") {
            if !matches!(level, "local" | "available") {
                return Err(bad_value(format!(
                    "readConcern `{level}` is not supported; use `local`"
                )));
            }
        }
    }
    if let Some(Bson::Document(concern)) = body.get("writeConcern") {
        if let Some(w) = concern.get("w") {
            let supported = match w {
                Bson::Int32(value) => *value == 1,
                Bson::Int64(value) => *value == 1,
                Bson::Double(value) => *value == 1.0,
                Bson::String(value) => value == "1",
                _ => false,
            };
            if !supported {
                return Err(bad_value(
                    "only writeConcern w:1 is supported by the wire listener",
                ));
            }
        }
        if concern
            .get("wtimeout")
            .and_then(bson_int)
            .is_some_and(|value| value != 0)
        {
            return Err(bad_value("writeConcern wtimeout is not supported"));
        }
    }
    Ok(())
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

fn new_object_id() -> Value {
    json!({"$oid": bson::oid::ObjectId::new().to_hex()})
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
    fn unsupported_wire_options_fail_loudly() {
        for (command, body, option) in [
            (
                "find",
                bson::doc! { "find": "items", "collation": { "locale": "en" } },
                "collation",
            ),
            (
                "find",
                bson::doc! { "find": "items", "returnKey": true },
                "returnKey",
            ),
            (
                "insert",
                bson::doc! { "insert": "items", "documents": [], "bypassDocumentValidation": true },
                "bypassDocumentValidation",
            ),
            (
                "findAndModify",
                bson::doc! { "findAndModify": "items", "query": {}, "collation": { "locale": "en" } },
                "collation",
            ),
            (
                "find",
                bson::doc! { "find": "items", "writeConcern": { "w": "majority" } },
                "writeConcern",
            ),
            (
                "find",
                bson::doc! { "find": "items", "readConcern": { "level": "snapshot" } },
                "readConcern",
            ),
        ] {
            let error = validate_mongo_options(command, &body).unwrap_err();
            assert_eq!(error.code, "invalid_query", "{option}");
            assert!(error.message.contains(option), "{}", error.message);
        }

        assert!(validate_mongo_options(
            "find",
            &bson::doc! {
                "find": "items",
                "readConcern": { "level": "local" },
                "writeConcern": { "w": 1 },
            }
        )
        .is_ok());
        assert!(validate_mongo_options(
            "aggregate",
            &bson::doc! { "aggregate": "items", "pipeline": [], "cursor": {} }
        )
        .is_ok());
        assert!(validate_mongo_options(
            "listCollections",
            &bson::doc! { "listCollections": 1, "filter": { "name": "items" } }
        )
        .is_ok());
    }

    #[test]
    fn aggregate_cursor_support_is_explicit() {
        let streamable = vec![
            json!({"$match": {"tenant": "acme"}}),
            json!({"$project": {"name": 1}}),
        ];
        let (filter, stages) = streamable_aggregate_pipeline(&streamable).unwrap();
        assert_eq!(filter, json!({"tenant": "acme"}));
        assert_eq!(stages.len(), 1);

        assert!(streamable_aggregate_pipeline(&[
            json!({"$match": {}}),
            json!({"$group": {"_id": "$tenant"}}),
        ])
        .is_none());

        let options = aggregate_cursor_options(
            &bson::doc! { "aggregate": "items", "cursor": { "batchSize": 7 } },
        )
        .unwrap();
        assert!(options.requested);
        assert_eq!(options.batch_size, Some(7));
        assert!(aggregate_cursor_options(
            &bson::doc! { "aggregate": "items", "cursor": { "batchSize": -1 } }
        )
        .is_err());
        assert!(aggregate_cursor_options(
            &bson::doc! { "aggregate": "items", "cursor": { "collation": {} } }
        )
        .is_err());
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
