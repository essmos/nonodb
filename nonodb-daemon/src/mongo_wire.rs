//! Experimental MongoDB wire-protocol frontend.
//!
//! This is deliberately a small compatibility layer: it accepts the MongoDB
//! handshake and reports unsupported commands instead of silently behaving
//! differently. Command dispatch to NonoDB is added incrementally.

use std::io;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use bson::{Bson, Document};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;

const OP_REPLY: i32 = 1;
const OP_QUERY: i32 = 2004;
const OP_MSG: i32 = 2013;
const MAX_MESSAGE_BYTES: usize = 48_000_000;
const MAX_BSON_BYTES: i32 = 16 * 1024 * 1024;
const MAX_WRITE_BATCH_SIZE: i32 = 100_000;
const MAX_WIRE_VERSION: i32 = 9;
static NEXT_RESPONSE_ID: AtomicI32 = AtomicI32::new(1);

struct Message {
    request_id: i32,
    op_code: i32,
    body: Document,
}

/// Starts the experimental MongoDB wire listener.
pub async fn serve(
    addr: &str,
    mut shutdown: watch::Receiver<bool>,
    drain_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?.to_string();
    tracing::warn!(
        %addr,
        bound = %bound,
        "experimental MongoDB wire listener enabled; only handshake commands are implemented"
    );

    let mut connections = JoinSet::new();
    loop {
        let (stream, peer) = tokio::select! {
            _ = super::wait_for_shutdown(&mut shutdown) => break,
            accepted = listener.accept() => accepted?,
        };
        let shutdown = shutdown.clone();
        connections.spawn(async move {
            if let Err(error) = handle_connection(stream, shutdown).await {
                tracing::debug!(?error, %peer, "Mongo wire connection closed");
            }
        });
    }
    drop(listener);
    super::drain_connections(connections, drain_timeout, "mongo-wire").await;
    Ok(())
}

async fn handle_connection<S>(mut stream: S, mut shutdown: watch::Receiver<bool>) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let message = tokio::select! {
            _ = super::wait_for_shutdown(&mut shutdown) => return Ok(()),
            message = read_message(&mut stream) => message?,
        };
        let Some(message) = message else {
            return Ok(());
        };
        let response = command_reply(&message.body);
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
                while position < sequence_end && payload[position] != 0 {
                    position += 1;
                }
                if position >= sequence_end {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "OP_MSG document sequence identifier is invalid",
                    ));
                }
                position += 1;
                while position < sequence_end {
                    let (_, next) = read_bson_at(payload, position, sequence_end)?;
                    position = next;
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unsupported OP_MSG section kind",
                ));
            }
        }
    }
    body.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "OP_MSG has no body"))
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
