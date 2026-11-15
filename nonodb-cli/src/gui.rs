//! Local GUI server with embedded React assets and a same-origin API proxy.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::time::Duration;

use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "$CARGO_MANIFEST_DIR/../gui/dist"]
struct Assets;

#[derive(Clone)]
struct ClusterNode {
    name: String,
    url: String,
}

#[derive(Clone)]
enum Target {
    #[cfg(unix)]
    Unix(String),
    Tcp(String),
}

trait ReadWrite: Read + Write {}
impl<T: Read + Write> ReadWrite for T {}

type Upstream = Box<dyn ReadWrite>;

#[derive(Debug)]
struct IncomingRequest {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

pub fn run(args: &[String]) -> Result<(), String> {
    let mut host = "127.0.0.1".to_string();
    let mut port = 8787u16;
    let mut open = true;
    let mut allow_remote = false;
    let mut url: Option<String> = None;
    let mut cluster_spec: Option<String> = None;
    let mut progress_file: Option<PathBuf> = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--host" => {
                host = args
                    .get(i + 1)
                    .ok_or("--host wymaga adresu, np. 127.0.0.1 albo 0.0.0.0")?
                    .clone();
                i += 2;
            }
            "--port" => {
                port = args
                    .get(i + 1)
                    .ok_or("--port wymaga numeru")?
                    .parse()
                    .map_err(|e| format!("port: {e}"))?;
                i += 2;
            }
            "--allow-remote" => {
                allow_remote = true;
                i += 1;
            }
            "--url" => {
                url = Some(
                    args.get(i + 1)
                        .ok_or("--url wymaga adresu, np. http://127.0.0.1:7879")?
                        .clone(),
                );
                i += 2;
            }
            "--cluster" => {
                cluster_spec = Some(
                    args.get(i + 1)
                        .ok_or("--cluster wymaga listy name=url,name=url")?
                        .clone(),
                );
                i += 2;
            }
            "--progress-file" => {
                progress_file = Some(PathBuf::from(
                    args.get(i + 1).ok_or("--progress-file wymaga ścieżki")?,
                ));
                i += 2;
            }
            "--no-open" => {
                open = false;
                i += 1;
            }
            other => return Err(format!("nieznana opcja gui: {other}")),
        }
    }

    let loopback = matches!(host.as_str(), "127.0.0.1" | "localhost" | "::1" | "[::1]");
    if !loopback && !allow_remote {
        return Err(format!(
            "odmowa nasłuchu na {host}: użyj --allow-remote, aby świadomie wystawić GUI"
        ));
    }
    if !loopback {
        eprintln!(
            "UWAGA: GUI nasłuchuje na {host}:{port}. Wystawiaj je tylko za TLS, auth i zaufanym reverse proxy."
        );
    }

    let cluster = match cluster_spec.or_else(|| std::env::var("NONODB_GUI_CLUSTER").ok()) {
        Some(raw) if !raw.trim().is_empty() => parse_cluster(&raw)?,
        _ => Vec::new(),
    };
    let progress_file = progress_file.or_else(|| {
        std::env::var("NONODB_GUI_TEST_PROGRESS")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
    });
    let target = target(url.as_deref())?;
    let listener = TcpListener::bind((host.as_str(), port))
        .map_err(|e| format!("cannot bind {host}:{port}: {e}"))?;
    let browser_host = match host.as_str() {
        "0.0.0.0" | "[::]" | "::" => "127.0.0.1",
        other => other,
    };
    let url = format!("http://{browser_host}:{port}/");
    println!("NonoDB GUI: {url}");
    println!("Press Ctrl+C to stop.");
    if open {
        open_browser(&url);
    }
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let target = target.clone();
                let cluster = cluster.clone();
                let progress_file = progress_file.clone();
                std::thread::spawn(move || {
                    if let Err(err) = handle(stream, &target, &cluster, progress_file.as_deref()) {
                        eprintln!("gui request: {err}");
                    }
                });
            }
            Err(err) => eprintln!("gui accept: {err}"),
        }
    }
    Ok(())
}

fn parse_cluster(raw: &str) -> Result<Vec<ClusterNode>, String> {
    let mut nodes = Vec::new();
    for (index, part) in raw
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .enumerate()
    {
        let (name, url) = match part.split_once('=') {
            Some((name, url)) => (name.trim().to_string(), url.trim().to_string()),
            None => (format!("node{}", index + 1), part.to_string()),
        };
        if name.is_empty() || url.is_empty() {
            return Err(format!("invalid --cluster entry: {part}"));
        }
        if !url.starts_with("http://") {
            return Err(format!(
                "cluster node {name}: only http:// URLs are supported"
            ));
        }
        nodes.push(ClusterNode { name, url });
    }
    if nodes.is_empty() {
        return Err("--cluster requires at least one name=url entry".into());
    }
    Ok(nodes)
}

fn target(url: Option<&str>) -> Result<Target, String> {
    if let Some(url) = url {
        let addr = url
            .strip_prefix("http://")
            .ok_or("--url must start with http://")?
            .trim_end_matches('/')
            .to_string();
        return Ok(Target::Tcp(addr));
    }
    #[cfg(unix)]
    {
        Ok(Target::Unix(
            std::env::var("NONODB_SOCK").unwrap_or_else(|_| "/run/nonodb.sock".into()),
        ))
    }
    #[cfg(not(unix))]
    {
        let url = std::env::var("NONODB_URL").unwrap_or_else(|_| "http://127.0.0.1:7878".into());
        target(Some(&url))
    }
}

fn handle(
    mut client: TcpStream,
    target: &Target,
    cluster: &[ClusterNode],
    progress_file: Option<&Path>,
) -> Result<(), String> {
    client
        .set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(|e| e.to_string())?;
    client
        .set_write_timeout(Some(Duration::from_secs(300)))
        .map_err(|e| e.to_string())?;
    let request = read_request(&mut client)?;

    if request.path == "/gui/cluster/status" {
        return serve_cluster(client, cluster, target);
    }
    if request.path == "/gui/test/progress" {
        return serve_test_progress(client, progress_file);
    }
    if request.path == "/gui/session" {
        let token = std::env::var("NONODB_TOKEN").unwrap_or_default();
        let body = serde_json::json!({ "token": token }).to_string();
        return write_response(
            &mut client,
            200,
            "application/json; charset=utf-8",
            body.as_bytes(),
            false,
        );
    }
    let plain_path = request.path.split('?').next().unwrap_or("");
    let segments: Vec<&str> = plain_path.trim_start_matches('/').split('/').collect();
    let database_scoped = segments.len() >= 3
        && !segments[0].is_empty()
        && segments[0] != "v1"
        && segments[1] == "v1";
    if plain_path == "/v1/watch" || (database_scoped && segments[2] == "watch") {
        return proxy_stream(client, target, &request);
    }
    if plain_path == "/v1/exec"
        || plain_path == "/healthz"
        || plain_path == "/v1/server"
        || plain_path == "/v1/network"
        || plain_path.starts_with("/v1/network/")
        || plain_path == "/readyz"
        || plain_path == "/metrics"
        || plain_path == "/metrics/prometheus"
        || plain_path == "/v1/auth"
        || plain_path.starts_with("/v1/auth/")
        || plain_path == "/v1/backups"
        || plain_path.starts_with("/v1/backups/")
        || plain_path == "/v1/import"
        || plain_path.starts_with("/v1/import/")
        || plain_path == "/v1/replication"
        || plain_path.starts_with("/v1/replication/")
        || (database_scoped && segments[2] == "replication")
        || (database_scoped && segments[2] == "exec")
    {
        return proxy(client, target, &request);
    }
    if plain_path.starts_with("/v1/") {
        return write_response(
            &mut client,
            501,
            "text/plain; charset=utf-8",
            b"unknown API endpoint\n",
            false,
        );
    }
    serve_asset(client, &request.path)
}

fn read_request(client: &mut TcpStream) -> Result<IncomingRequest, String> {
    let mut reader = BufReader::new(client.try_clone().map_err(|e| e.to_string())?);
    let mut line = Vec::new();
    reader
        .read_until(b'\n', &mut line)
        .map_err(|e| e.to_string())?;
    let line = String::from_utf8_lossy(&line);
    let mut parts = line.split_whitespace();
    let method = parts.next().ok_or("missing method")?.to_string();
    let path = parts.next().ok_or("missing path")?.to_string();

    let mut headers = Vec::new();
    loop {
        let mut raw = Vec::new();
        reader
            .read_until(b'\n', &mut raw)
            .map_err(|e| e.to_string())?;
        if raw == b"\r\n" || raw == b"\n" || raw.is_empty() {
            break;
        }
        let text = String::from_utf8_lossy(&raw);
        if let Some((name, value)) = text.split_once(':') {
            headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
        }
    }

    let content_length = headers
        .iter()
        .find(|(name, _)| name == "content-length")
        .and_then(|(_, value)| value.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body).map_err(|e| e.to_string())?;
    }
    Ok(IncomingRequest {
        method,
        path,
        headers,
        body,
    })
}

fn serve_test_progress(mut client: TcpStream, progress_file: Option<&Path>) -> Result<(), String> {
    let progress = progress_file
        .and_then(|path| std::fs::read(path).ok())
        .and_then(|raw| serde_json::from_slice::<serde_json::Value>(&raw).ok());
    let body = serde_json::to_vec(&serde_json::json!({
        "ok": true,
        "progress": progress,
    }))
    .map_err(|e| e.to_string())?;
    write_response(
        &mut client,
        200,
        "application/json; charset=utf-8",
        &body,
        false,
    )
}

fn serve_cluster(
    mut client: TcpStream,
    nodes: &[ClusterNode],
    target: &Target,
) -> Result<(), String> {
    let token = std::env::var("NONODB_CLUSTER_TOKEN")
        .or_else(|_| std::env::var("NONODB_TOKEN"))
        .ok();
    let mut merged = nodes.to_vec();
    for node in dynamic_cluster_nodes(target, token.as_deref()) {
        if !merged
            .iter()
            .any(|existing| existing.name == node.name || existing.url == node.url)
        {
            merged.push(node);
        }
    }
    let nodes = merged.as_slice();
    let payload = if nodes.is_empty() {
        serde_json::json!({
            "ok": true,
            "nodes": [],
            "message": "cluster view is not configured; start gui with --cluster name=url,name=url",
        })
    } else {
        let probed: Vec<serde_json::Value> = std::thread::scope(|scope| {
            let handles: Vec<_> = nodes
                .iter()
                .map(|node| {
                    let token = token.as_deref();
                    scope.spawn(move || probe_node(node, token))
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| {
                    handle.join().unwrap_or_else(
                        |_| serde_json::json!({"ok": false, "error": "probe panicked"}),
                    )
                })
                .collect()
        });
        serde_json::json!({
            "ok": true,
            "nodes": probed,
            "generated_at_ms": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        })
    };
    let body = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;
    write_response(
        &mut client,
        200,
        "application/json; charset=utf-8",
        &body,
        false,
    )
}

fn dynamic_cluster_nodes(target: &Target, token: Option<&str>) -> Vec<ClusterNode> {
    let Ok(status) = http_json_target(target, "/v1/replication/status", token) else {
        return Vec::new();
    };
    let Some(acks) = status
        .pointer("/result/acks")
        .and_then(serde_json::Value::as_array)
    else {
        return Vec::new();
    };
    acks.iter()
        .filter_map(|ack| {
            let name = ack.get("node").and_then(serde_json::Value::as_str)?;
            let url = ack.get("url").and_then(serde_json::Value::as_str)?;
            if !url.starts_with("http://") {
                return None;
            }
            Some(ClusterNode {
                name: name.to_string(),
                url: url.trim_end_matches('/').to_string(),
            })
        })
        .collect()
}

fn http_json_target(
    target: &Target,
    path: &str,
    token: Option<&str>,
) -> Result<serde_json::Value, String> {
    let mut stream = connect(target)?;
    let mut request = format!(
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\nConnection: close\r\n"
    );
    if let Some(token) = token.filter(|token| !token.is_empty()) {
        request.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|e| e.to_string())?;
    parse_http_json(&response)
}

fn parse_http_json(response: &[u8]) -> Result<serde_json::Value, String> {
    let split = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| "malformed HTTP response".to_string())?;
    let head = String::from_utf8_lossy(&response[..split]);
    let status_code = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| "missing HTTP status".to_string())?;
    if status_code != 200 {
        return Err(format!("HTTP {status_code}"));
    }
    let mut body = response[split + 4..].to_vec();
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        body = decode_chunked(&body)?;
    }
    serde_json::from_slice(&body).map_err(|e| format!("invalid JSON: {e}"))
}

fn probe_node(node: &ClusterNode, token: Option<&str>) -> serde_json::Value {
    let status = match http_json(
        &format!("{}/v1/replication/status", node.url.trim_end_matches('/')),
        token,
    ) {
        Ok(value) => value,
        Err(err) => {
            return serde_json::json!({
                "name": node.name,
                "url": node.url,
                "ok": false,
                "error": err,
            });
        }
    };
    let metrics = http_json(
        &format!("{}/metrics", node.url.trim_end_matches('/')),
        token,
    )
    .ok();
    let status = status.get("result").cloned().unwrap_or(status);
    serde_json::json!({
        "name": node.name,
        "url": node.url,
        "ok": true,
        "status": status,
        "metrics": metrics,
    })
}

fn http_json(url: &str, token: Option<&str>) -> Result<serde_json::Value, String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| "only http:// cluster URLs are supported".to_string())?;
    let (authority, path) = match rest.split_once('/') {
        Some((authority, path)) => (authority, format!("/{path}")),
        None => (rest, "/".to_string()),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (
            host.trim_matches(['[', ']']),
            port.parse::<u16>().map_err(|e| e.to_string())?,
        ),
        None => (authority, 80),
    };
    let mut addrs = (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("resolve {host}:{port}: {e}"))?;
    let addr = addrs
        .next()
        .ok_or_else(|| format!("resolve {host}:{port}: no address"))?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .map_err(|e| format!("connect {authority}: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .map_err(|e| e.to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_secs(3)))
        .map_err(|e| e.to_string())?;
    let mut request = format!(
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nAccept: application/json\r\nConnection: close\r\n"
    );
    if let Some(token) = token.filter(|token| !token.is_empty()) {
        request.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|e| e.to_string())?;
    let split = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| "malformed HTTP response".to_string())?;
    let head = String::from_utf8_lossy(&response[..split]);
    let status_code = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| "missing HTTP status".to_string())?;
    if status_code != 200 {
        return Err(format!("HTTP {status_code}"));
    }
    let mut body = response[split + 4..].to_vec();
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        body = decode_chunked(&body)?;
    }
    serde_json::from_slice(&body).map_err(|e| format!("invalid JSON: {e}"))
}

fn decode_chunked(body: &[u8]) -> Result<Vec<u8>, String> {
    let mut decoded = Vec::new();
    let mut offset = 0;
    loop {
        let line_end = body[offset..]
            .windows(2)
            .position(|window| window == b"\r\n")
            .map(|pos| offset + pos)
            .ok_or_else(|| "malformed chunked response".to_string())?;
        let line = String::from_utf8_lossy(&body[offset..line_end]);
        let size = usize::from_str_radix(line.split(';').next().unwrap_or("").trim(), 16)
            .map_err(|e| format!("invalid chunk size: {e}"))?;
        offset = line_end + 2;
        if size == 0 {
            return Ok(decoded);
        }
        if offset + size + 2 > body.len() {
            return Err("truncated chunked response".into());
        }
        decoded.extend_from_slice(&body[offset..offset + size]);
        offset += size + 2;
    }
}

fn proxy(mut client: TcpStream, target: &Target, request: &IncomingRequest) -> Result<(), String> {
    let started = std::time::Instant::now();
    let mut upstream = connect(target)?;

    let mut head = format!(
        "{} {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n",
        request.method, request.path
    );
    for (name, value) in &request.headers {
        if matches!(
            name.as_str(),
            "authorization"
                | "content-type"
                | "accept"
                | "x-nonodb-tenant"
                | "x-nonodb-key-id"
                | "x-nonodb-key"
        ) {
            head.push_str(name);
            head.push_str(": ");
            head.push_str(value);
            head.push_str("\r\n");
        }
    }
    if !request.body.is_empty() {
        head.push_str(&format!("Content-Length: {}\r\n", request.body.len()));
    }
    head.push_str("\r\n");
    upstream
        .write_all(head.as_bytes())
        .map_err(|e| e.to_string())?;
    upstream
        .write_all(&request.body)
        .map_err(|e| e.to_string())?;

    let mut response = Vec::new();
    upstream
        .read_to_end(&mut response)
        .map_err(|e| e.to_string())?;
    let server_ms = started.elapsed().as_secs_f64() * 1000.0;
    let header = format!("x-nonodb-server-ms: {server_ms:.3}\r\n");
    if let Some(split) = response.windows(4).position(|window| window == b"\r\n\r\n") {
        response.splice(split + 2..split + 2, header.bytes());
    }
    client.write_all(&response).map_err(|e| e.to_string())?;
    Ok(())
}

fn proxy_stream(
    mut client: TcpStream,
    target: &Target,
    request: &IncomingRequest,
) -> Result<(), String> {
    let mut upstream = connect(target)?;
    let mut head = format!(
        "{} {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n",
        request.method, request.path
    );
    for (name, value) in &request.headers {
        if matches!(
            name.as_str(),
            "authorization"
                | "content-type"
                | "accept"
                | "x-nonodb-tenant"
                | "x-nonodb-key-id"
                | "x-nonodb-key"
        ) {
            head.push_str(name);
            head.push_str(": ");
            head.push_str(value);
            head.push_str("\r\n");
        }
    }
    if !request.body.is_empty() {
        head.push_str(&format!("Content-Length: {}\r\n", request.body.len()));
    }
    head.push_str("\r\n");
    upstream
        .write_all(head.as_bytes())
        .map_err(|e| e.to_string())?;
    upstream
        .write_all(&request.body)
        .map_err(|e| e.to_string())?;
    client.set_write_timeout(None).map_err(|e| e.to_string())?;
    std::io::copy(&mut upstream, &mut client).map_err(|e| e.to_string())?;
    Ok(())
}

fn connect(target: &Target) -> Result<Upstream, String> {
    match target {
        #[cfg(unix)]
        Target::Unix(path) => {
            let stream = std::os::unix::net::UnixStream::connect(path)
                .map_err(|e| format!("connect: {e}"))?;
            Ok(Box::new(stream))
        }
        Target::Tcp(addr) => {
            let stream = TcpStream::connect(addr).map_err(|e| format!("connect {addr}: {e}"))?;
            stream
                .set_read_timeout(Some(Duration::from_secs(300)))
                .map_err(|e| e.to_string())?;
            stream
                .set_write_timeout(Some(Duration::from_secs(30)))
                .map_err(|e| e.to_string())?;
            Ok(Box::new(stream))
        }
    }
}

fn serve_asset(mut client: TcpStream, path: &str) -> Result<(), String> {
    let normalized = path.trim_start_matches('/');
    let normalized = if normalized.is_empty() {
        "index.html"
    } else {
        normalized
    };
    let mut asset_path = normalized.to_string();
    let mut asset = Assets::get(&asset_path);
    if asset.is_none() && !normalized.contains('.') {
        asset_path = "index.html".to_string();
        asset = Assets::get(&asset_path);
    }
    let Some(asset) = asset else {
        return write_response(
            &mut client,
            404,
            "text/plain; charset=utf-8",
            b"not found\n",
            false,
        );
    };
    let mime = mime_guess::from_path(&asset_path)
        .first_or_octet_stream()
        .to_string();
    let cache = if asset_path == "index.html" {
        "no-cache"
    } else {
        "public, max-age=31536000, immutable"
    };
    write_response_with_cache(&mut client, 200, &mime, &asset.data, cache)
}

fn write_response(
    client: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
    long_cache: bool,
) -> Result<(), String> {
    write_response_with_cache(
        client,
        status,
        content_type,
        body,
        if long_cache {
            "public, max-age=31536000, immutable"
        } else {
            "no-cache"
        },
    )
}

fn write_response_with_cache(
    client: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
    cache: &str,
) -> Result<(), String> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        501 => "Not Implemented",
        _ => "Error",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: {cache}\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\nContent-Security-Policy: default-src 'self'; connect-src 'self' http: https:; style-src 'self' 'unsafe-inline'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'\r\nConnection: close\r\n\r\n",
        body.len()
    );
    client
        .write_all(head.as_bytes())
        .and_then(|_| client.write_all(body))
        .map_err(|e| e.to_string())
}

fn open_browser(url: &str) {
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut command = std::process::Command::new("open");
        command.arg(url);
        command
    };
    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = std::process::Command::new("cmd");
        command.args(["/C", "start", "", url]);
        command
    };
    #[cfg(all(unix, not(target_os = "macos")))]
    let mut command = {
        let mut command = std::process::Command::new("xdg-open");
        command.arg(url);
        command
    };
    if let Err(err) = command.spawn() {
        eprintln!("could not open browser ({err}); open {url} manually");
    }
}
