//! Local GUI server with embedded React assets and a same-origin API proxy.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "$CARGO_MANIFEST_DIR/../gui/dist"]
struct Assets;

#[derive(Clone)]
enum Target {
    #[cfg(unix)]
    Unix(String),
    #[cfg(not(unix))]
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

    let target = target()?;
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
                std::thread::spawn(move || {
                    if let Err(err) = handle(stream, &target) {
                        eprintln!("gui request: {err}");
                    }
                });
            }
            Err(err) => eprintln!("gui accept: {err}"),
        }
    }
    Ok(())
}

fn target() -> Result<Target, String> {
    #[cfg(unix)]
    {
        Ok(Target::Unix(
            std::env::var("NONODB_SOCK").unwrap_or_else(|_| "/run/nonodb.sock".into()),
        ))
    }
    #[cfg(not(unix))]
    {
        let url = std::env::var("NONODB_URL").unwrap_or_else(|_| "http://127.0.0.1:7878".into());
        let addr = url
            .strip_prefix("http://")
            .ok_or("NONODB_URL must start with http://")?
            .trim_end_matches('/')
            .to_string();
        Ok(Target::Tcp(addr))
    }
}

fn handle(mut client: TcpStream, target: &Target) -> Result<(), String> {
    client
        .set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(|e| e.to_string())?;
    client
        .set_write_timeout(Some(Duration::from_secs(300)))
        .map_err(|e| e.to_string())?;
    let request = read_request(&mut client)?;

    if request.path.starts_with("/v1/watch") {
        return proxy_stream(client, target, &request);
    }
    if request.path == "/v1/exec"
        || request.path == "/healthz"
        || request.path == "/metrics"
        || request.path.starts_with("/v1/exec?")
    {
        return proxy(client, target, &request);
    }
    if request.path.starts_with("/v1/") {
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
        #[cfg(not(unix))]
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
