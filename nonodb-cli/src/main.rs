//! `nonodb-cli` — proste narzędzie administracyjne.
//!
//! On Unix, connections go through the UDS socket. On Windows, the CLI uses
//! plaintext HTTP over TCP (`NONODB_URL`). Backup/restore/inspect work on LMDB
//! files and do not require a running daemon (backup/inspect open the database
//! read-only).

use std::io::{Read, Write};
#[cfg(not(unix))]
use std::net::TcpStream;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use nonodb_core::heed::types::Bytes;
use nonodb_core::heed::{CompactionOption, EnvFlags, EnvOpenOptions};
use serde_json::{json, Value};

fn main() {
    if let Err(err) = run() {
        eprintln!("nonodb-cli: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(cmd) = args.first().map(String::as_str) else {
        return usage();
    };
    match cmd {
        "ping" => {
            let res = http_get("/healthz")?;
            println!("{}", serde_json::to_string_pretty(&res).unwrap());
        }
        "exec" => {
            let raw = args.get(1).ok_or("użycie: nonodb-cli exec '<json>'")?;
            let payload: Value = serde_json::from_str(raw).map_err(|e| e.to_string())?;
            let res = http_post("/v1/exec", &payload)?;
            println!("{}", serde_json::to_string_pretty(&res).unwrap());
        }
        "collections" => {
            let res = http_post("/v1/exec", &json!({"op": "list_collections"}))?;
            print_result(&res);
        }
        "count" => {
            let coll = args
                .get(1)
                .ok_or("użycie: nonodb-cli count <kolekcja> [filtr-json]")?;
            let filter = parse_json_arg(args.get(2))?;
            let res = http_post(
                "/v1/exec",
                &json!({"op": "count", "coll": coll, "filter": filter}),
            )?;
            print_result(&res);
        }
        "find" => {
            let coll = args.get(1).ok_or(
                "użycie: nonodb-cli find <kolekcja> [filtr-json] [--sort pole:asc] [--limit N]",
            )?;
            let mut i = 2;
            let filter = if args.get(2).is_some_and(|s| !s.starts_with("--")) {
                i = 3;
                parse_json_arg(args.get(2))?
            } else {
                json!({})
            };
            let mut sort = Vec::new();
            let mut limit = 100usize;
            let mut skip = 0usize;
            while i < args.len() {
                match args[i].as_str() {
                    "--sort" => {
                        let spec = args.get(i + 1).ok_or("--sort wymaga pole:asc|desc")?;
                        let (field, dir) = spec.split_once(':').unwrap_or((spec, "asc"));
                        sort.push(json!([
                            field,
                            if dir.eq_ignore_ascii_case("desc") {
                                -1
                            } else {
                                1
                            }
                        ]));
                        i += 2;
                    }
                    "--limit" => {
                        limit = args
                            .get(i + 1)
                            .ok_or("--limit wymaga liczby")?
                            .parse()
                            .map_err(|e| format!("limit: {e}"))?;
                        i += 2;
                    }
                    "--skip" => {
                        skip = args
                            .get(i + 1)
                            .ok_or("--skip wymaga liczby")?
                            .parse()
                            .map_err(|e| format!("skip: {e}"))?;
                        i += 2;
                    }
                    other => return Err(format!("nieznana opcja: {other}")),
                }
            }
            let res = http_post(
                "/v1/exec",
                &json!({
                    "op": "find", "coll": coll, "filter": filter,
                    "sort": sort, "skip": skip, "limit": limit,
                }),
            )?;
            print_result(&res);
        }
        "indexes" => {
            let coll = args.get(1).ok_or("użycie: nonodb-cli indexes <kolekcja>")?;
            let res = http_post("/v1/exec", &json!({"op": "list_indexes", "coll": coll}))?;
            print_result(&res);
        }
        "backup" => {
            let data = PathBuf::from(
                args.get(1)
                    .ok_or("użycie: nonodb-cli backup <data-dir> <out-dir>")?,
            );
            let out = PathBuf::from(
                args.get(2)
                    .ok_or("użycie: nonodb-cli backup <data-dir> <out-dir>")?,
            );
            backup(&data, &out)?;
        }
        "inspect" => {
            let data = PathBuf::from(
                args.get(1)
                    .ok_or("użycie: nonodb-cli inspect <data-dir> [kolekcja]")?,
            );
            inspect(&data, args.get(2).map(String::as_str))?;
        }
        "restore" => {
            let backup = PathBuf::from(
                args.get(1)
                    .ok_or("użycie: nonodb-cli restore <backup-dir> <data-dir> [--force]")?,
            );
            let data = PathBuf::from(
                args.get(2)
                    .ok_or("użycie: nonodb-cli restore <backup-dir> <data-dir> [--force]")?,
            );
            let force = args.iter().any(|a| a == "--force");
            restore(&backup, &data, force)?;
        }
        "-h" | "--help" | "help" => return usage(),
        other => return Err(format!("nieznana komenda: {other} (spróbuj `help`)")),
    }
    Ok(())
}

fn usage() -> Result<(), String> {
    eprintln!(
        "nonodb-cli <komenda>\n\
         \n\
         Komendy:\n\
         ping                       sprawdź daemona\n\
         exec '<json>'              dowolne żądanie /v1/exec\n\
         collections                lista kolekcji\n\
         count <coll> [filtr]       liczba dokumentów\n\
         find <coll> [filtr] [--sort pole:asc|desc] [--limit N] [--skip N]\n\
         indexes <coll>             indeksy kolekcji\n\
         backup <data> <out>        kopia LMDB (także przy działającym daemonie)\n\
         inspect <data> [coll]      statystyki bazy/kolekcji\n\
         restore <backup> <data> [--force]  przywróć data.mdb (najpierw zatrzymaj daemona)\n\
         \n\
         Zmienne: NONODB_SOCK (Unix, domyślnie /run/nonodb.sock),\n\
         NONODB_URL (Windows, domyślnie http://127.0.0.1:7878), NONODB_TOKEN"
    );
    Ok(())
}

fn print_result(res: &Value) {
    if res.get("ok").and_then(Value::as_bool) == Some(true) {
        println!("{}", serde_json::to_string_pretty(&res["result"]).unwrap());
    } else {
        eprintln!("{}", serde_json::to_string_pretty(res).unwrap());
        std::process::exit(1);
    }
}

fn parse_json_arg(raw: Option<&String>) -> Result<Value, String> {
    match raw {
        Some(s) if !s.starts_with("--") => serde_json::from_str(s).map_err(|e| e.to_string()),
        _ => Ok(json!({})),
    }
}

#[cfg(unix)]
fn sock_path() -> String {
    std::env::var("NONODB_SOCK").unwrap_or_else(|_| "/run/nonodb.sock".into())
}

#[cfg(not(unix))]
fn tcp_addr() -> Result<String, String> {
    let url = std::env::var("NONODB_URL").unwrap_or_else(|_| "http://127.0.0.1:7878".into());
    let rest = url
        .strip_prefix("http://")
        .ok_or("NONODB_URL musi zaczynać się od http:// (CLI nie obsługuje TLS)")?;
    let rest = rest.trim_end_matches('/');
    if rest.is_empty() {
        return Err("NONODB_URL nie zawiera adresu host:port".into());
    }
    Ok(rest.to_string())
}

fn http_get(path: &str) -> Result<Value, String> {
    http_request("GET", path, None)
}

fn http_post(path: &str, body: &Value) -> Result<Value, String> {
    http_request("POST", path, Some(body))
}

fn http_request(method: &str, path: &str, body: Option<&Value>) -> Result<Value, String> {
    #[cfg(unix)]
    let mut stream = UnixStream::connect(sock_path()).map_err(|e| format!("connect: {e}"))?;
    #[cfg(not(unix))]
    let mut stream = {
        let addr = tcp_addr()?;
        TcpStream::connect(&addr).map_err(|e| format!("connect {addr}: {e}"))?
    };
    let payload = body
        .map(|b| serde_json::to_vec(b).unwrap())
        .unwrap_or_default();
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    if let Ok(token) = std::env::var("NONODB_TOKEN") {
        if !token.is_empty() {
            req.push_str(&format!("Authorization: Bearer {token}\r\n"));
        }
    }
    if body.is_some() {
        req.push_str("Content-Type: application/json\r\n");
        req.push_str(&format!("Content-Length: {}\r\n", payload.len()));
    }
    req.push_str("\r\n");
    stream
        .write_all(req.as_bytes())
        .map_err(|e| e.to_string())?;
    stream.write_all(&payload).map_err(|e| e.to_string())?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).map_err(|e| e.to_string())?;
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("niepoprawna odpowiedź HTTP")?;
    let head = String::from_utf8_lossy(&raw[..split]);
    let status = head.lines().next().unwrap_or("");
    if !status.contains(" 200 ") && !status.contains(" 201 ") {
        return Err(format!(
            "HTTP: {status}: {}",
            String::from_utf8_lossy(&raw[split + 4..])
        ));
    }
    serde_json::from_slice(&raw[split + 4..]).map_err(|e| e.to_string())
}

fn backup(data: &Path, out_dir: &Path) -> Result<(), String> {
    std::fs::create_dir_all(out_dir).map_err(|e| e.to_string())?;
    let out = out_dir.join("data.mdb");
    let env = unsafe {
        EnvOpenOptions::new()
            .flags(EnvFlags::READ_ONLY)
            .open(data)
            .map_err(|e| e.to_string())?
    };
    let file = env
        .copy_to_file(&out, CompactionOption::Enabled)
        .map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    eprintln!("backup: {} -> {}", data.display(), out.display());
    Ok(())
}

fn inspect(data: &Path, only: Option<&str>) -> Result<(), String> {
    let env = unsafe {
        EnvOpenOptions::new()
            .flags(EnvFlags::READ_ONLY)
            .max_dbs(256)
            .open(data)
            .map_err(|e| e.to_string())?
    };
    let rtxn = env.read_txn().map_err(|e| e.to_string())?;
    let meta = env
        .open_database::<Bytes, Bytes>(&rtxn, Some("meta"))
        .map_err(|e| e.to_string())?
        .ok_or("brak bazy meta")?;
    let mut colls = Vec::new();
    for item in meta.iter(&rtxn).map_err(|e| e.to_string())? {
        let (k, _) = item.map_err(|e| e.to_string())?;
        if let Ok(k) = std::str::from_utf8(k) {
            if let Some(name) = k.strip_prefix("collection:") {
                colls.push(name.to_string());
            }
        }
    }
    println!("data: {}", data.display());
    println!("collections: {}", colls.len());
    for name in colls {
        if only.is_some_and(|o| o != name) {
            continue;
        }
        if let Some(db) = env
            .open_database::<Bytes, Bytes>(&rtxn, Some(&name))
            .map_err(|e| e.to_string())?
        {
            let entries = db.stat(&rtxn).map_err(|e| e.to_string())?.entries;
            println!("  {name}: {entries} docs");
        }
    }
    let _ = rtxn.commit();
    Ok(())
}

fn restore(backup: &Path, data: &Path, force: bool) -> Result<(), String> {
    let src = if backup.join("data.mdb").exists() {
        backup.join("data.mdb")
    } else {
        backup.to_path_buf()
    };
    if !src.is_file() {
        return Err(format!("brak pliku kopii: {}", src.display()));
    }
    std::fs::create_dir_all(data).map_err(|e| e.to_string())?;
    let dst = data.join("data.mdb");
    if dst.exists() && !force {
        return Err(format!(
            "{} istnieje; użyj --force po zatrzymaniu daemona",
            dst.display()
        ));
    }
    let _ = std::fs::remove_file(data.join("lock.mdb"));
    std::fs::copy(&src, &dst).map_err(|e| e.to_string())?;
    let file = std::fs::File::open(&dst).map_err(|e| e.to_string())?;
    let _ = file.sync_all();
    eprintln!("restore: {} -> {}", src.display(), dst.display());
    Ok(())
}
