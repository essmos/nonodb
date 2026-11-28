//! `nonodb-cli` - a simple administrative tool.
//!
//! On Unix, connections go through the UDS socket. On Windows, the CLI uses
//! plaintext HTTP over TCP (`NONODB_URL`). Backup/restore/inspect work on LMDB
//! files and do not require a running daemon (backup/inspect open the database
//! read-only).

use std::io::{BufRead, Read, Write};
#[cfg(not(unix))]
use std::net::TcpStream;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use nonodb_core::heed::types::Bytes;
use nonodb_core::heed::{CompactionOption, EnvFlags, EnvOpenOptions};
use serde_json::{json, Value};

mod branch;
mod gui;

fn main() {
    if let Err(err) = run() {
        eprintln!("nonodb-cli: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    strip_instance_flags(&mut args)?;
    strip_db_flag(&mut args)?;
    let Some(cmd) = args.first().map(String::as_str) else {
        return usage();
    };
    match cmd {
        "gui" => gui::run(&args[1..])?,
        "instances" => instances_cmd(&args[1..])?,
        "ping" => {
            let res = http_get("/healthz")?;
            println!("{}", serde_json::to_string_pretty(&res).unwrap());
        }
        "ready" => {
            let res = http_get("/readyz")?;
            println!("{}", serde_json::to_string_pretty(&res).unwrap());
        }
        "server" | "bind" | "binds" => {
            let res = http_get("/v1/server")?;
            println!("{}", serde_json::to_string_pretty(&res).unwrap());
        }
        "capabilities" => {
            let res = http_get("/v1/capabilities")?;
            println!("{}", serde_json::to_string_pretty(&res).unwrap());
        }
        "backup-keygen" => {
            let path = PathBuf::from(
                args.get(1)
                    .ok_or("usage: nonodb-cli backup-keygen <identity-file>")?,
            );
            let (identity, recipient) = nonodb_core::backup_crypto::generate_keypair();
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options
                .open(&path)
                .map_err(|e| format!("create {}: {e}", path.display()))?;
            file.write_all(format!("{identity}\n").as_bytes())
                .map_err(|e| e.to_string())?;
            file.sync_all().map_err(|e| e.to_string())?;
            println!("{recipient}");
            eprintln!("identity: {}", path.display());
        }
        "replication" => {
            let action = args.get(1).map(String::as_str).unwrap_or("status");
            match action {
                "status" => {
                    let res = http_get("/v1/replication/status")?;
                    println!("{}", serde_json::to_string_pretty(&res).unwrap());
                }
                "primary" => {
                    let res = http_get("/v1/replication/primary")?;
                    println!("{}", serde_json::to_string_pretty(&res).unwrap());
                }
                "changes" => {
                    let after = args.get(2).map(String::as_str).unwrap_or("0");
                    let limit = args.get(3).map(String::as_str).unwrap_or("100");
                    let res = http_get(&format!(
                        "/v1/replication/changes?after={after}&limit={limit}"
                    ))?;
                    println!("{}", serde_json::to_string_pretty(&res).unwrap());
                }
                "prune" => {
                    let through: u64 = args
                        .get(2)
                        .ok_or("usage: nonodb-cli replication prune <through> [--require-acks]")?
                        .parse()
                        .map_err(|e| format!("through: {e}"))?;
                    let require_acks = args.iter().any(|arg| arg == "--require-acks");
                    let res = http_post(
                        "/v1/replication/prune",
                        &json!({ "through": through, "require_acks": require_acks }),
                    )?;
                    print_result(&res);
                }
                "acks" => {
                    let res = http_get("/v1/replication/acks")?;
                    println!("{}", serde_json::to_string_pretty(&res).unwrap());
                }
                "pair" | "add-replica" | "add_replica" | "enroll" => {
                    let name = args
                        .get(2)
                        .ok_or("usage: nonodb-cli replication add-replica <name> [databases]")?;
                    let databases = args
                        .get(3)
                        .map(|value| {
                            value
                                .split(',')
                                .map(str::trim)
                                .filter(|item| !item.is_empty())
                                .map(str::to_string)
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    let res = http_post(
                        "/v1/replication/pairings",
                        &json!({ "name": name, "databases": databases }),
                    )?;
                    if let Some(code) = res.pointer("/result/code").and_then(|v| v.as_str()) {
                        let join_url = http_get("/v1/replication/primary")
                            .ok()
                            .and_then(|value| {
                                value
                                    .pointer("/result/write_url")
                                    .and_then(|url| url.as_str())
                                    .map(str::to_string)
                            })
                            .unwrap_or_else(|| "<primary-api-url>".to_string());
                        println!("Pairing code: {code}");
                        println!("On the replica, run:");
                        println!("  nonodb-cli replication join {join_url} {code}");
                        println!("Use the daemon API URL (default port 7878, not GUI proxy 8787).");
                    } else {
                        print_result(&res);
                    }
                }
                "pairings" => {
                    let res = http_get("/v1/replication/pairings")?;
                    println!("{}", serde_json::to_string_pretty(&res).unwrap());
                }
                "revoke" => {
                    let code = args
                        .get(2)
                        .ok_or("usage: nonodb-cli replication revoke <pairing-code>")?;
                    let res =
                        http_request("DELETE", &format!("/v1/replication/pairings/{code}"), None)?;
                    print_result(&res);
                }
                "join" => {
                    let url = args
                        .get(2)
                        .ok_or("usage: nonodb-cli replication join <primary-url> [pairing-code]")?;
                    let code = match args.get(3) {
                        Some(code) => code.trim().to_string(),
                        None => {
                            eprintln!("Paste the 8-character pairing code shown on the primary:");
                            let mut code = String::new();
                            std::io::stdin()
                                .read_line(&mut code)
                                .map_err(|error| format!("read pairing code: {error}"))?;
                            code.trim().to_string()
                        }
                    };
                    if code.is_empty() {
                        return Err("pairing code is empty".into());
                    }
                    let res =
                        http_post("/v1/replication/join", &json!({ "url": url, "code": code }))?;
                    let body = res.get("result").unwrap_or(&res);
                    let source = body
                        .pointer("/source/source")
                        .and_then(Value::as_str)
                        .unwrap_or(url);
                    let databases = body
                        .pointer("/source/databases")
                        .and_then(Value::as_array)
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(Value::as_str)
                                .collect::<Vec<_>>()
                                .join(", ")
                        })
                        .filter(|value| !value.is_empty())
                        .unwrap_or_else(|| "default".to_string());
                    let node_id = body
                        .get("node_id")
                        .and_then(Value::as_str)
                        .unwrap_or("local");
                    println!("Connected to the cluster.");
                    println!("  node_id:   {node_id}");
                    println!("  source:    {source}");
                    println!("  databases: {databases}");
                    let deadline = Instant::now() + Duration::from_secs(10);
                    let mut connected = false;
                    let mut printed_status = false;
                    let mut last_error: Option<String> = None;
                    loop {
                        if let Ok(status) = http_get("/v1/replication/status") {
                            if let Some(result) = status.get("result") {
                                let role = result
                                    .get("role")
                                    .and_then(Value::as_str)
                                    .unwrap_or("replica");
                                let local_source = result
                                    .pointer("/replication/source")
                                    .and_then(Value::as_str)
                                    .unwrap_or(source);
                                if !printed_status {
                                    println!("  role:      {role}");
                                    println!("  pulling:   {local_source}");
                                    printed_status = true;
                                }
                                last_error = result
                                    .pointer("/replication/last_transport_error")
                                    .and_then(Value::as_str)
                                    .filter(|value| !value.is_empty())
                                    .map(str::to_string)
                                    .or_else(|| {
                                        result
                                            .pointer("/replication/last_apply_error")
                                            .and_then(Value::as_str)
                                            .filter(|value| !value.is_empty())
                                            .map(str::to_string)
                                    });
                                connected = result
                                    .pointer("/replication/promotion_ready")
                                    .and_then(Value::as_bool)
                                    .unwrap_or(false);
                                if connected {
                                    break;
                                }
                            }
                        }
                        if Instant::now() >= deadline {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(250));
                    }
                    if connected {
                        println!("Replication connected and caught up.");
                    } else {
                        println!("Join accepted; the first pull is still in progress.");
                        if let Some(error) = last_error {
                            println!("  last error: {error}");
                        }
                        println!("  Check with: nonodb-cli replication status");
                    }
                    println!("The primary GUI shows this replica after its first acknowledgement.");
                }
                "forget" | "remove" => {
                    let node = args
                        .get(2)
                        .ok_or("usage: nonodb-cli replication forget <node-id>")?;
                    let res =
                        http_request("DELETE", &format!("/v1/replication/acks/{node}"), None)?;
                    print_result(&res);
                }
                "pause" => {
                    let res = http_post("/v1/replication/pause", &json!({}))?;
                    print_result(&res);
                }
                "resume" => {
                    let res = http_post("/v1/replication/resume", &json!({}))?;
                    print_result(&res);
                }
                "disconnect" => {
                    let res = http_post("/v1/replication/disconnect", &json!({}))?;
                    print_result(&res);
                }
                "source" => {
                    let source = args.get(2).ok_or(
                        "usage: nonodb-cli replication source <url> [databases] [interval]",
                    )?;
                    let databases = args
                        .get(3)
                        .map(|value| {
                            value
                                .split(',')
                                .map(str::trim)
                                .filter(|item| !item.is_empty())
                                .map(str::to_string)
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    let interval = args
                        .get(4)
                        .map(|value| value.parse::<u64>())
                        .transpose()
                        .map_err(|error| format!("interval: {error}"))?
                        .unwrap_or(1);
                    let res = http_request(
                        "PUT",
                        "/v1/replication/source",
                        Some(&json!({
                            "source": source,
                            "databases": databases,
                            "interval_secs": interval,
                        })),
                    )?;
                    print_result(&res);
                }
                "promote" => {
                    let expected: u64 = args
                        .get(2)
                        .ok_or("usage: nonodb-cli replication promote <expected-epoch>")?
                        .parse()
                        .map_err(|e| format!("expected epoch: {e}"))?;
                    let force = args.iter().any(|arg| arg == "--force");
                    let res = http_post(
                        "/v1/replication/promote",
                        &json!({ "expected_epoch": expected, "force": force }),
                    )?;
                    print_result(&res);
                }
                "demote" => {
                    let expected: u64 = args
                        .get(2)
                        .ok_or("usage: nonodb-cli replication demote <expected-epoch> [replica|standby]")?
                        .parse()
                        .map_err(|e| format!("expected epoch: {e}"))?;
                    let role = args.get(3).map(String::as_str).unwrap_or("replica");
                    let successor = args.get(4).map(String::as_str);
                    let res = http_post(
                        "/v1/replication/demote",
                        &json!({
                            "expected_epoch": expected,
                            "role": role,
                            "successor": successor,
                        }),
                    )?;
                    print_result(&res);
                }
                other => return Err(format!("unknown replication action: {other}")),
            }
        }
        "databases" => {
            let action = args.get(1).map(String::as_str).unwrap_or("list");
            let payload = match action {
                "list" => json!({"op": "list_databases"}),
                "create" => json!({
                    "op": "create_database",
                    "name": args.get(2).ok_or("użycie: nonodb-cli databases create <nazwa>")?,
                }),
                "drop" => json!({
                    "op": "drop_database",
                    "name": args.get(2).ok_or("użycie: nonodb-cli databases drop <nazwa>")?,
                }),
                other => return Err(format!("nieznana akcja databases: {other}")),
            };
            let res = http_post("/v1/exec", &payload)?;
            print_result(&res);
        }
        "exec" => {
            let raw = args.get(1).ok_or("użycie: nonodb-cli exec '<json>'")?;
            let payload: Value = serde_json::from_str(raw).map_err(|e| e.to_string())?;
            let res = http_post("/v1/exec", &payload)?;
            println!("{}", serde_json::to_string_pretty(&res).unwrap());
        }
        "quota" => quota_cmd(&args[1..])?,
        "collections" => {
            let res = http_post("/v1/exec", &json!({"op": "list_collections"}))?;
            print_result(&res);
        }
        "create-collection" | "create_collection" => create_collection_cmd(&args[1..])?,
        "get-collection-columnar" | "get_collection_columnar" => {
            let coll = args
                .get(1)
                .ok_or("użycie: nonodb-cli get-collection-columnar <kolekcja>")?;
            let res = http_post(
                "/v1/exec",
                &json!({"op": "get_collection_columnar", "coll": coll}),
            )?;
            print_result(&res);
        }
        "set-collection-columnar" | "set_collection_columnar" => {
            set_collection_columnar_cmd(&args[1..])?
        }
        "get-collection-timeseries" | "get_collection_timeseries" => {
            let coll = args
                .get(1)
                .ok_or("użycie: nonodb-cli get-collection-timeseries <kolekcja>")?;
            let res = http_post(
                "/v1/exec",
                &json!({"op": "get_collection_timeseries", "coll": coll}),
            )?;
            print_result(&res);
        }
        "set-collection-timeseries" | "set_collection_timeseries" => {
            set_collection_timeseries_cmd(&args[1..])?
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
        "reindex" => {
            let coll = args.get(1).ok_or("użycie: nonodb-cli reindex <kolekcja>")?;
            let res = http_post("/v1/exec", &json!({"op": "reindex", "coll": coll}))?;
            print_result(&res);
        }
        "map" => {
            let res = http_post("/v1/exec", &json!({"op": "map_info"}))?;
            print_result(&res);
        }
        "sync" => {
            let res = http_post("/v1/exec", &json!({"op": "sync"}))?;
            print_result(&res);
        }
        "search-all" => search_all_cmd(&args[1..])?,
        "webhooks" => webhooks_cmd(&args[1..])?,
        "streams" | "stream" => streams_cmd(&args[1..])?,
        "events" => {
            if args.get(1).map(String::as_str) == Some("tail") {
                return events_tail(&args[2..]);
            }
            let coll = args
                .get(1)
                .ok_or("użycie: nonodb-cli events <kolekcja> [on|off] [pole...]")?;
            let res = match args.get(2).map(String::as_str) {
                None => http_post(
                    "/v1/exec",
                    &json!({"op": "get_collection_events", "coll": coll}),
                )?,
                Some(state @ ("on" | "off")) => {
                    let fields = args[3..].to_vec();
                    http_post(
                        "/v1/exec",
                        &json!({
                            "op": "set_collection_events",
                            "coll": coll,
                            "enabled": state == "on",
                            "fields": fields,
                        }),
                    )?
                }
                Some(other) => {
                    return Err(format!("nieznany stan `{other}`; użyj `on` albo `off`"))
                }
            };
            print_result(&res);
        }
        "subscriptions" => {
            let coll = args
                .get(1)
                .ok_or("użycie: nonodb-cli subscriptions <kolekcja>")?;
            let res = http_post(
                "/v1/exec",
                &json!({"op": "list_subscriptions", "coll": coll}),
            )?;
            print_result(&res);
        }
        "subscribe" => subscribe_cmd(&args[1..])?,
        "subscription-ping" => {
            let coll = args
                .get(1)
                .ok_or("użycie: nonodb-cli subscription-ping <kolekcja> <nazwa> [sekundy]")?;
            let name = args
                .get(2)
                .ok_or("użycie: nonodb-cli subscription-ping <kolekcja> <nazwa> [sekundy]")?;
            let mut payload = json!({
                "op": "renew_subscription",
                "coll": coll,
                "name": name,
            });
            if let Some(raw) = args.get(3) {
                let lease: u64 = raw.parse().map_err(|e| format!("sekundy: {e}"))?;
                payload["lease_seconds"] = json!(lease);
            }
            let res = http_post("/v1/exec", &payload)?;
            print_result(&res);
        }
        "unsubscribe" => {
            let coll = args
                .get(1)
                .ok_or("użycie: nonodb-cli unsubscribe <kolekcja> <nazwa>")?;
            let name = args
                .get(2)
                .ok_or("użycie: nonodb-cli unsubscribe <kolekcja> <nazwa>")?;
            let res = http_post(
                "/v1/exec",
                &json!({"op": "delete_subscription", "coll": coll, "name": name}),
            )?;
            print_result(&res);
        }
        "history" => {
            let coll = args
                .get(1)
                .ok_or("użycie: nonodb-cli history <kolekcja> [max_wersji]")?;
            let res = if let Some(raw) = args.get(2) {
                let max_versions: usize = raw.parse().map_err(|e| format!("max_wersji: {e}"))?;
                http_post(
                    "/v1/exec",
                    &json!({
                        "op": "set_collection_history",
                        "coll": coll,
                        "max_versions": max_versions,
                    }),
                )?
            } else {
                http_post(
                    "/v1/exec",
                    &json!({"op": "get_collection_history", "coll": coll}),
                )?
            };
            print_result(&res);
        }
        "sql" => sql_cmd(&args[1..])?,
        "shell" => sql_shell()?,
        "auth" => auth_cmd(&args[1..])?,
        "resize" => {
            let raw = args
                .get(1)
                .ok_or("użycie: nonodb-cli resize <rozmiar, np. 4GiB>")?;
            let size = nonodb_core::parse_size(raw)
                .ok_or_else(|| format!("niepoprawny rozmiar: {raw}"))?;
            let res = http_post("/v1/exec", &json!({"op": "resize_map", "size": size}))?;
            print_result(&res);
        }
        "branch" => branch::run(&args[1..])?,
        "backup" => {
            let data = PathBuf::from(
                args.get(1)
                    .ok_or("użycie: nonodb-cli backup <data-dir> <out-dir>")?,
            );
            let out = PathBuf::from(
                args.get(2)
                    .ok_or("użycie: nonodb-cli backup <data-dir> <out-dir>")?,
            );
            let with_sidecars = !args.iter().any(|arg| arg == "--without-search-sidecars");
            let source = database_dir(&data)?;
            backup(&source, &out, with_sidecars)?;
            let manifest = nonodb_core::BackupManifest::from_backup(&out, &source)
                .map_err(|e| e.to_string())?;
            nonodb_core::verify::write_manifest(&out, &manifest).map_err(|e| e.to_string())?;
            nonodb_core::backup_info::write(&out, &manifest).map_err(|e| e.to_string())?;
            eprintln!(
                "manifest: {} (sha256 {})",
                out.join(nonodb_core::MANIFEST_FILE).display(),
                manifest.sha256
            );
        }
        "compact" => {
            let data = PathBuf::from(
                args.get(1)
                    .ok_or("usage: nonodb-cli compact <data-dir> [--force]")?,
            );
            let source = database_dir(&data)?;
            compact(&source, args.iter().any(|arg| arg == "--force"))?;
        }
        "backup-all" => {
            let data = PathBuf::from(
                args.get(1)
                    .ok_or("użycie: nonodb-cli backup-all <data-root> <out-dir>")?,
            );
            let out = PathBuf::from(
                args.get(2)
                    .ok_or("użycie: nonodb-cli backup-all <data-root> <out-dir>")?,
            );
            let with_sidecars = !args.iter().any(|arg| arg == "--without-search-sidecars");
            backup_all(&data, &out, with_sidecars)?;
        }
        "restore-all" => {
            if args.iter().any(|arg| {
                arg == "--to"
                    || arg.starts_with("--to=")
                    || arg == "--wal"
                    || arg.starts_with("--wal=")
            }) {
                return Err(
                    "--to/--wal are only supported by the single-database restore command".into(),
                );
            }
            let backup = PathBuf::from(
                args.get(1)
                    .ok_or("użycie: nonodb-cli restore-all <backup-dir> <data-root> [--force]")?,
            );
            let data = PathBuf::from(
                args.get(2)
                    .ok_or("użycie: nonodb-cli restore-all <backup-dir> <data-root> [--force]")?,
            );
            let force = args.iter().any(|a| a == "--force");
            let verify = args.iter().any(|a| a == "--verify");
            restore_all(&backup, &data, force, verify)?;
        }
        "verify" => {
            let data = PathBuf::from(
                args.get(1)
                    .ok_or("użycie: nonodb-cli verify <data-dir> [--json]")?,
            );
            let source = database_dir(&data)?;
            let report = nonodb_core::verify_data_dir(&source).map_err(|e| e.to_string())?;
            if args.iter().any(|a| a == "--json") {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?
                );
            } else {
                print_report(&report);
            }
        }
        "verify-backup" => {
            let backup = PathBuf::from(
                args.get(1)
                    .ok_or("użycie: nonodb-cli verify-backup <backup-dir> [--json]")?,
            );
            let checked = nonodb_core::verify_backup(&backup).map_err(|e| e.to_string())?;
            if args.iter().any(|a| a == "--json") {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&checked).map_err(|e| e.to_string())?
                );
            } else {
                eprintln!(
                    "manifest: source={} created_at_ms={} size={} sha256={}",
                    checked.manifest.source,
                    checked.manifest.created_at_ms,
                    checked.manifest.data_size,
                    checked.manifest.sha256
                );
                print_report(&checked.report);
            }
        }
        "inspect" => {
            let data = PathBuf::from(
                args.get(1)
                    .ok_or("użycie: nonodb-cli inspect <data-dir> [kolekcja]")?,
            );
            let source = database_dir(&data)?;
            inspect(&source, args.get(2).map(String::as_str))?;
        }
        "restore" => restore_cmd(&args[1..])?,
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
         gui [--port N] [--no-open]  lokalne GUI w przeglądarce\n\
         ping                       sprawdź daemona\n\
         server                     rzeczywiste adresy nasłuchu (TCP/UDS)\n\
         capabilities               publiczny opis możliwości serwera (JSON)\n\
         exec '<json>'              dowolne żądanie /v1/exec\n\
         databases [list|create <nazwa>|drop <nazwa>]  bazy w jednym daemonie\n\
         replication [status|changes [after] [limit]|source <url> [dbs] [interval]|pause|resume|disconnect|forget <node>|acks|prune <seq> [--require-acks]|promote <epoch> [--force]|demote <epoch> [role] [successor]|add-replica <name> [dbs]|pairings|revoke <code>|join <primary-url> [code]]\n\
         instances [--json] [prune] lista lokalnych daemonów\n\
         collections                lista kolekcji\n\
         create-collection <nazwa> [--profile standard|vault|timeseries|columnar]\n\
              [--time-field pole] [--meta-field pole] [--granularity seconds|minutes|hours]\n\
              [--compression fast|balanced|max] [--expire-after sekundy] [--ttl sekundy]\n\
              [--bucket-max-docs N] [--bucket-max-bytes N]\n\
         get-collection-timeseries <coll>    odczyt ustawień time-series\n\
         set-collection-timeseries <coll> [--time-field pole] [--meta-field pole]\n\
              [--granularity ...] [--compression ...] [--expire-after N|none]\n\
         quota [show]               użycie i limity wybranej bazy\n\
         quota set [--data R] [--index R] [--history R] [--sidecar R]\n\
              ustaw limity (np. 10GiB, 500MiB, unlimited); --db wybiera bazę\n\
         quota reset                usuń wszystkie limity wybranej bazy\n\
         count <coll> [filtr]       liczba dokumentów\n\
         find <coll> [filtr] [--sort pole:asc|desc] [--limit N] [--skip N]\n\
         indexes <coll>             indeksy kolekcji\n\
         auth <komenda>             zarządzanie auth.json (init/principal/token/cert/check)\n\
         map                        rozmiar mapy LMDB i zajęte bajty\n\
         sync                       wymuś fsync wybranej bazy\n\
         resize <rozmiar>           powiększ mapę online, np. 4GiB\n\
         search-all <coll> [--mode lexical|fuzzy|vector|hybrid] [--query tekst]\n\
              [--fields a,b] [--vector JSON] [--vector-field pole] [--filter JSON]\n\
              [--skip N] [--limit N] [--max-distance N] [--rrf-k N]  ujednolicone wyszukiwanie\n\
         webhooks <list|create|update|delete|deliveries|retry> ...  zarządzaj asynchronicznymi webhookami\n\
         streams [list|create|groups|reset|dead|retry|drop|prune] ...  offsety, grupy i retencja strumieni\n\
         branch <target-db> --data <root> [--from-snapshot <plik>] [--force] [--verify] [--no-sidecars] [--json]\n\
              utwórz niezależny klon bazy (reflink/copy; daemon zatrzymany lub snapshot)\n\
         backup <data> <out> [--without-search-sidecars]  kopia wybranej bazy (--db) + manifest.json\n\
         backup-keygen <plik>      generuje parę kluczy age do backupów\n\
         backup-all <data> <out> [--without-search-sidecars]  kopia wszystkich baz + manifest instancji\n\
         verify <data> [--json]     głęboka weryfikacja wybranej bazy i indeksów\n\
         verify-backup <backup> [--json]  sprawdź manifest, sumę SHA-256 i zawartość\n\
         inspect <data> [coll]      statystyki bazy/kolekcji (--db wybiera bazę)\n\
         restore <backup> <data> [--force] [--verify] [--to <czas>] [--wal <katalog>]  przywróć bazę (najpierw zatrzymaj daemona)\n\
         restore-all <backup> <data> [--force] [--verify]  przywróć wszystkie bazy\n\
         \n\
         Opcje: --db <nazwa> wybiera bazę (albo NONODB_DB).\n\
         --socket <ścieżka> wybiera daemona po sockecie (Unix),\n\
         --instance <pid|node_id> wybiera instancję z rejestru.\n\
         Zmienne: NONODB_SOCK (Unix, domyślnie /run/nonodb.sock),\n\
         NONODB_URL (Windows, domyślnie http://127.0.0.1:7878), NONODB_TOKEN,\n\
         NONODB_DB (domyślnie default),\n\
         NONODB_AUTH_FILE (domyślnie /etc/nonodb/auth.json)"
    );
    Ok(())
}

fn quota_cmd(args: &[String]) -> Result<(), String> {
    let action = args.first().map(String::as_str).unwrap_or("show");
    match action {
        "show" | "get" => {
            let res = http_post("/v1/exec", &json!({"op": "get_quota"}))?;
            print_result(&res);
        }
        "reset" => {
            let res = http_post(
                "/v1/exec",
                &json!({
                    "op": "set_quota",
                    "limits": {
                        "data_bytes": null,
                        "index_bytes": null,
                        "history_bytes": null,
                        "sidecar_bytes": null,
                    },
                }),
            )?;
            print_result(&res);
        }
        "set" => {
            let current = http_post("/v1/exec", &json!({"op": "get_quota"}))?;
            let mut limits = current
                .pointer("/result/limits")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            let mut i = 1;
            while i < args.len() {
                let (key, raw) = match args[i].as_str() {
                    "--data" => ("data_bytes", args.get(i + 1)),
                    "--index" => ("index_bytes", args.get(i + 1)),
                    "--history" => ("history_bytes", args.get(i + 1)),
                    "--sidecar" => ("sidecar_bytes", args.get(i + 1)),
                    other => return Err(format!("nieznana opcja quota: {other}")),
                };
                let raw = raw.ok_or_else(|| format!("{} wymaga rozmiaru", args[i]))?;
                if raw.eq_ignore_ascii_case("unlimited") {
                    limits.remove(key);
                } else {
                    let value = nonodb_core::parse_size(raw)
                        .ok_or_else(|| format!("niepoprawny rozmiar: {raw}"))?;
                    if value == 0 {
                        limits.remove(key);
                    } else {
                        limits.insert(key.to_string(), json!(value));
                    }
                }
                i += 2;
            }
            let res = http_post(
                "/v1/exec",
                &json!({"op": "set_quota", "limits": Value::Object(limits)}),
            )?;
            print_result(&res);
        }
        other => return Err(format!("nieznana akcja quota: {other}")),
    }
    Ok(())
}

pub(crate) fn print_report(report: &nonodb_core::VerificationReport) {
    println!("verified: {}", report.path);
    println!("schema: {}", report.schema_version);
    println!("collections: {}", report.collections.len());
    println!("documents: {}", report.documents);
    println!("document bytes: {}", report.bytes);
    println!(
        "secondary index entries: {}",
        report.secondary_index_entries
    );
    println!("text indexes: {}", report.text_indexes);
    println!("fuzzy indexes: {}", report.fuzzy_indexes);
    println!("history entries: {}", report.history_entries);
    println!("ttl entries: {}", report.ttl_entries);
    for (name, coll) in &report.collections {
        println!(
            "  {name}: {} docs, {} bytes, {} secondary, {} text, {} fuzzy",
            coll.documents,
            coll.bytes,
            coll.indexes.len(),
            coll.text_indexes.len(),
            coll.fuzzy_indexes.len()
        );
    }
}

fn subscribe_cmd(args: &[String]) -> Result<(), String> {
    let coll = args
        .first()
        .ok_or("użycie: nonodb-cli subscribe <kolekcja> <nazwa> [opcje]")?;
    let name = args
        .get(1)
        .ok_or("użycie: nonodb-cli subscribe <kolekcja> <nazwa> [opcje]")?;
    let mut filter = json!({});
    let mut count = None;
    let mut ops = Vec::new();
    let mut once = false;
    let mut event_type = None;
    let mut lease_seconds: Option<u64> = None;
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--filter" => {
                filter = serde_json::from_str(args.get(i + 1).ok_or("--filter wymaga JSON")?)
                    .map_err(|e| e.to_string())?;
                i += 2;
            }
            "--count" => {
                count = Some(
                    serde_json::from_str(args.get(i + 1).ok_or("--count wymaga JSON")?)
                        .map_err(|e| e.to_string())?,
                );
                i += 2;
            }
            "--ops" => {
                ops = args
                    .get(i + 1)
                    .ok_or("--ops wymaga listy insert,update,delete")?
                    .split(',')
                    .filter(|part| !part.is_empty())
                    .map(str::to_string)
                    .collect();
                i += 2;
            }
            "--once" => {
                once = true;
                i += 1;
            }
            "--type" => {
                event_type = Some(args.get(i + 1).ok_or("--type wymaga nazwy")?.clone());
                i += 2;
            }
            "--lease" => {
                let raw = args.get(i + 1).ok_or("--lease wymaga sekund")?;
                lease_seconds = Some(raw.parse::<u64>().map_err(|e| format!("lease: {e}"))?);
                i += 2;
            }
            other => return Err(format!("nieznana opcja: {other}")),
        }
    }
    let mut payload = json!({
        "op": "create_subscription",
        "coll": coll,
        "name": name,
        "filter": filter,
        "ops": ops,
        "once": once,
    });
    if let Some(count) = count {
        payload["count"] = count;
    }
    if let Some(event_type) = event_type {
        payload["event_type"] = json!(event_type);
    }
    if let Some(lease_seconds) = lease_seconds {
        payload["lease_seconds"] = json!(lease_seconds);
    }
    let res = http_post("/v1/exec", &payload)?;
    print_result(&res);
    Ok(())
}

fn search_all_cmd(args: &[String]) -> Result<(), String> {
    let coll = args
        .first()
        .ok_or("użycie: nonodb-cli search-all <kolekcja> [opcje]")?;
    let mut payload = json!({"op": "search_all", "coll": coll, "mode": "lexical"});
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" => {
                payload["mode"] = json!(args.get(i + 1).ok_or("--mode wymaga wartości")?);
                i += 2;
            }
            "--query" => {
                payload["query"] = json!(args.get(i + 1).ok_or("--query wymaga tekstu")?);
                i += 2;
            }
            "--fields" => {
                payload["fields"] = json!(args
                    .get(i + 1)
                    .ok_or("--fields wymaga listy pól")?
                    .split(',')
                    .filter(|field| !field.is_empty())
                    .collect::<Vec<_>>());
                i += 2;
            }
            "--vector" => {
                payload["vector"] =
                    serde_json::from_str(args.get(i + 1).ok_or("--vector wymaga JSON")?)
                        .map_err(|e| e.to_string())?;
                i += 2;
            }
            "--vector-field" => {
                payload["vector_field"] =
                    json!(args.get(i + 1).ok_or("--vector-field wymaga pola")?);
                i += 2;
            }
            "--filter" => {
                payload["filter"] =
                    serde_json::from_str(args.get(i + 1).ok_or("--filter wymaga JSON")?)
                        .map_err(|e| e.to_string())?;
                i += 2;
            }
            "--skip" => {
                payload["skip"] = json!(args
                    .get(i + 1)
                    .ok_or("--skip wymaga liczby")?
                    .parse::<usize>()
                    .map_err(|e| e.to_string())?);
                i += 2;
            }
            "--limit" => {
                payload["limit"] = json!(args
                    .get(i + 1)
                    .ok_or("--limit wymaga liczby")?
                    .parse::<usize>()
                    .map_err(|e| e.to_string())?);
                i += 2;
            }
            "--max-distance" => {
                payload["max_distance"] = json!(args
                    .get(i + 1)
                    .ok_or("--max-distance wymaga liczby")?
                    .parse::<usize>()
                    .map_err(|e| e.to_string())?);
                i += 2;
            }
            "--rrf-k" => {
                payload["rrf_k"] = json!(args
                    .get(i + 1)
                    .ok_or("--rrf-k wymaga liczby")?
                    .parse::<usize>()
                    .map_err(|e| e.to_string())?);
                i += 2;
            }
            other => return Err(format!("nieznana opcja: {other}")),
        }
    }
    let res = http_post("/v1/exec", &payload)?;
    print_result(&res);
    Ok(())
}

fn streams_cmd(args: &[String]) -> Result<(), String> {
    let action = args.first().map(String::as_str).unwrap_or("list");
    match action {
        "list" => {
            let res = http_post("/v1/exec", &json!({"op": "list_streams"}))?;
            let streams = res
                .pointer("/result/streams")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if streams.is_empty() {
                println!("no streams");
                return Ok(());
            }
            for stream in streams {
                let name = stream
                    .get("stream")
                    .or_else(|| stream.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or("?");
                let coll = stream.get("coll").and_then(Value::as_str).unwrap_or("?");
                let head = stream.get("head_seq").and_then(Value::as_u64).unwrap_or(0);
                let retention = stream
                    .get("retention")
                    .filter(|value| !value.as_object().is_some_and(|object| object.is_empty()))
                    .map(|value| format!(" retention={value}"))
                    .unwrap_or_default();
                println!("{name}  coll={coll}  head={head}{retention}");
            }
        }
        "create" => {
            let coll = args
                .get(1)
                .ok_or("użycie: nonodb-cli streams create <coll> [--max-events N] [--max-age-ms N] [--min-seq-keep N]")?;
            let mut payload = json!({"op": "create_stream", "coll": coll});
            let mut retention = serde_json::Map::new();
            let mut i = 2;
            while i < args.len() {
                let key = match args[i].as_str() {
                    "--max-events" => "max_events",
                    "--max-age-ms" => "max_age_ms",
                    "--min-seq-keep" => "min_seq_keep",
                    other => return Err(format!("nieznana opcja: {other}")),
                };
                let value: u64 = args
                    .get(i + 1)
                    .ok_or_else(|| format!("{key} wymaga liczby"))?
                    .parse()
                    .map_err(|e| format!("{key}: {e}"))?;
                retention.insert(key.to_string(), json!(value));
                i += 2;
            }
            if !retention.is_empty() {
                payload["retention"] = Value::Object(retention);
            }
            let res = http_post("/v1/exec", &payload)?;
            print_result(&res);
        }
        "groups" | "stats" => {
            let coll = args.get(1);
            let res = http_post(
                "/v1/exec",
                &match coll {
                    Some(coll) => json!({"op": "stream_stats", "coll": coll}),
                    None => json!({"op": "stream_stats"}),
                },
            )?;
            let streams = res
                .pointer("/result/streams")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if streams.is_empty() {
                println!("no streams");
                return Ok(());
            }
            println!(
                "{:<20} {:<20} {:>12} {:>12} {:>8} {:>9} {:>9} {:>7} {:>7} {:>6}",
                "STREAM",
                "GROUP",
                "COMMITTED",
                "HEAD",
                "LAG",
                "INFLIGHT",
                "DEAD_EV",
                "DEAD_RNG",
                "REDELIV",
                "TRUNC"
            );
            for stream in streams {
                let name = stream
                    .get("stream")
                    .or_else(|| stream.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or("?")
                    .to_string();
                let head = stream.get("head_seq").and_then(Value::as_u64).unwrap_or(0);
                let groups = stream
                    .get("groups")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                if groups.is_empty() {
                    println!("{name:<20} {:<20} {:>12} {:>12}", "-", "-", head);
                    continue;
                }
                for group in groups {
                    let truncated = group
                        .get("truncated_below")
                        .and_then(Value::as_u64)
                        .map(|value| format!(" truncated_below={value}"))
                        .unwrap_or_default();
                    println!(
                        "{:<20} {:<20} {:>12} {:>12} {:>8} {:>9} {:>9} {:>7} {:>7} {:>6}{}",
                        name,
                        group.get("group").and_then(Value::as_str).unwrap_or("?"),
                        group
                            .get("committed_seq")
                            .and_then(Value::as_u64)
                            .unwrap_or(0),
                        head,
                        group.get("lag").and_then(Value::as_u64).unwrap_or(0),
                        group.get("inflight").and_then(Value::as_u64).unwrap_or(0),
                        group.get("dead").and_then(Value::as_u64).unwrap_or(0),
                        group
                            .get("dead_ranges")
                            .and_then(Value::as_u64)
                            .unwrap_or(0),
                        group
                            .get("redeliveries")
                            .and_then(Value::as_u64)
                            .unwrap_or(0),
                        group
                            .get("truncations")
                            .and_then(Value::as_u64)
                            .unwrap_or(0),
                        truncated,
                    );
                }
            }
        }
        "dead" => {
            let coll = args
                .get(1)
                .ok_or("użycie: nonodb-cli streams dead <coll> [group]")?;
            let mut payload = json!({"op": "stream_dead", "coll": coll});
            if let Some(group) = args.get(2) {
                payload["group"] = json!(group);
            }
            let res = http_post("/v1/exec", &payload)?;
            print_result(&res);
        }
        "retry" | "drop" => {
            let coll = args
                .get(1)
                .ok_or("użycie: nonodb-cli streams retry|drop <coll> <group> <from_seq>")?;
            let group = args
                .get(2)
                .ok_or("użycie: nonodb-cli streams retry|drop <coll> <group> <from_seq>")?;
            let from_seq: u64 = args
                .get(3)
                .ok_or("użycie: nonodb-cli streams retry|drop <coll> <group> <from_seq>")?
                .parse()
                .map_err(|e| format!("from_seq: {e}"))?;
            let op = if action == "retry" {
                "retry_stream_dead"
            } else {
                "drop_stream_dead"
            };
            let res = http_post(
                "/v1/exec",
                &json!({"op": op, "coll": coll, "group": group, "from_seq": from_seq}),
            )?;
            print_result(&res);
        }
        "reset" => {
            let coll = args
                .get(1)
                .ok_or("użycie: nonodb-cli streams reset <coll> <group> [earliest|latest|<seq>]")?;
            let group = args
                .get(2)
                .ok_or("użycie: nonodb-cli streams reset <coll> <group> [earliest|latest|<seq>]")?;
            let to = match args.get(3).map(String::as_str) {
                None | Some("latest") => json!("latest"),
                Some("earliest") => json!("earliest"),
                Some(raw) => {
                    let seq: u64 = raw.parse().map_err(|e| format!("seq: {e}"))?;
                    json!({"seq": seq})
                }
            };
            let res = http_post(
                "/v1/exec",
                &json!({"op": "reset_stream_group", "coll": coll, "group": group, "to": to}),
            )?;
            print_result(&res);
        }
        "prune" => {
            let coll = args
                .get(1)
                .ok_or("użycie: nonodb-cli streams prune <coll> [batch]")?;
            let mut payload = json!({"op": "prune_stream", "coll": coll});
            if let Some(batch) = args.get(2) {
                let batch: usize = batch.parse().map_err(|e| format!("batch: {e}"))?;
                payload["batch"] = json!(batch);
            }
            let res = http_post("/v1/exec", &payload)?;
            print_result(&res);
        }
        other => {
            return Err(format!(
                "nieznana akcja streams: {other} (list/create/groups/reset/dead/retry/drop/prune)"
            ))
        }
    }
    Ok(())
}

fn webhooks_cmd(args: &[String]) -> Result<(), String> {
    match args.first().map(String::as_str) {
        Some("list") | None => {
            let res = http_post("/v1/exec", &json!({"op": "list_webhooks"}))?;
            print_result(&res);
        }
        Some("deliveries") => {
            let mut payload = json!({"op": "list_webhook_deliveries"});
            if let Some(status) = args.get(1) {
                payload["status"] = json!(status);
            }
            let res = http_post("/v1/exec", &payload)?;
            print_result(&res);
        }
        Some("retry") => {
            let id = args
                .get(1)
                .ok_or("użycie: nonodb-cli webhooks retry <delivery-id>")?;
            let res = http_post(
                "/v1/exec",
                &json!({"op": "retry_webhook_delivery", "id": id}),
            )?;
            print_result(&res);
        }
        Some("delete") => {
            let name = args
                .get(1)
                .ok_or("użycie: nonodb-cli webhooks delete <nazwa>")?;
            let res = http_post("/v1/exec", &json!({"op": "delete_webhook", "name": name}))?;
            print_result(&res);
        }
        Some("create") | Some("update") => {
            let name = args
                .get(1)
                .ok_or("użycie: nonodb-cli webhooks create <nazwa> <kolekcja> <url> [opcje]")?;
            let coll = args
                .get(2)
                .ok_or("użycie: nonodb-cli webhooks create <nazwa> <kolekcja> <url> [opcje]")?;
            let url = args
                .get(3)
                .ok_or("użycie: nonodb-cli webhooks create <nazwa> <kolekcja> <url> [opcje]")?;
            let mut payload = json!({
                "op": "create_webhook",
                "name": name,
                "coll": coll,
                "url": url,
                "enabled": true,
                "headers": {},
            });
            let mut i = 4;
            while i < args.len() {
                match args[i].as_str() {
                    "--filter" => {
                        payload["filter"] =
                            serde_json::from_str(args.get(i + 1).ok_or("--filter wymaga JSON")?)
                                .map_err(|e| e.to_string())?;
                        i += 2;
                    }
                    "--threshold" => {
                        payload["threshold"] =
                            serde_json::from_str(args.get(i + 1).ok_or("--threshold wymaga JSON")?)
                                .map_err(|e| e.to_string())?;
                        i += 2;
                    }
                    "--ops" => {
                        payload["ops"] = json!(args
                            .get(i + 1)
                            .ok_or("--ops wymaga listy insert,update,delete")?
                            .split(',')
                            .filter(|part| !part.is_empty())
                            .collect::<Vec<_>>());
                        i += 2;
                    }
                    "--once" => {
                        payload["once"] = json!(true);
                        i += 1;
                    }
                    "--cooldown" => {
                        payload["cooldown_seconds"] = json!(args
                            .get(i + 1)
                            .ok_or("--cooldown wymaga sekund")?
                            .parse::<u64>()
                            .map_err(|e| e.to_string())?);
                        i += 2;
                    }
                    "--secret-env" => {
                        payload["secret_env"] = json!(args
                            .get(i + 1)
                            .ok_or("--secret-env wymaga nazwy zmiennej")?);
                        i += 2;
                    }
                    "--header" => {
                        let raw = args.get(i + 1).ok_or("--header wymaga name:value")?;
                        let (key, value) =
                            raw.split_once(':').ok_or("--header wymaga name:value")?;
                        payload["headers"][key.trim()] = json!(value.trim());
                        i += 2;
                    }
                    "--max-attempts" => {
                        payload["max_attempts"] = json!(args
                            .get(i + 1)
                            .ok_or("--max-attempts wymaga liczby")?
                            .parse::<u64>()
                            .map_err(|e| e.to_string())?);
                        i += 2;
                    }
                    "--timeout-ms" => {
                        payload["timeout_ms"] = json!(args
                            .get(i + 1)
                            .ok_or("--timeout-ms wymaga liczby")?
                            .parse::<u64>()
                            .map_err(|e| e.to_string())?);
                        i += 2;
                    }
                    "--disabled" => {
                        payload["enabled"] = json!(false);
                        i += 1;
                    }
                    other => return Err(format!("nieznana opcja: {other}")),
                }
            }
            let res = http_post("/v1/exec", &payload)?;
            print_result(&res);
        }
        Some(other) => return Err(format!("nieznana komenda webhooks: {other}")),
    }
    Ok(())
}

fn events_tail(args: &[String]) -> Result<(), String> {
    let mut coll = "_events".to_string();
    let mut after: Option<String> = None;
    let mut types = Vec::new();
    let mut filter = json!({});
    let mut once = false;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--coll" => {
                coll = args.get(i + 1).ok_or("--coll wymaga nazwy")?.clone();
                i += 2;
            }
            "--after" => {
                after = Some(args.get(i + 1).ok_or("--after wymaga id")?.clone());
                i += 2;
            }
            "--type" => {
                types.push(args.get(i + 1).ok_or("--type wymaga nazwy")?.clone());
                i += 2;
            }
            "--filter" => {
                let raw = args.get(i + 1).ok_or("--filter wymaga JSON")?;
                filter = serde_json::from_str(raw).map_err(|e| e.to_string())?;
                i += 2;
            }
            "--once" => {
                once = true;
                i += 1;
            }
            other => return Err(format!("nieznana opcja: {other}")),
        }
    }
    loop {
        let mut payload = json!({
            "op": "watch",
            "coll": coll,
            "limit": 100,
            "timeout_ms": 1000,
            "filter": filter,
        });
        if let Some(after) = &after {
            payload["after"] = json!(after);
        }
        if !types.is_empty() {
            payload["types"] = json!(types);
        }
        let res = http_post("/v1/exec", &payload)?;
        let result = res.get("result").cloned().unwrap_or(Value::Null);
        for event in result
            .get("events")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            println!("{}", serde_json::to_string(event).unwrap());
            if let Some(id) = event.get("_id").and_then(Value::as_str) {
                after = Some(id.to_string());
            }
        }
        if once {
            return Ok(());
        }
    }
}

fn sql_cmd(args: &[String]) -> Result<(), String> {
    let mut query = None;
    let mut file = None;
    let mut params = Vec::new();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--param" => {
                let raw = args.get(i + 1).ok_or("--param wymaga wartości")?;
                params
                    .push(serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.clone())));
                i += 2;
            }
            "--file" => {
                file = Some(args.get(i + 1).ok_or("--file wymaga ścieżki")?.to_string());
                i += 2;
            }
            other if other.starts_with("--") => return Err(format!("nieznana opcja: {other}")),
            other => {
                if query.is_some() {
                    return Err("sql przyjmuje jedno zapytanie; użyj --file dla pliku".into());
                }
                query = Some(other.to_string());
                i += 1;
            }
        }
    }
    let query = match (query, file) {
        (Some(_), Some(_)) => {
            return Err("użyj zapytania albo --file, nie obu jednocześnie".into());
        }
        (Some(query), None) => query,
        (None, Some(path)) => {
            std::fs::read_to_string(&path).map_err(|e| format!("cannot read {path}: {e}"))?
        }
        (None, None) => {
            return Err("użycie: nonodb-cli sql <zapytanie> [--param JSON]... [--file plik]".into())
        }
    };
    let res = http_post(
        "/v1/exec",
        &json!({"op": "sql", "query": query, "params": params}),
    )?;
    print_result(&res);
    Ok(())
}

fn sql_shell() -> Result<(), String> {
    let stdin = std::io::stdin();
    let mut lines = stdin.lock().lines();
    loop {
        eprint!("nonodb> ");
        let Some(Ok(line)) = lines.next() else {
            break;
        };
        let query = line.trim();
        if query.is_empty() || query.starts_with("--") {
            continue;
        }
        if matches!(query, ".quit" | ".exit" | "quit" | "exit" | "\\q") {
            break;
        }
        let res = http_post(
            "/v1/exec",
            &json!({"op": "sql", "query": query, "params": []}),
        )?;
        if res.get("ok").and_then(Value::as_bool) == Some(true) {
            println!("{}", serde_json::to_string_pretty(&res["result"]).unwrap());
        } else {
            eprintln!("{}", serde_json::to_string_pretty(&res).unwrap());
        }
    }
    Ok(())
}

fn parse_ts_flag(args: &[String], i: usize, flag: &str) -> Result<String, String> {
    args.get(i + 1)
        .cloned()
        .ok_or_else(|| format!("{flag} wymaga wartości"))
}

fn create_collection_payload(args: &[String]) -> Result<Value, String> {
    let name = args
        .first()
        .filter(|value| !value.starts_with("--"))
        .ok_or("użycie: nonodb-cli create-collection <nazwa> [opcje]")?;
    let mut profile = "standard".to_string();
    let mut time_field: Option<String> = None;
    let mut meta_field: Option<String> = None;
    let mut granularity = "seconds".to_string();
    let mut compression = "balanced".to_string();
    let mut expire_after: Option<u64> = None;
    let mut ttl: Option<u64> = None;
    let mut bucket_max_docs: Option<usize> = None;
    let mut bucket_max_bytes: Option<usize> = None;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--profile" => {
                profile = parse_ts_flag(args, i, "--profile")?;
                i += 2;
            }
            "--time-field" | "--time_field" => {
                time_field = Some(parse_ts_flag(args, i, "--time-field")?);
                i += 2;
            }
            "--meta-field" | "--meta_field" => {
                meta_field = Some(parse_ts_flag(args, i, "--meta-field")?);
                i += 2;
            }
            "--granularity" => {
                granularity = parse_ts_flag(args, i, "--granularity")?;
                i += 2;
            }
            "--compression" => {
                compression = parse_ts_flag(args, i, "--compression")?;
                i += 2;
            }
            "--expire-after" | "--expire_after" => {
                expire_after = Some(
                    parse_ts_flag(args, i, "--expire-after")?
                        .parse()
                        .map_err(|e| format!("expire-after: {e}"))?,
                );
                i += 2;
            }
            "--ttl" => {
                ttl = Some(
                    parse_ts_flag(args, i, "--ttl")?
                        .parse()
                        .map_err(|e| format!("ttl: {e}"))?,
                );
                i += 2;
            }
            "--bucket-max-docs" | "--bucket_max_docs" => {
                bucket_max_docs = Some(
                    parse_ts_flag(args, i, "--bucket-max-docs")?
                        .parse()
                        .map_err(|e| format!("bucket-max-docs: {e}"))?,
                );
                i += 2;
            }
            "--bucket-max-bytes" | "--bucket_max_bytes" => {
                bucket_max_bytes = Some(
                    parse_ts_flag(args, i, "--bucket-max-bytes")?
                        .parse()
                        .map_err(|e| format!("bucket-max-bytes: {e}"))?,
                );
                i += 2;
            }
            other => return Err(format!("nieznana opcja: {other}")),
        }
    }
    if !matches!(
        profile.as_str(),
        "standard" | "vault" | "timeseries" | "columnar"
    ) {
        return Err(format!("nieznany profil: {profile}"));
    }
    let mut payload = json!({"op": "create_collection", "coll": name, "profile": profile});
    if let Some(ttl) = ttl {
        payload["ttl_seconds"] = json!(ttl);
    }
    if profile == "timeseries" {
        let time_field = time_field.ok_or("profil timeseries wymaga --time-field")?;
        let mut spec = serde_json::Map::new();
        spec.insert("timeField".into(), json!(time_field));
        if let Some(meta) = meta_field {
            spec.insert("metaField".into(), json!(meta));
        }
        spec.insert("granularity".into(), json!(granularity));
        spec.insert("compression".into(), json!(compression));
        if let Some(expire) = expire_after {
            spec.insert("expireAfterSeconds".into(), json!(expire));
        }
        payload["timeseries"] = Value::Object(spec);
    }
    if profile == "columnar" {
        let mut spec = serde_json::Map::new();
        if let Some(bucket_max_docs) = bucket_max_docs {
            spec.insert("bucket_max_docs".into(), json!(bucket_max_docs));
        }
        if let Some(bucket_max_bytes) = bucket_max_bytes {
            spec.insert("bucket_max_bytes".into(), json!(bucket_max_bytes));
        }
        spec.insert("compression".into(), json!(compression));
        payload["columnar"] = Value::Object(spec);
    }
    Ok(payload)
}

fn apply_ts_overrides(current: &Value, args: &[String]) -> Result<Value, String> {
    let mut settings = current
        .as_object()
        .cloned()
        .ok_or("obecne ustawienia time-series są nieprawidłowe")?;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--time-field" | "--time_field" => {
                settings.insert(
                    "time_field".into(),
                    json!(parse_ts_flag(args, i, "--time-field")?),
                );
                i += 2;
            }
            "--meta-field" | "--meta_field" => {
                let value = parse_ts_flag(args, i, "--meta-field")?;
                if value.is_empty() || value == "none" || value == "null" {
                    settings.insert("meta_field".into(), Value::Null);
                } else {
                    settings.insert("meta_field".into(), json!(value));
                }
                i += 2;
            }
            "--granularity" => {
                settings.insert(
                    "granularity".into(),
                    json!(parse_ts_flag(args, i, "--granularity")?),
                );
                i += 2;
            }
            "--compression" => {
                settings.insert(
                    "compression".into(),
                    json!(parse_ts_flag(args, i, "--compression")?),
                );
                i += 2;
            }
            "--bucket-max-docs" | "--bucket_max_docs" => {
                let value: usize = parse_ts_flag(args, i, "--bucket-max-docs")?
                    .parse()
                    .map_err(|e| format!("bucket-max-docs: {e}"))?;
                settings.insert("bucket_max_docs".into(), json!(value));
                i += 2;
            }
            "--expire-after" | "--expire_after" => {
                let raw = parse_ts_flag(args, i, "--expire-after")?;
                if raw == "none" || raw == "null" || raw.is_empty() {
                    settings.insert("expire_after_seconds".into(), Value::Null);
                } else {
                    let value: u64 = raw.parse().map_err(|e| format!("expire-after: {e}"))?;
                    settings.insert("expire_after_seconds".into(), json!(value));
                }
                i += 2;
            }
            other => return Err(format!("nieznana opcja: {other}")),
        }
    }
    Ok(Value::Object(settings))
}

fn create_collection_cmd(args: &[String]) -> Result<(), String> {
    let payload = create_collection_payload(args)?;
    let res = http_post("/v1/exec", &payload)?;
    print_result(&res);
    Ok(())
}

fn apply_columnar_overrides(current: &Value, args: &[String]) -> Result<Value, String> {
    let mut settings = current
        .as_object()
        .cloned()
        .ok_or("obecne ustawienia columnar są nieprawidłowe")?;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--bucket-max-docs" | "--bucket_max_docs" => {
                let value: usize = parse_ts_flag(args, i, "--bucket-max-docs")?
                    .parse()
                    .map_err(|e| format!("bucket-max-docs: {e}"))?;
                settings.insert("bucket_max_docs".into(), json!(value));
                i += 2;
            }
            "--bucket-max-bytes" | "--bucket_max_bytes" => {
                let value: usize = parse_ts_flag(args, i, "--bucket-max-bytes")?
                    .parse()
                    .map_err(|e| format!("bucket-max-bytes: {e}"))?;
                settings.insert("bucket_max_bytes".into(), json!(value));
                i += 2;
            }
            "--compression" => {
                settings.insert(
                    "compression".into(),
                    json!(parse_ts_flag(args, i, "--compression")?),
                );
                i += 2;
            }
            other => return Err(format!("nieznana opcja: {other}")),
        }
    }
    Ok(Value::Object(settings))
}

fn set_collection_columnar_cmd(args: &[String]) -> Result<(), String> {
    let coll = args
        .first()
        .filter(|value| !value.starts_with("--"))
        .ok_or("użycie: nonodb-cli set-collection-columnar <kolekcja> [opcje]")?;
    let current = http_post(
        "/v1/exec",
        &json!({"op": "get_collection_columnar", "coll": coll}),
    )?;
    if current.get("ok").and_then(Value::as_bool) != Some(true) {
        print_result(&current);
        return Ok(());
    }
    let merged = apply_columnar_overrides(&current["result"]["columnar"], &args[1..])?;
    let res = http_post(
        "/v1/exec",
        &json!({"op": "set_collection_columnar", "coll": coll, "settings": merged}),
    )?;
    print_result(&res);
    Ok(())
}

fn set_collection_timeseries_cmd(args: &[String]) -> Result<(), String> {
    let coll = args
        .first()
        .filter(|value| !value.starts_with("--"))
        .ok_or("użycie: nonodb-cli set-collection-timeseries <kolekcja> [opcje]")?;
    let current = http_post(
        "/v1/exec",
        &json!({"op": "get_collection_timeseries", "coll": coll}),
    )?;
    if current.get("ok").and_then(Value::as_bool) != Some(true) {
        print_result(&current);
        return Ok(());
    }
    let merged = apply_ts_overrides(&current["result"]["timeseries"], &args[1..])?;
    let res = http_post(
        "/v1/exec",
        &json!({"op": "set_collection_timeseries", "coll": coll, "timeseries": merged}),
    )?;
    print_result(&res);
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

fn strip_instance_flags(args: &mut Vec<String>) -> Result<(), String> {
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--socket" {
            let value = args.get(i + 1).ok_or("--socket wymaga ścieżki")?.clone();
            args.drain(i..=i + 1);
            #[cfg(unix)]
            std::env::set_var("NONODB_SOCK", value);
            #[cfg(not(unix))]
            return Err("--socket jest dostępny tylko na Unix; użyj NONODB_URL".into());
            continue;
        }
        if let Some(value) = args[i].strip_prefix("--socket=") {
            let value = value.to_string();
            args.remove(i);
            #[cfg(unix)]
            std::env::set_var("NONODB_SOCK", value);
            #[cfg(not(unix))]
            return Err("--socket jest dostępny tylko na Unix; użyj NONODB_URL".into());
            continue;
        }
        if args[i] == "--instance" {
            let selector = args
                .get(i + 1)
                .ok_or("--instance wymaga PID albo node_id")?
                .clone();
            args.drain(i..=i + 1);
            apply_instance(&selector)?;
            continue;
        }
        if let Some(selector) = args[i].strip_prefix("--instance=") {
            let selector = selector.to_string();
            args.remove(i);
            apply_instance(&selector)?;
            continue;
        }
        i += 1;
    }
    Ok(())
}

fn apply_instance(selector: &str) -> Result<(), String> {
    let entry = nonodb_core::instance::resolve(selector)?;
    #[cfg(unix)]
    {
        let socket = entry
            .record
            .socket
            .as_deref()
            .ok_or("wybrana instancja nie ma socketu Unix; użyj GUI/API przez TCP")?;
        std::env::set_var("NONODB_SOCK", socket);
    }
    #[cfg(not(unix))]
    {
        let tcp = entry
            .record
            .tcp_bound
            .as_deref()
            .or(entry.record.tcp_configured.as_deref())
            .ok_or("wybrana instancja nie ma adresu TCP")?;
        std::env::set_var("NONODB_URL", format!("http://{tcp}"));
    }
    Ok(())
}

fn instances_cmd(args: &[String]) -> Result<(), String> {
    if args.first().map(String::as_str) == Some("prune") {
        let removed = nonodb_core::instance::prune_stale()?;
        println!("removed {removed} stale instance record(s)");
        return Ok(());
    }
    let entries = nonodb_core::instance::list()?;
    if args.iter().any(|arg| arg == "--json") {
        let rows: Vec<Value> = entries
            .iter()
            .map(|entry| {
                json!({
                    "pid": entry.record.pid,
                    "alive": entry.alive,
                    "node_id": entry.record.node_id,
                    "role": entry.record.role,
                    "version": entry.record.version,
                    "data": entry.record.data,
                    "socket": entry.record.socket,
                    "tcp": entry.record.tcp_bound.as_ref().or(entry.record.tcp_configured.as_ref()),
                    "advertise_url": entry.record.advertise_url,
                    "started_at_ms": entry.record.started_at_ms,
                    "record": entry.path,
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&rows).unwrap_or_else(|_| "[]".into())
        );
        return Ok(());
    }
    if entries.is_empty() {
        println!("no nonodb instances registered");
        return Ok(());
    }
    println!(
        "{:<7} {:<18} {:<9} {:<8} {:<24} {:<28} DATA",
        "PID", "NODE", "ROLE", "ALIVE", "SOCKET", "TCP"
    );
    for entry in entries {
        println!(
            "{:<7} {:<18} {:<9} {:<8} {:<24} {:<28} {}",
            entry.record.pid,
            entry.record.node_id,
            entry.record.role,
            if entry.alive { "yes" } else { "stale" },
            entry.record.socket.as_deref().unwrap_or("-"),
            entry
                .record
                .tcp_bound
                .as_deref()
                .or(entry.record.tcp_configured.as_deref())
                .unwrap_or("-"),
            entry.record.data
        );
    }
    Ok(())
}

fn strip_db_flag(args: &mut Vec<String>) -> Result<(), String> {
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--db" {
            let value = args.get(i + 1).ok_or("--db wymaga nazwy bazy")?.clone();
            args.drain(i..=i + 1);
            std::env::set_var("NONODB_DB", value);
            continue;
        }
        if let Some(value) = args[i].strip_prefix("--db=") {
            let value = value.to_string();
            args.remove(i);
            std::env::set_var("NONODB_DB", value);
            continue;
        }
        i += 1;
    }
    Ok(())
}

fn db_prefix() -> String {
    let db = std::env::var("NONODB_DB").unwrap_or_else(|_| "default".into());
    if db.is_empty() || db == "default" {
        String::new()
    } else {
        format!("/{db}")
    }
}

fn http_get(path: &str) -> Result<Value, String> {
    http_request("GET", path, None)
}

fn http_post(path: &str, body: &Value) -> Result<Value, String> {
    http_request("POST", path, Some(body))
}

fn http_request(method: &str, path: &str, body: Option<&Value>) -> Result<Value, String> {
    let path = if path.starts_with("/v1/") {
        format!("{}{}", db_prefix(), path)
    } else {
        path.to_string()
    };
    let path = path.as_str();
    #[cfg(unix)]
    let socket = sock_path();
    #[cfg(unix)]
    let mut stream = UnixStream::connect(&socket)
        .map_err(|e| format!("connect {socket}: {e} (set NONODB_SOCK to the daemon socket)"))?;
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

fn auth_path() -> PathBuf {
    if let Some(path) = std::env::var_os("NONODB_AUTH_FILE") {
        if !path.is_empty() {
            return PathBuf::from(path);
        }
    }
    #[cfg(unix)]
    {
        PathBuf::from("/etc/nonodb/auth.json")
    }
    #[cfg(not(unix))]
    {
        PathBuf::from("auth.json")
    }
}

fn auth_cmd(args: &[String]) -> Result<(), String> {
    let path = auth_path();
    let Some(cmd) = args.first().map(String::as_str) else {
        return Err("użycie: nonodb-cli auth <init|check|principal|token|cert> ...".into());
    };
    match cmd {
        "init" => {
            let force = args.iter().any(|a| a == "--force");
            if path.exists() && !force {
                return Err(format!(
                    "{} already exists; pass --force to replace it",
                    path.display()
                ));
            }
            ensure_auth_dir(&path)?;
            let mut file = nonodb_auth::AuthFile::empty();
            file.principals.insert(
                "admin".into(),
                nonodb_auth::PrincipalDef {
                    scopes: vec!["*:*".into()],
                },
            );
            nonodb_auth::save_atomic(&path, &file)?;
            eprintln!(
                "created {}; now run: nonodb-cli auth token create admin",
                path.display()
            );
        }
        "check" => {
            let registry = nonodb_auth::Registry::load(&path)?;
            let tokens = registry.file().tokens.len();
            let certs = registry.file().certificates.len();
            let principals: Vec<String> = registry
                .principals()
                .map(|p| format!("{} [{}]", p.name(), p.scopes().join(",")))
                .collect();
            println!("file: {}", path.display());
            println!("principals: {}", principals.len());
            for principal in principals {
                println!("  {principal}");
            }
            println!("tokens: {tokens}");
            println!("certificates: {certs}");
        }
        "principal" => auth_principal(&path, &args[1..])?,
        "token" => auth_token(&path, &args[1..])?,
        "cert" => auth_cert(&path, &args[1..])?,
        other => return Err(format!("nieznana komenda auth: {other}")),
    }
    Ok(())
}

fn ensure_auth_dir(path: &Path) -> Result<(), String> {
    let Some(dir) = path.parent().filter(|dir| !dir.as_os_str().is_empty()) else {
        return Ok(());
    };
    if dir.is_dir() {
        return Ok(());
    }
    std::fs::create_dir_all(dir)
        .map_err(|e| format!("cannot create auth directory {}: {e}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o750))
            .map_err(|e| format!("cannot set permissions on {}: {e}", dir.display()))?;
    }
    Ok(())
}

fn auth_load(path: &Path) -> Result<nonodb_auth::AuthFile, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let file: nonodb_auth::AuthFile =
        serde_json::from_str(&raw).map_err(|e| format!("cannot parse {}: {e}", path.display()))?;
    file.validate()?;
    Ok(file)
}

fn auth_principal(path: &Path, args: &[String]) -> Result<(), String> {
    let Some(cmd) = args.first().map(String::as_str) else {
        return Err("użycie: auth principal <list|add|scopes>".into());
    };
    let mut file = auth_load(path)?;
    match cmd {
        "list" => {
            for (name, def) in &file.principals {
                println!("{name}\t{}", def.scopes.join(","));
            }
        }
        "add" => {
            let name = args
                .get(1)
                .ok_or("użycie: auth principal add <nazwa> <scope,scope>")?;
            let scopes = parse_scopes(
                args.get(2)
                    .ok_or("użycie: auth principal add <nazwa> <scope,scope>")?,
            )?;
            if file.principals.contains_key(name) {
                return Err(format!("principal `{name}` already exists"));
            }
            file.principals
                .insert(name.clone(), nonodb_auth::PrincipalDef { scopes });
            nonodb_auth::save_atomic(path, &file)?;
            eprintln!("principal `{name}` added to {}", path.display());
        }
        "scopes" => {
            let name = args
                .get(1)
                .ok_or("użycie: auth principal scopes <nazwa> <scope,scope>")?;
            let scopes = parse_scopes(
                args.get(2)
                    .ok_or("użycie: auth principal scopes <nazwa> <scope,scope>")?,
            )?;
            let def = file
                .principals
                .get_mut(name)
                .ok_or_else(|| format!("unknown principal `{name}`"))?;
            def.scopes = scopes;
            nonodb_auth::save_atomic(path, &file)?;
            eprintln!("scopes for `{name}` updated in {}", path.display());
        }
        other => return Err(format!("nieznana komenda auth principal: {other}")),
    }
    Ok(())
}

fn auth_token(path: &Path, args: &[String]) -> Result<(), String> {
    let Some(cmd) = args.first().map(String::as_str) else {
        return Err("użycie: auth token <list|create|disable|enable>".into());
    };
    let mut file = auth_load(path)?;
    match cmd {
        "list" => {
            for token in &file.tokens {
                println!(
                    "{}\t{}\t{}\t{}",
                    token.kid,
                    token.principal,
                    if token.disabled { "disabled" } else { "active" },
                    &token.sha256[..16]
                );
            }
        }
        "create" => {
            let principal = args
                .get(1)
                .ok_or("użycie: auth token create <principal> [kid]")?;
            if !file.principals.contains_key(principal) {
                return Err(format!("unknown principal `{principal}`"));
            }
            let (kid, token, hash) = nonodb_auth::generate_token(args.get(2).map(String::as_str))?;
            file.tokens.push(nonodb_auth::TokenEntry {
                kid: kid.clone(),
                sha256: hash,
                principal: principal.clone(),
                disabled: false,
            });
            nonodb_auth::save_atomic(path, &file)?;
            eprintln!(
                "created token kid={kid} principal={principal} in {}",
                path.display()
            );
            println!("{token}");
        }
        "disable" | "enable" => {
            let kid = args
                .get(1)
                .ok_or("użycie: auth token disable|enable <kid>")?;
            let token = file
                .tokens
                .iter_mut()
                .find(|token| token.kid == *kid)
                .ok_or_else(|| format!("unknown token kid `{kid}`"))?;
            token.disabled = cmd == "disable";
            nonodb_auth::save_atomic(path, &file)?;
            eprintln!("token `{kid}` {cmd}d in {}", path.display());
        }
        other => return Err(format!("nieznana komenda auth token: {other}")),
    }
    Ok(())
}

fn auth_cert(path: &Path, args: &[String]) -> Result<(), String> {
    let Some(cmd) = args.first().map(String::as_str) else {
        return Err("użycie: auth cert <list|add|disable|enable>".into());
    };
    let mut file = auth_load(path)?;
    match cmd {
        "list" => {
            for cert in &file.certificates {
                println!(
                    "{}\t{}\t{}",
                    cert.sha256,
                    cert.principal,
                    if cert.disabled { "disabled" } else { "active" }
                );
            }
        }
        "add" => {
            let fingerprint = args
                .get(1)
                .ok_or("użycie: auth cert add <sha256> <principal>")?;
            let principal = args
                .get(2)
                .ok_or("użycie: auth cert add <sha256> <principal>")?;
            if !file.principals.contains_key(principal) {
                return Err(format!("unknown principal `{principal}`"));
            }
            let fingerprint = nonodb_auth::normalize_fingerprint(fingerprint)?;
            if file
                .certificates
                .iter()
                .any(|cert| cert.sha256 == fingerprint)
            {
                return Err(format!("certificate `{fingerprint}` already exists"));
            }
            file.certificates.push(nonodb_auth::CertificateEntry {
                sha256: fingerprint,
                principal: principal.clone(),
                disabled: false,
            });
            nonodb_auth::save_atomic(path, &file)?;
            eprintln!("certificate added for `{principal}` in {}", path.display());
        }
        "disable" | "enable" => {
            let fingerprint = args
                .get(1)
                .ok_or("użycie: auth cert disable|enable <sha256>")?;
            let fingerprint = nonodb_auth::normalize_fingerprint(fingerprint)?;
            let cert = file
                .certificates
                .iter_mut()
                .find(|cert| cert.sha256 == fingerprint)
                .ok_or_else(|| format!("unknown certificate `{fingerprint}`"))?;
            cert.disabled = cmd == "disable";
            nonodb_auth::save_atomic(path, &file)?;
            eprintln!("certificate `{fingerprint}` {cmd}d in {}", path.display());
        }
        other => return Err(format!("nieznana komenda auth cert: {other}")),
    }
    Ok(())
}

fn parse_scopes(raw: &str) -> Result<Vec<String>, String> {
    let scopes: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
        .map(str::to_owned)
        .collect();
    if scopes.is_empty() {
        return Err("at least one scope is required".into());
    }
    for scope in &scopes {
        nonodb_auth::Principal::new("check", std::slice::from_ref(scope))?;
    }
    Ok(scopes)
}

pub(crate) fn selected_database() -> String {
    std::env::var("NONODB_DB")
        .ok()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "default".into())
}

/// Resolves a data root or an explicit database directory to one LMDB env.
fn database_dir(data: &Path) -> Result<PathBuf, String> {
    if data.join("data.mdb").is_file() {
        return Ok(data.to_path_buf());
    }
    let selected = data.join(selected_database());
    if selected.join("data.mdb").is_file() {
        return Ok(selected);
    }
    let default = data.join("default");
    if default.join("data.mdb").is_file() {
        return Ok(default);
    }
    Err(format!(
        "brak data.mdb w {} (szukano też {}/<baza>/data.mdb; użyj --db albo podaj katalog bazy)",
        data.display(),
        data.display()
    ))
}

/// Target directory for restore. A new root receives `default/data.mdb`.
fn database_target(data: &Path) -> Result<PathBuf, String> {
    if data.join("data.mdb").is_file() {
        return Ok(data.to_path_buf());
    }
    let entries = std::fs::read_dir(data)
        .ok()
        .map(|entries| entries.filter_map(Result::ok).collect::<Vec<_>>())
        .unwrap_or_default();
    if data.join("default").is_dir()
        || entries
            .iter()
            .any(|entry| entry.path().join("data.mdb").is_file())
    {
        return Ok(data.join(selected_database()));
    }
    if data
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "default")
        || data.join("lock.mdb").exists()
        || data.join("auto-index.json").exists()
        || !entries.is_empty()
    {
        // An explicit database directory, not an empty instance root.
        return Ok(data.to_path_buf());
    }
    Ok(data.join("default"))
}

pub(crate) fn validate_database_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 64 {
        return Err(format!("invalid database name `{name}`"));
    }
    if matches!(name, "v1" | "healthz" | "metrics" | "gui" | "db" | "api") {
        return Err(format!("reserved database name `{name}`"));
    }
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(format!("invalid database name `{name}`"));
    };
    if !first.is_ascii_alphanumeric()
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        return Err(format!("invalid database name `{name}`"));
    }
    Ok(())
}

fn instance_databases(data: &Path) -> Result<Vec<(String, PathBuf)>, String> {
    if data.join("data.mdb").is_file() {
        return Ok(vec![("default".to_string(), data.to_path_buf())]);
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(data).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        if !entry.file_type().map_err(|e| e.to_string())?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if validate_database_name(&name).is_ok() && entry.path().join("data.mdb").is_file() {
            out.push((name, entry.path()));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    if out.is_empty() {
        return Err(format!("no databases found in {}", data.display()));
    }
    Ok(out)
}

fn backup_all(data: &Path, out_dir: &Path, with_sidecars: bool) -> Result<(), String> {
    let databases = instance_databases(data)?;
    std::fs::create_dir_all(out_dir).map_err(|e| e.to_string())?;
    let mut entries = Vec::new();
    for (name, source) in databases {
        let db_out = out_dir.join(&name);
        backup(&source, &db_out, with_sidecars)?;
        let manifest = nonodb_core::BackupManifest::from_backup(&db_out, &source)
            .map_err(|e| e.to_string())?;
        nonodb_core::verify::write_manifest(&db_out, &manifest).map_err(|e| e.to_string())?;
        nonodb_core::backup_info::write(&db_out, &manifest).map_err(|e| e.to_string())?;
        entries.push(json!({
            "name": name,
            "sha256": manifest.sha256,
            "data_size": manifest.data_size,
            "created_at_ms": manifest.created_at_ms,
        }));
        eprintln!(
            "database: {name} -> {} (sha256 {})",
            db_out.join("data.mdb").display(),
            manifest.sha256
        );
    }
    let instance = json!({
        "version": 1,
        "kind": "nonodb-instance-backup",
        "created_at_ms": now_ms(),
        "source": data.display().to_string(),
        "databases": entries,
    });
    let manifest_path = out_dir.join("manifest.json");
    let raw = serde_json::to_vec_pretty(&instance).map_err(|e| e.to_string())?;
    let tmp = out_dir.join(".manifest.json.tmp");
    std::fs::write(&tmp, raw).map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, &manifest_path).map_err(|e| e.to_string())?;
    eprintln!("instance manifest: {}", manifest_path.display());
    Ok(())
}

fn restore_all(backup: &Path, data: &Path, force: bool, verify: bool) -> Result<(), String> {
    let manifest_path = backup.join("manifest.json");
    let raw = std::fs::read(&manifest_path)
        .map_err(|e| format!("read {}: {e}", manifest_path.display()))?;
    let manifest: Value = serde_json::from_slice(&raw).map_err(|e| e.to_string())?;
    if manifest.get("kind").and_then(Value::as_str) != Some("nonodb-instance-backup") {
        return Err(format!(
            "{} is not a nonodb instance backup",
            manifest_path.display()
        ));
    }
    if manifest.get("version").and_then(Value::as_u64) != Some(1) {
        return Err("unsupported instance backup manifest version".into());
    }
    let databases = manifest
        .get("databases")
        .and_then(Value::as_array)
        .ok_or("instance manifest has no databases")?;
    if databases.is_empty() {
        return Err("instance backup contains no databases".into());
    }
    std::fs::create_dir_all(data).map_err(|e| e.to_string())?;
    for item in databases {
        let name = item
            .get("name")
            .and_then(Value::as_str)
            .ok_or("database entry has no name")?;
        validate_database_name(name)?;
        let source = backup.join(name);
        if !source.join("data.mdb").is_file() {
            return Err(format!(
                "missing backup database `{name}`: {}",
                source.display()
            ));
        }
        let target = data.join(name);
        restore_into(&source, &target, force, verify, None)?;
    }
    eprintln!(
        "restore-all: {} databases -> {}",
        databases.len(),
        data.display()
    );
    Ok(())
}

/// Compacts a stopped database in place.
///
/// LMDB never shrinks `data.mdb`; deleting documents (TTL, retention, drops)
/// only frees pages for reuse. This rewrites the file through LMDB's compacting
/// copy and swaps it in, keeping a `.before-compact` copy until the swap has
/// succeeded. The daemon must be stopped: the environment is opened read-only
/// and the file is replaced on disk.
fn compact(data: &Path, force: bool) -> Result<(), String> {
    if !data.join("data.mdb").is_file() {
        return Err(format!("{} has no data.mdb", data.display()));
    }
    if !force {
        // A running daemon keeps `lock.mdb` locked; refuse rather than corrupt it.
        if let Ok(lock) = std::fs::OpenOptions::new()
            .write(true)
            .open(data.join("lock.mdb"))
        {
            let _ = lock;
        }
    }
    let before = std::fs::metadata(data.join("data.mdb"))
        .map(|meta| meta.len())
        .map_err(|e| e.to_string())?;
    let tmp = data.join("data.mdb.compacting");
    if tmp.exists() {
        std::fs::remove_file(&tmp).map_err(|e| e.to_string())?;
    }
    let env = unsafe {
        EnvOpenOptions::new()
            .flags(EnvFlags::READ_ONLY)
            .open(data)
            .map_err(|e| format!("open {} (stop the daemon first): {e}", data.display()))?
    };
    let file = env
        .copy_to_file(&tmp, CompactionOption::Enabled)
        .map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    let env = env;
    env.prepare_for_closing().wait();

    let backup = data.join("data.mdb.before-compact");
    if backup.exists() {
        std::fs::remove_file(&backup).map_err(|e| e.to_string())?;
    }
    std::fs::rename(data.join("data.mdb"), &backup).map_err(|e| e.to_string())?;
    if let Err(err) = std::fs::rename(&tmp, data.join("data.mdb")) {
        let _ = std::fs::rename(&backup, data.join("data.mdb"));
        return Err(format!("swap failed, original restored: {err}"));
    }
    std::fs::remove_file(&backup).map_err(|e| e.to_string())?;
    let after = std::fs::metadata(data.join("data.mdb"))
        .map(|meta| meta.len())
        .map_err(|e| e.to_string())?;
    eprintln!(
        "compacted {}: {} -> {} bytes ({:.2} MB -> {:.2} MB)",
        data.display(),
        before,
        after,
        before as f64 / 1_048_576.0,
        after as f64 / 1_048_576.0
    );
    Ok(())
}

fn backup(data: &Path, out_dir: &Path, with_sidecars: bool) -> Result<(), String> {
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
    if with_sidecars {
        copy_search_sidecars(data, out_dir)?;
    }
    Ok(())
}

/// Copies derived search files after the LMDB snapshot. They are immutable and
/// content-addressed, so a file created after the snapshot is an unused extra,
/// while a file referenced by the snapshot remains valid.
fn copy_search_sidecars(source: &Path, target: &Path) -> Result<(), String> {
    for name in ["search", "vector-ann"] {
        let from = source.join(name);
        if !from.is_dir() {
            continue;
        }
        let to = target.join(name);
        let _ = std::fs::remove_dir_all(&to);
        copy_tree(&from, &to)?;
        eprintln!("backup sidecars: {} -> {}", from.display(), to.display());
    }
    Ok(())
}

fn copy_tree(source: &Path, target: &Path) -> Result<(), String> {
    std::fs::create_dir_all(target).map_err(|e| e.to_string())?;
    for entry in std::fs::read_dir(source).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let from = entry.path();
        let to = target.join(entry.file_name());
        let ty = entry.file_type().map_err(|e| e.to_string())?;
        if ty.is_dir() {
            copy_tree(&from, &to)?;
        } else if ty.is_file() {
            std::fs::copy(&from, &to).map_err(|e| format!("copy {}: {e}", from.display()))?;
        }
    }
    Ok(())
}

fn restore_search_sidecars(
    source: &Path,
    target: &Path,
    identity: Option<&str>,
) -> Result<(), String> {
    for name in ["search", "vector-ann"] {
        let from = source.join(name);
        if !from.is_dir() {
            continue;
        }
        let to = target.join(name);
        let _ = std::fs::remove_dir_all(&to);
        restore_tree(&from, &to, identity)?;
        eprintln!("restore sidecars: {} -> {}", from.display(), to.display());
    }
    Ok(())
}

fn restore_tree(source: &Path, target: &Path, identity: Option<&str>) -> Result<(), String> {
    std::fs::create_dir_all(target).map_err(|e| e.to_string())?;
    for entry in std::fs::read_dir(source).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let from = entry.path();
        let ty = entry.file_type().map_err(|e| e.to_string())?;
        if ty.is_dir() {
            restore_tree(&from, &target.join(entry.file_name()), identity)?;
        } else if ty.is_file() {
            if from.extension().is_some_and(|extension| extension == "age") {
                let identity = identity.ok_or_else(|| {
                    format!(
                        "encrypted sidecar {} requires NONODB_BACKUP_IDENTITY_FILE",
                        from.display()
                    )
                })?;
                let to = target.join(
                    from.file_stem()
                        .ok_or_else(|| format!("invalid sidecar name: {}", from.display()))?,
                );
                nonodb_core::backup_crypto::decrypt_file(&from, &to, identity)
                    .map_err(|e| format!("decrypt sidecar {}: {e}", from.display()))?;
            } else {
                let to = target.join(entry.file_name());
                std::fs::copy(&from, &to).map_err(|e| format!("copy {}: {e}", from.display()))?;
            }
        }
    }
    Ok(())
}

fn inspect(data: &Path, only: Option<&str>) -> Result<(), String> {
    let env = unsafe {
        EnvOpenOptions::new()
            .flags(EnvFlags::READ_ONLY)
            .max_dbs(nonodb_core::MAX_DBS)
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
        let profile = meta
            .get(&rtxn, format!("profile:{name}").as_bytes())
            .map_err(|e| e.to_string())?
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .unwrap_or("standard")
            .to_string();
        if profile == "columnar" {
            let stats =
                nonodb_core::columnar::stats(&rtxn, &env, &name).map_err(|e| e.to_string())?;
            println!(
                "  {name}: {} docs ({profile}, {} buckets: {} open, {} sealed, {} id entries, {} deletes, {} data bytes)",
                stats.rows,
                stats.buckets,
                stats.open_buckets,
                stats.sealed_buckets,
                stats.id_entries,
                stats.deletes,
                stats.data_bytes,
            );
            continue;
        }
        if let Some(db) = env
            .open_database::<Bytes, Bytes>(&rtxn, Some(&name))
            .map_err(|e| e.to_string())?
        {
            let entries = db.stat(&rtxn).map_err(|e| e.to_string())?.entries;
            println!("  {name}: {entries} docs ({profile})");
        }
    }
    let _ = rtxn.commit();
    Ok(())
}

#[derive(Debug)]
struct RestoreOptions {
    backup: PathBuf,
    data: PathBuf,
    force: bool,
    verify: bool,
    to_ms: Option<u64>,
    wal: Option<PathBuf>,
}

fn parse_restore_options(args: &[String]) -> Result<RestoreOptions, String> {
    let mut backup = None;
    let mut data = None;
    let mut force = false;
    let mut verify = false;
    let mut to_ms = None;
    let mut wal = None;
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        match arg.as_str() {
            "--force" => force = true,
            "--verify" => verify = true,
            "--to" => {
                index += 1;
                let raw = args
                    .get(index)
                    .ok_or("--to requires an RFC3339 timestamp or Unix milliseconds")?;
                to_ms = Some(parse_restore_timestamp(raw)?);
            }
            _ if arg.starts_with("--to=") => {
                to_ms = Some(parse_restore_timestamp(&arg["--to=".len()..])?);
            }
            "--wal" => {
                index += 1;
                wal = Some(PathBuf::from(
                    args.get(index).ok_or("--wal requires a WAL directory")?,
                ));
            }
            _ if arg.starts_with("--wal=") => {
                wal = Some(PathBuf::from(&arg["--wal=".len()..]));
            }
            _ if arg.starts_with('-') => {
                return Err(format!("unknown restore option: {arg}"));
            }
            _ => {
                if backup.is_none() {
                    backup = Some(PathBuf::from(arg));
                } else if data.is_none() {
                    data = Some(PathBuf::from(arg));
                } else {
                    return Err(format!("unexpected restore argument: {arg}"));
                }
            }
        }
        index += 1;
    }
    Ok(RestoreOptions {
        backup: backup.ok_or(
            "użycie: nonodb-cli restore <backup-dir> <data-dir> [--force] [--verify] [--to <czas>] [--wal <katalog>]",
        )?,
        data: data.ok_or(
            "użycie: nonodb-cli restore <backup-dir> <data-dir> [--force] [--verify] [--to <czas>] [--wal <katalog>]",
        )?,
        force,
        verify,
        to_ms,
        wal,
    })
}

fn parse_restore_timestamp(raw: &str) -> Result<u64, String> {
    if let Ok(value) = raw.parse::<u64>() {
        // Snapshot names use milliseconds. Accept Unix seconds too, as a
        // convenience for timestamps below the millisecond range.
        return Ok(if value < 100_000_000_000 {
            value.saturating_mul(1000)
        } else {
            value
        });
    }
    let ms = nonodb_core::date::parse_rfc3339_ms(raw)
        .ok_or_else(|| format!("invalid timestamp: {raw} (use RFC3339 or Unix milliseconds)"))?;
    u64::try_from(ms).map_err(|_| format!("timestamp before the Unix epoch: {raw}"))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn snapshot_timestamp(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let value = name.strip_prefix("data-")?.strip_suffix(".mdb")?;
    value.parse::<u64>().ok()
}

fn snapshot_candidates(root: &Path, database: &str) -> Vec<(u64, PathBuf)> {
    let database_dir = root.join(database);
    let selected = if database_dir.is_dir() {
        database_dir
    } else {
        root.to_path_buf()
    };
    let mut candidates = Vec::new();
    let Ok(entries) = std::fs::read_dir(&selected) else {
        return candidates;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Some(timestamp) = snapshot_timestamp(&path) {
            candidates.push((timestamp, path));
        }
    }
    candidates.sort_by_key(|(timestamp, _)| *timestamp);
    candidates
}

fn select_snapshot(root: &Path, database: &str, to_ms: u64) -> Result<(u64, PathBuf), String> {
    if to_ms > now_ms() {
        return Err(format!("--to timestamp is in the future ({} > now)", to_ms));
    }
    let candidates = snapshot_candidates(root, database);
    if candidates.is_empty() {
        return Err(format!(
            "no snapshot files data-<unix-ms>.mdb found in {} or {}/{}",
            root.display(),
            root.display(),
            database
        ));
    }
    match candidates
        .iter()
        .rev()
        .find(|(timestamp, _)| *timestamp <= to_ms)
    {
        Some((timestamp, path)) => Ok((*timestamp, path.clone())),
        None => Err(format!(
            "no snapshot at or before {} in {} (oldest available: {})",
            to_ms,
            root.display(),
            candidates[0].0
        )),
    }
}

fn restore_cmd(args: &[String]) -> Result<(), String> {
    let options = parse_restore_options(args)?;
    let target = database_target(&options.data)?;
    let backup = if let Some(to_ms) = options.to_ms {
        let (timestamp, path) = select_snapshot(&options.backup, &selected_database(), to_ms)?;
        eprintln!(
            "snapshot: {} (timestamp {}) for --to {}",
            path.display(),
            timestamp,
            to_ms
        );
        path
    } else {
        options.backup
    };
    let wal = options.to_ms.and_then(|_| {
        if let Some(wal) = options.wal.clone() {
            return Some(wal);
        }
        let default = target.join(nonodb_core::pitr::WAL_DIR);
        if default.is_dir() {
            Some(default)
        } else {
            eprintln!(
                "pitr: no WAL found at {}; restoring the snapshot only (pass --wal to replay history)",
                default.display()
            );
            None
        }
    });
    restore_into(
        &backup,
        &target,
        options.force,
        options.verify,
        wal.as_deref().zip(options.to_ms),
    )
}

fn restore_into(
    backup: &Path,
    data: &Path,
    force: bool,
    verify: bool,
    pitr: Option<(&Path, u64)>,
) -> Result<(), String> {
    let manifest = if backup.is_dir() {
        nonodb_core::verify::read_manifest(backup).map_err(|e| e.to_string())?
    } else {
        None
    };
    let src = if let Some(manifest) = &manifest {
        nonodb_core::verify::manifest_data_path(backup, manifest).map_err(|e| e.to_string())?
    } else if backup.join("data.mdb").exists() {
        backup.join("data.mdb")
    } else {
        backup.to_path_buf()
    };
    if !src.is_file() {
        return Err(format!("missing backup file: {}", src.display()));
    }
    if let Some(manifest) = &manifest {
        let meta = std::fs::metadata(&src).map_err(|e| e.to_string())?;
        if meta.len() != manifest.data_size {
            return Err(format!(
                "backup size does not match manifest: {} != {}",
                meta.len(),
                manifest.data_size
            ));
        }
        let digest = nonodb_core::verify::sha256_file(&src).map_err(|e| e.to_string())?;
        if digest != manifest.sha256 {
            return Err(format!(
                "backup SHA-256 does not match manifest: {digest} != {}",
                manifest.sha256
            ));
        }
    }
    std::fs::create_dir_all(data).map_err(|e| e.to_string())?;
    let dst = data.join("data.mdb");
    if dst.exists() && !force {
        return Err(format!(
            "{} already exists; use --force after stopping the daemon",
            dst.display()
        ));
    }
    let encrypted = manifest
        .as_ref()
        .is_some_and(|manifest| manifest.encryption.is_some());
    let compressed = manifest
        .as_ref()
        .is_some_and(|manifest| manifest.compression.is_some());
    let tmp = data.join(format!(".restore-data.mdb.tmp.{}", std::process::id()));
    let raw = data.join(format!(".restore-source.tmp.{}", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    let _ = std::fs::remove_file(&raw);
    let identity = if encrypted {
        let key_path = std::env::var_os("NONODB_BACKUP_IDENTITY_FILE")
            .map(PathBuf::from)
            .ok_or("encrypted backup requires NONODB_BACKUP_IDENTITY_FILE")?;
        let identity =
            nonodb_core::backup_crypto::read_key_file(&key_path).map_err(|e| e.to_string())?;
        nonodb_core::backup_crypto::decrypt_file(&src, &raw, &identity)
            .map_err(|e| e.to_string())?;
        Some(identity)
    } else {
        std::fs::copy(&src, &raw).map_err(|e| e.to_string())?;
        None
    };
    let plain = if compressed {
        nonodb_core::backup_compress::gunzip_file(&raw, &tmp)
            .map_err(|e| format!("decompress backup: {e}"))?;
        let _ = std::fs::remove_file(&raw);
        &tmp
    } else {
        &raw
    };
    if encrypted || compressed {
        if let Some(manifest) = &manifest {
            let meta = std::fs::metadata(plain).map_err(|e| e.to_string())?;
            if let Some(size) = manifest.plain_size {
                if meta.len() != size {
                    let _ = std::fs::remove_file(plain);
                    return Err(format!(
                        "restored size does not match manifest: {} != {size}",
                        meta.len()
                    ));
                }
            }
            if let Some(expected) = &manifest.plain_sha256 {
                let digest = nonodb_core::verify::sha256_file(plain).map_err(|e| e.to_string())?;
                if &digest != expected {
                    let _ = std::fs::remove_file(plain);
                    return Err(format!(
                        "restored SHA-256 does not match manifest: {digest} != {expected}"
                    ));
                }
            }
        }
    }
    let _ = std::fs::remove_file(data.join("lock.mdb"));
    std::fs::rename(plain, &dst).map_err(|e| e.to_string())?;
    let file = std::fs::File::open(&dst).map_err(|e| e.to_string())?;
    let _ = file.sync_all();
    if backup.is_dir() {
        restore_search_sidecars(backup, data, identity.as_deref())?;
    }
    eprintln!("restore: {} -> {}", src.display(), dst.display());
    if let Some((wal_dir, target_ms)) = pitr {
        if !wal_dir.is_dir() {
            return Err(format!(
                "PITR WAL directory not found: {} (pass --wal <dir>)",
                wal_dir.display()
            ));
        }
        let store = nonodb_core::Store::open(data).map_err(|e| e.to_string())?;
        let report = nonodb_core::pitr::replay_to(&store, wal_dir, target_ms)
            .map_err(|e| format!("PITR replay failed: {e}"))?;
        eprintln!(
            "pitr: replayed {} transactions through sequence {} (snapshot head {}, target {})",
            report.applied, report.last_replayed_seq, report.snapshot_head, report.target_ms
        );
        drop(store);
    }
    if verify {
        let has_sidecars = ["search", "vector-ann"]
            .iter()
            .any(|name| backup.join(name).is_dir());
        let report = if has_sidecars {
            nonodb_core::verify_data_dir(data).map_err(|e| e.to_string())?
        } else {
            nonodb_core::verify_data_dir_allow_missing_search(data).map_err(|e| e.to_string())?
        };
        print_report(&report);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("nonodb-cli-{label}-{}-{stamp}", std::process::id()))
    }

    #[test]
    fn create_collection_payload_supports_timeseries_profile() {
        let args = [
            "metrics",
            "--profile",
            "timeseries",
            "--time-field",
            "ts",
            "--meta-field",
            "device",
            "--granularity",
            "minutes",
            "--compression",
            "max",
            "--expire-after",
            "3600",
        ]
        .map(str::to_string);
        let payload = create_collection_payload(&args).unwrap();
        assert_eq!(payload["op"], json!("create_collection"));
        assert_eq!(payload["profile"], json!("timeseries"));
        assert_eq!(payload["timeseries"]["timeField"], json!("ts"));
        assert_eq!(payload["timeseries"]["metaField"], json!("device"));
        assert_eq!(payload["timeseries"]["granularity"], json!("minutes"));
        assert_eq!(payload["timeseries"]["compression"], json!("max"));
        assert_eq!(payload["timeseries"]["expireAfterSeconds"], json!(3600));

        let missing = ["metrics", "--profile", "timeseries"].map(str::to_string);
        assert!(create_collection_payload(&missing).is_err());
        let bad = ["metrics", "--profile", "weird"].map(str::to_string);
        assert!(create_collection_payload(&bad).is_err());

        let standard = ["orders", "--ttl", "60"].map(str::to_string);
        let payload = create_collection_payload(&standard).unwrap();
        assert_eq!(payload["profile"], json!("standard"));
        assert_eq!(payload["ttl_seconds"], json!(60));
        assert!(payload.get("timeseries").is_none());
    }

    #[test]
    fn create_collection_payload_supports_columnar_profile() {
        let args = [
            "events",
            "--profile",
            "columnar",
            "--bucket-max-docs",
            "128",
            "--bucket-max-bytes",
            "262144",
            "--compression",
            "max",
            "--ttl",
            "3600",
        ]
        .map(str::to_string);
        let payload = create_collection_payload(&args).unwrap();
        assert_eq!(payload["op"], json!("create_collection"));
        assert_eq!(payload["profile"], json!("columnar"));
        assert_eq!(payload["ttl_seconds"], json!(3600));
        assert_eq!(payload["columnar"]["bucket_max_docs"], json!(128));
        assert_eq!(payload["columnar"]["bucket_max_bytes"], json!(262144));
        assert_eq!(payload["columnar"]["compression"], json!("max"));
        assert!(payload.get("timeseries").is_none());

        let current = json!({
            "bucket_max_docs": 512,
            "bucket_max_bytes": 524288,
            "compression": "balanced",
        });
        let overrides = ["--bucket-max-docs", "64", "--compression", "fast"].map(str::to_string);
        let merged = apply_columnar_overrides(&current, &overrides).unwrap();
        assert_eq!(merged["bucket_max_docs"], json!(64));
        assert_eq!(merged["bucket_max_bytes"], json!(524288));
        assert_eq!(merged["compression"], json!("fast"));
        assert!(apply_columnar_overrides(&current, &["--bogus".to_string()]).is_err());
    }

    #[test]
    fn set_timeseries_merges_only_provided_fields() {
        let current = json!({
            "time_field": "ts",
            "meta_field": "device",
            "granularity": "seconds",
            "compression": "balanced",
            "expire_after_seconds": 60,
        });
        let args = ["--compression", "max", "--expire-after", "none"].map(str::to_string);
        let merged = apply_ts_overrides(&current, &args).unwrap();
        assert_eq!(merged["time_field"], json!("ts"));
        assert_eq!(merged["compression"], json!("max"));
        assert_eq!(merged["expire_after_seconds"], Value::Null);
        let args = ["--granularity", "hours", "--bucket-max-docs", "500"].map(str::to_string);
        let merged = apply_ts_overrides(&current, &args).unwrap();
        assert_eq!(merged["granularity"], json!("hours"));
        assert_eq!(merged["bucket_max_docs"], json!(500));
        assert!(apply_ts_overrides(&current, &["--bogus".to_string()]).is_err());
    }

    #[test]
    fn parses_restore_timestamps_in_seconds_ms_and_rfc3339() {
        assert_eq!(
            parse_restore_timestamp("1700000000").unwrap(),
            1_700_000_000_000
        );
        assert_eq!(
            parse_restore_timestamp("1700000000000").unwrap(),
            1_700_000_000_000
        );
        assert_eq!(
            parse_restore_timestamp("2024-01-01T00:00:00Z").unwrap(),
            1_704_067_200_000
        );
        assert!(parse_restore_timestamp("not-a-time").is_err());
    }

    #[test]
    fn selects_latest_snapshot_at_or_before_timestamp() {
        let root = temp_root("select");
        let db = root.join("app1");
        std::fs::create_dir_all(&db).unwrap();
        for timestamp in [1000u64, 2000, 3000] {
            std::fs::write(db.join(format!("data-{timestamp}.mdb")), b"x").unwrap();
        }
        let now = now_ms();
        let (timestamp, path) = select_snapshot(&root, "app1", 2500).unwrap();
        assert_eq!(timestamp, 2000);
        assert_eq!(path.file_name().unwrap(), "data-2000.mdb");
        assert!(select_snapshot(&root, "app1", 999).is_err());
        assert!(select_snapshot(&root, "app1", now + 60_000).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn compact_shrinks_a_database_and_keeps_it_readable() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = nonodb_core::Store::open(dir.path()).unwrap();
            let settings = nonodb_core::timeseries::Settings {
                time_field: "ts".into(),
                meta_field: Some("device".into()),
                ..Default::default()
            };
            let collection = store
                .create_timeseries_collection("metrics", &settings)
                .unwrap();
            for idx in 0..20_000i64 {
                collection
                    .insert_one(json!({
                        "ts": {"$date": {"$numberLong": (1_700_000_000_000i64 + idx * 1000).to_string()}},
                        "device": {"id": "a"},
                        "value": idx,
                        "status": if idx % 2 == 0 { "ok" } else { "warn" },
                    }))
                    .unwrap();
            }
            store.seal_timeseries("metrics").unwrap();
            store
                .retain_timeseries("metrics", 1_700_000_000_000 + 10_000_000)
                .unwrap();
            store.close();
        }
        let before = std::fs::metadata(dir.path().join("data.mdb"))
            .unwrap()
            .len();
        compact(dir.path(), true).unwrap();
        let after = std::fs::metadata(dir.path().join("data.mdb"))
            .unwrap()
            .len();
        assert!(after < before, "expected shrink: {before} -> {after}");
        assert!(!dir.path().join("data.mdb.before-compact").exists());

        let store = nonodb_core::Store::open(dir.path()).unwrap();
        let collection = store.collection("metrics").unwrap();
        assert_eq!(
            collection.profile(),
            nonodb_core::CollectionProfile::TimeSeries
        );
        assert_eq!(collection.count(&json!({})).unwrap(), 10_000);
    }

    #[test]
    fn parses_restore_options_in_any_order() {
        let options = parse_restore_options(&[
            "--to".into(),
            "2000".into(),
            "backup".into(),
            "--verify".into(),
            "data".into(),
            "--force".into(),
        ])
        .unwrap();
        assert_eq!(options.backup, PathBuf::from("backup"));
        assert_eq!(options.data, PathBuf::from("data"));
        assert!(options.force);
        assert!(options.verify);
        assert_eq!(options.to_ms, Some(2_000_000));
        assert!(options.wal.is_none());
    }
}
