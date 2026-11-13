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

use nonodb_core::heed::types::Bytes;
use nonodb_core::heed::{CompactionOption, EnvFlags, EnvOpenOptions};
use serde_json::{json, Value};

mod gui;

fn main() {
    if let Err(err) = run() {
        eprintln!("nonodb-cli: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    strip_db_flag(&mut args)?;
    let Some(cmd) = args.first().map(String::as_str) else {
        return usage();
    };
    match cmd {
        "gui" => gui::run(&args[1..])?,
        "ping" => {
            let res = http_get("/healthz")?;
            println!("{}", serde_json::to_string_pretty(&res).unwrap());
        }
        "ready" => {
            let res = http_get("/readyz")?;
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
        "map" => {
            let res = http_post("/v1/exec", &json!({"op": "map_info"}))?;
            print_result(&res);
        }
        "sync" => {
            let res = http_post("/v1/exec", &json!({"op": "sync"}))?;
            print_result(&res);
        }
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
        "backup" => {
            let data = PathBuf::from(
                args.get(1)
                    .ok_or("użycie: nonodb-cli backup <data-dir> <out-dir>")?,
            );
            let out = PathBuf::from(
                args.get(2)
                    .ok_or("użycie: nonodb-cli backup <data-dir> <out-dir>")?,
            );
            let source = database_dir(&data)?;
            backup(&source, &out)?;
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
        "backup-all" => {
            let data = PathBuf::from(
                args.get(1)
                    .ok_or("użycie: nonodb-cli backup-all <data-root> <out-dir>")?,
            );
            let out = PathBuf::from(
                args.get(2)
                    .ok_or("użycie: nonodb-cli backup-all <data-root> <out-dir>")?,
            );
            backup_all(&data, &out)?;
        }
        "restore-all" => {
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
            let verify = args.iter().any(|a| a == "--verify");
            let target = database_target(&data)?;
            restore(&backup, &target, force, verify)?;
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
         gui [--port N] [--no-open]  lokalne GUI w przeglądarce\n\
         ping                       sprawdź daemona\n\
         exec '<json>'              dowolne żądanie /v1/exec\n\
         databases [list|create <nazwa>|drop <nazwa>]  bazy w jednym daemonie\n\
         collections                lista kolekcji\n\
         count <coll> [filtr]       liczba dokumentów\n\
         find <coll> [filtr] [--sort pole:asc|desc] [--limit N] [--skip N]\n\
         indexes <coll>             indeksy kolekcji\n\
         auth <komenda>             zarządzanie auth.json (init/principal/token/cert/check)\n\
         map                        rozmiar mapy LMDB i zajęte bajty\n\
         sync                       wymuś fsync wybranej bazy\n\
         resize <rozmiar>           powiększ mapę online, np. 4GiB\n\
         backup <data> <out>        kopia wybranej bazy (--db) + manifest.json\n\
         backup-keygen <plik>      generuje parę kluczy age do backupów\n\
         backup-all <data> <out>    kopia wszystkich baz + manifest instancji\n\
         verify <data> [--json]     głęboka weryfikacja wybranej bazy i indeksów\n\
         verify-backup <backup> [--json]  sprawdź manifest, sumę SHA-256 i zawartość\n\
         inspect <data> [coll]      statystyki bazy/kolekcji (--db wybiera bazę)\n\
         restore <backup> <data> [--force] [--verify]  przywróć bazę (najpierw zatrzymaj daemona)\n\
         restore-all <backup> <data> [--force] [--verify]  przywróć wszystkie bazy\n\
         \n\
         Opcje: --db <nazwa> wybiera bazę (albo NONODB_DB).\n\
         Zmienne: NONODB_SOCK (Unix, domyślnie /run/nonodb.sock),\n\
         NONODB_URL (Windows, domyślnie http://127.0.0.1:7878), NONODB_TOKEN,\n\
         NONODB_DB (domyślnie default),\n\
         NONODB_AUTH_FILE (domyślnie /etc/nonodb/auth.json)"
    );
    Ok(())
}

fn print_report(report: &nonodb_core::VerificationReport) {
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

fn selected_database() -> String {
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn validate_database_name(name: &str) -> Result<(), String> {
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

fn backup_all(data: &Path, out_dir: &Path) -> Result<(), String> {
    let databases = instance_databases(data)?;
    std::fs::create_dir_all(out_dir).map_err(|e| e.to_string())?;
    let mut entries = Vec::new();
    for (name, source) in databases {
        let db_out = out_dir.join(&name);
        backup(&source, &db_out)?;
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
        restore_into(&source, &target, force, verify)?;
    }
    eprintln!(
        "restore-all: {} databases -> {}",
        databases.len(),
        data.display()
    );
    Ok(())
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

fn restore(backup: &Path, data: &Path, force: bool, verify: bool) -> Result<(), String> {
    let target = database_target(data)?;
    restore_into(backup, &target, force, verify)
}

fn restore_into(backup: &Path, data: &Path, force: bool, verify: bool) -> Result<(), String> {
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
    if encrypted {
        let key_path = std::env::var_os("NONODB_BACKUP_IDENTITY_FILE")
            .map(PathBuf::from)
            .ok_or("encrypted backup requires NONODB_BACKUP_IDENTITY_FILE")?;
        let identity =
            nonodb_core::backup_crypto::read_key_file(&key_path).map_err(|e| e.to_string())?;
        nonodb_core::backup_crypto::decrypt_file(&src, &raw, &identity)
            .map_err(|e| e.to_string())?;
    } else {
        std::fs::copy(&src, &raw).map_err(|e| e.to_string())?;
    }
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
    eprintln!("restore: {} -> {}", src.display(), dst.display());
    if verify {
        let report = nonodb_core::verify_data_dir(data).map_err(|e| e.to_string())?;
        print_report(&report);
    }
    Ok(())
}
