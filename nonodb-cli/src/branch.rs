//! Offline database branch/clone support.
//!
//! A branch is a new database directory with its own `data.mdb`, writer and
//! lock file. The source must be stopped or be an immutable snapshot. On
//! filesystems with reflink support the copy is cheap; otherwise the command
//! falls back to a normal byte copy and says so.

use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde_json::{json, Value};

use crate::{print_report, selected_database, validate_database_name};

const USAGE: &str = "użycie: nonodb-cli branch <target-db> --data <root> [--from-snapshot <plik>] [--force] [--verify] [--no-sidecars] [--json]";

fn print_help() {
    println!("{USAGE}");
    println!();
    println!("Tworzy niezależny klon bazy (offline).");
    println!();
    println!("Opcje:");
    println!("  --data <root>             katalog danych (albo NONODB_DATA)");
    println!("  --from-snapshot <path>    plik data-<unix-ms>.mdb/dir zamiast bazy --db");
    println!("  --force                   zastąp istniejący cel po udanym klonie");
    println!("  --verify                  głęboka weryfikacja klonu");
    println!("  --no-sidecars             nie kopiuj search/ i vector-ann/");
    println!("  --json                    wynik JSON (method, bytes, elapsed_ms)");
    println!();
    println!("Przykład:");
    println!("  nonodb-cli --db app1 branch app1-ci --data /var/lib/nonodb --verify");
}

#[derive(Debug)]
struct Options {
    data_root: PathBuf,
    target: String,
    snapshot: Option<PathBuf>,
    force: bool,
    verify: bool,
    no_sidecars: bool,
    json: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Method {
    Reflink,
    Clonefile,
    Copy,
}

impl Method {
    fn as_str(self) -> &'static str {
        match self {
            Self::Reflink => "reflink",
            Self::Clonefile => "clonefile",
            Self::Copy => "copy",
        }
    }
}

struct CopyStats {
    bytes: u64,
    method: Method,
}

fn parse_args(args: &[String]) -> Result<Options, String> {
    let mut data_root = None;
    let mut target = None;
    let mut snapshot = None;
    let mut force = false;
    let mut verify = false;
    let mut no_sidecars = false;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        match arg.as_str() {
            "--force" => force = true,
            "--verify" => verify = true,
            "--no-sidecars" | "--without-search-sidecars" => no_sidecars = true,
            "--json" => json = true,
            "--data" => {
                index += 1;
                data_root = Some(PathBuf::from(
                    args.get(index)
                        .ok_or("--data requires a data root directory")?,
                ));
            }
            "--from-snapshot" => {
                index += 1;
                snapshot = Some(PathBuf::from(
                    args.get(index)
                        .ok_or("--from-snapshot requires a data-<unix-ms>.mdb file")?,
                ));
            }
            _ if arg.starts_with("--data=") => {
                data_root = Some(PathBuf::from(&arg["--data=".len()..]));
            }
            _ if arg.starts_with("--from-snapshot=") => {
                snapshot = Some(PathBuf::from(&arg["--from-snapshot=".len()..]));
            }
            _ if arg.starts_with('-') => {
                return Err(format!("unknown branch option: {arg}"));
            }
            _ => {
                if target.is_some() {
                    return Err(format!("unexpected branch argument: {arg}"));
                }
                target = Some(arg.clone());
            }
        }
        index += 1;
    }
    let target = target.ok_or(USAGE)?;
    validate_database_name(&target)?;
    let data_root = data_root
        .or_else(|| std::env::var_os("NONODB_DATA").map(PathBuf::from))
        .ok_or("branch requires --data <root> or NONODB_DATA")?;
    Ok(Options {
        data_root,
        target,
        snapshot,
        force,
        verify,
        no_sidecars,
        json,
    })
}

fn resolve_source(options: &Options) -> Result<(PathBuf, PathBuf), String> {
    if let Some(snapshot) = &options.snapshot {
        let (data, sidecar_root) = if snapshot.is_dir() {
            (snapshot.join("data.mdb"), snapshot.clone())
        } else {
            let root = snapshot
                .parent()
                .ok_or_else(|| format!("invalid snapshot path: {}", snapshot.display()))?;
            (snapshot.clone(), root.to_path_buf())
        };
        if !data.is_file() {
            return Err(format!("snapshot data file not found: {}", data.display()));
        }
        return Ok((data, sidecar_root));
    }

    let database = selected_database();
    validate_database_name(&database)?;
    let database_dir = options.data_root.join(&database);
    if database_dir.join("data.mdb").is_file() {
        return Ok((database_dir.join("data.mdb"), database_dir));
    }
    // Legacy single-database layout.
    if options.data_root.join("data.mdb").is_file() {
        return Ok((
            options.data_root.join("data.mdb"),
            options.data_root.clone(),
        ));
    }
    Err(format!(
        "source database `{database}` not found under {} (stop the daemon before branching)",
        options.data_root.display()
    ))
}

fn try_reflink(source: &Path, target: &Path) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let src = File::open(source)?;
        let dst = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(target)?;
        let result = unsafe { libc::ioctl(dst.as_raw_fd(), libc::FICLONE, src.as_raw_fd()) };
        if result == 0 {
            return Ok(());
        }
        Err(std::io::Error::last_os_error())
    }
    #[cfg(target_os = "macos")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let source = CString::new(source.as_os_str().as_bytes())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "NUL in path"))?;
        let target = CString::new(target.as_os_str().as_bytes())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "NUL in path"))?;
        let result = unsafe { libc::clonefile(source.as_ptr(), target.as_ptr(), 0) };
        if result == 0 {
            return Ok(());
        }
        Err(std::io::Error::last_os_error())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (source, target);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "reflink is not supported on this platform",
        ))
    }
}

fn copy_file(source: &Path, target: &Path) -> Result<(u64, Method), String> {
    copy_file_mode(source, target, true)
}

fn copy_file_mode(source: &Path, target: &Path, use_cow: bool) -> Result<(u64, Method), String> {
    let metadata =
        fs::metadata(source).map_err(|error| format!("metadata {}: {error}", source.display()))?;
    let bytes = metadata.len();
    if use_cow {
        match try_reflink(source, target) {
            Ok(()) => {
                let result = (|| {
                    fs::set_permissions(target, metadata.permissions())
                        .map_err(|error| format!("chmod {}: {error}", target.display()))?;
                    sync_file(target)
                })();
                if let Err(error) = result {
                    let _ = fs::remove_file(target);
                    return Err(error);
                }
                let method = if cfg!(target_os = "macos") {
                    Method::Clonefile
                } else {
                    Method::Reflink
                };
                return Ok((bytes, method));
            }
            Err(_) => {
                let _ = fs::remove_file(target);
            }
        }
    }
    let result = (|| {
        fs::copy(source, target).map_err(|error| {
            format!("copy {} -> {}: {error}", source.display(), target.display())
        })?;
        fs::set_permissions(target, metadata.permissions())
            .map_err(|error| format!("chmod {}: {error}", target.display()))?;
        sync_file(target)
    })();
    if let Err(error) = result {
        let _ = fs::remove_file(target);
        return Err(error);
    }
    Ok((bytes, Method::Copy))
}

fn sync_file(path: &Path) -> Result<(), String> {
    let file = File::open(path).map_err(|error| format!("open {}: {error}", path.display()))?;
    file.sync_all()
        .map_err(|error| format!("sync {}: {error}", path.display()))
}

fn copy_tree(source: &Path, target: &Path, stats: &mut CopyStats) -> Result<(), String> {
    fs::create_dir_all(target).map_err(|error| format!("mkdir {}: {error}", target.display()))?;
    for entry in
        fs::read_dir(source).map_err(|error| format!("read {}: {error}", source.display()))?
    {
        let entry = entry.map_err(|error| error.to_string())?;
        let from = entry.path();
        let to = target.join(entry.file_name());
        let ty = entry.file_type().map_err(|error| error.to_string())?;
        if ty.is_dir() {
            copy_tree(&from, &to, stats)?;
        } else if ty.is_file() {
            let (bytes, method) = copy_file(&from, &to)?;
            stats.bytes += bytes;
            stats.method = merge_method(stats.method, method);
        }
    }
    if let Ok(metadata) = fs::metadata(source) {
        let _ = fs::set_permissions(target, metadata.permissions());
    }
    Ok(())
}

fn merge_method(current: Method, next: Method) -> Method {
    if current == Method::Copy || next == Method::Copy {
        Method::Copy
    } else if current == Method::Clonefile || next == Method::Clonefile {
        Method::Clonefile
    } else {
        Method::Reflink
    }
}

fn unique_temp(root: &Path, target: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    root.join(format!(".branch-{target}-{}-{nanos}", std::process::id()))
}

fn remove_path(path: &Path) -> std::io::Result<()> {
    if path.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}

fn replace_target(temp: &Path, target: &Path, root: &Path, name: &str) -> Result<(), String> {
    if !target.exists() {
        return fs::rename(temp, target).map_err(|error| {
            format!(
                "rename branch {} -> {}: {error}",
                temp.display(),
                target.display()
            )
        });
    }
    let backup = unique_temp(root, &format!("old-{name}"));
    fs::rename(target, &backup).map_err(|error| {
        format!(
            "move existing target {} out of the way: {error}",
            target.display()
        )
    })?;
    match fs::rename(temp, target) {
        Ok(()) => {
            let _ = remove_path(&backup);
            Ok(())
        }
        Err(error) => {
            let _ = fs::rename(&backup, target);
            Err(format!(
                "rename branch {} -> {}: {error}",
                temp.display(),
                target.display()
            ))
        }
    }
}

fn run_branch(
    options: &Options,
) -> Result<(Value, Option<nonodb_core::VerificationReport>), String> {
    let (source_data, source_root) = resolve_source(options)?;
    let target_dir = options.data_root.join(&options.target);
    if source_data == target_dir.join("data.mdb") {
        return Err("source and target database are the same".into());
    }
    if target_dir.exists() && !options.force {
        return Err(format!(
            "target database already exists: {} (use --force to replace it)",
            target_dir.display()
        ));
    }
    fs::create_dir_all(&options.data_root)
        .map_err(|error| format!("mkdir {}: {error}", options.data_root.display()))?;

    let temp = unique_temp(&options.data_root, &options.target);
    let _ = fs::remove_dir_all(&temp);
    fs::create_dir_all(&temp).map_err(|error| format!("mkdir {}: {error}", temp.display()))?;

    let result = (|| {
        let started = Instant::now();
        let (bytes, method) = copy_file(&source_data, &temp.join("data.mdb"))?;
        let mut stats = CopyStats { bytes, method };
        let mut sidecars = Vec::new();
        if !options.no_sidecars {
            for name in ["search", "vector-ann"] {
                let from = source_root.join(name);
                if from.is_dir() {
                    copy_tree(&from, &temp.join(name), &mut stats)?;
                    sidecars.push(name.to_string());
                }
            }
        }

        let mut verification = if options.verify {
            let report = if sidecars.is_empty() {
                nonodb_core::verify_data_dir_allow_missing_search(&temp)
            } else {
                nonodb_core::verify_data_dir(&temp)
            }
            .map_err(|error| format!("branch verification failed: {error}"))?;
            Some(report)
        } else {
            None
        };

        replace_target(&temp, &target_dir, &options.data_root, &options.target)?;
        if let Some(report) = verification.as_mut() {
            report.path = target_dir.display().to_string();
        }

        let mut output = json!({
            "target": options.target,
            "target_path": target_dir.display().to_string(),
            "source_path": source_data.display().to_string(),
            "method": stats.method.as_str(),
            "bytes": stats.bytes,
            "elapsed_ms": started.elapsed().as_millis() as u64,
            "sidecars": sidecars,
            "verification": verification
                .as_ref()
                .map(|report| serde_json::to_value(report).unwrap_or(Value::Null)),
        });
        if stats.method == Method::Copy {
            output["warning"] =
                json!("reflink unavailable; used a full byte copy (correct, but not instant)");
        }
        Ok::<(Value, Option<nonodb_core::VerificationReport>), String>((output, verification))
    })();

    if result.is_err() {
        let _ = fs::remove_dir_all(&temp);
    }
    result
}

pub(crate) fn run(args: &[String]) -> Result<(), String> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_help();
        return Ok(());
    }
    let options = parse_args(args)?;
    let (output, verification) = run_branch(&options)?;
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&output).map_err(|error| error.to_string())?
        );
    } else {
        println!(
            "branch: {} -> {} ({}; {} bytes, {} ms)",
            output["source_path"].as_str().unwrap_or(""),
            output["target_path"].as_str().unwrap_or(""),
            output["method"].as_str().unwrap_or("copy"),
            output["bytes"].as_u64().unwrap_or(0),
            output["elapsed_ms"].as_u64().unwrap_or(0)
        );
        if let Some(report) = verification.as_ref() {
            print_report(report);
        } else if output["method"].as_str() == Some("copy") {
            eprintln!(
                "branch: reflink unavailable; used a full byte copy (correct, but not instant)"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(label: &str) -> PathBuf {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "nonodb-branch-{label}-{}-{stamp}",
            std::process::id()
        ))
    }

    #[test]
    fn copy_file_preserves_bytes_permissions_and_fallback() {
        let root = temp_root("copy");
        fs::create_dir_all(&root).unwrap();
        let source = root.join("source");
        let target = root.join("target");
        fs::write(&source, vec![7u8; 64 * 1024]).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&source, fs::Permissions::from_mode(0o640)).unwrap();
        }
        let (bytes, method) = copy_file_mode(&source, &target, false).unwrap();
        assert_eq!(bytes, 64 * 1024);
        assert_eq!(method, Method::Copy);
        assert_eq!(fs::read(&target).unwrap(), fs::read(&source).unwrap());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&target).unwrap().permissions().mode() & 0o777,
                0o640
            );
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reflink_is_used_when_the_filesystem_supports_it() {
        let root = temp_root("reflink");
        fs::create_dir_all(&root).unwrap();
        let source = root.join("source");
        let probe = root.join("probe");
        fs::write(&source, b"cow").unwrap();
        let supported = try_reflink(&source, &probe).is_ok();
        let _ = fs::remove_file(&probe);
        let target = root.join("target");
        let (_, method) = copy_file(&source, &target).unwrap();
        if supported {
            assert!(matches!(method, Method::Reflink | Method::Clonefile));
        } else {
            assert_eq!(method, Method::Copy);
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_copy_leaves_no_partial_target() {
        let root = temp_root("partial");
        fs::create_dir_all(&root).unwrap();
        let target = root.join("target");
        let error = copy_file_mode(&root.join("missing"), &target, false).unwrap_err();
        assert!(error.contains("metadata"));
        assert!(!target.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn branch_does_not_copy_lock_file() {
        let root = temp_root("lock");
        let source_root = root.join("source");
        fs::create_dir_all(&source_root).unwrap();
        fs::write(source_root.join("data.mdb"), b"source bytes").unwrap();
        fs::write(source_root.join("lock.mdb"), b"must not be copied").unwrap();
        fs::create_dir_all(source_root.join("search/digest")).unwrap();
        fs::write(source_root.join("search/digest/seg-000001.seg"), b"search").unwrap();
        fs::create_dir_all(source_root.join("vector-ann/digest")).unwrap();
        fs::write(source_root.join("vector-ann/digest/ann.bin"), b"vectors").unwrap();
        let options = Options {
            data_root: root.clone(),
            target: "clone".into(),
            snapshot: Some(source_root.join("data.mdb")),
            force: false,
            verify: false,
            no_sidecars: false,
            json: true,
        };
        let (output, _) = run_branch(&options).unwrap();
        assert!(["reflink", "clonefile", "copy"].contains(&output["method"].as_str().unwrap()));
        assert_eq!(
            fs::read(root.join("clone/data.mdb")).unwrap(),
            b"source bytes"
        );
        assert!(!root.join("clone/lock.mdb").exists());
        assert_eq!(
            fs::read(root.join("clone/search/digest/seg-000001.seg")).unwrap(),
            b"search"
        );
        assert_eq!(
            fs::read(root.join("clone/vector-ann/digest/ann.bin")).unwrap(),
            b"vectors"
        );
        assert!(source_root.join("lock.mdb").is_file());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn parses_branch_options_and_rejects_duplicate_targets() {
        let options = parse_args(&[
            "app1-ci".into(),
            "--data".into(),
            "/tmp/nonodb".into(),
            "--from-snapshot".into(),
            "/tmp/snap/data-1.mdb".into(),
            "--force".into(),
            "--verify".into(),
            "--no-sidecars".into(),
            "--json".into(),
        ])
        .unwrap();
        assert_eq!(options.target, "app1-ci");
        assert_eq!(options.data_root, PathBuf::from("/tmp/nonodb"));
        assert_eq!(
            options.snapshot,
            Some(PathBuf::from("/tmp/snap/data-1.mdb"))
        );
        assert!(options.force && options.verify && options.no_sidecars && options.json);
        assert!(parse_args(&["a".into(), "b".into()]).is_err());
    }

    #[test]
    fn merging_copy_methods_is_conservative() {
        assert_eq!(
            merge_method(Method::Reflink, Method::Reflink),
            Method::Reflink
        );
        assert_eq!(merge_method(Method::Reflink, Method::Copy), Method::Copy);
        assert_eq!(merge_method(Method::Clonefile, Method::Copy), Method::Copy);
    }
}
