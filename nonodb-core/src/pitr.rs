//! Point-in-time recovery: segmented WAL files and restore-time replay.
//!
//! The WAL is a logical, line-oriented copy of the durable replication log.
//! Each transaction is written exactly once, so a snapshot plus the WAL after
//! the snapshot's replication head can reconstruct the database at any
//! timestamp covered by the retained files.
//!
//! Layout:
//!   `wal/wal-<start-seq>-<created-ms>.wal`
//!
//! Every file starts with a JSON header followed by one JSON record per line.
//! Records are self-describing and checksummed. A crash can leave a partial
//! trailing record; the writer truncates that tail when it reopens the newest
//! segment, keeping all complete records usable.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::replication::ReplicationTxn;
use crate::store::Store;

pub const WAL_DIR: &str = "wal";
pub const WAL_FORMAT: &str = "nonodb-wal";
pub const WAL_VERSION: u32 = 1;
pub const SNAPSHOT_META_FORMAT: &str = "nonodb-snapshot";
pub const SNAPSHOT_META_VERSION: u32 = 1;

const WAL_PREFIX: &str = "wal-";
const WAL_SUFFIX: &str = ".wal";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WalHeader {
    format: String,
    version: u32,
    start_seq: u64,
    created_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WalRecord {
    sha256: String,
    txn: ReplicationTxn,
}

#[derive(Clone, Debug)]
struct SegmentInfo {
    path: PathBuf,
    start_seq: u64,
    last_seq: u64,
    created_at_ms: u64,
    valid_bytes: u64,
}

/// Metadata written next to every PITR-capable snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub format: String,
    pub version: u32,
    pub database: String,
    pub created_at_ms: u64,
    /// Replication head observed before the copy. The snapshot may contain a
    /// slightly newer head; this value is deliberately conservative so WAL
    /// pruning can never remove data the snapshot does not cover.
    pub replication_head: u64,
}

/// One PITR restore result.
#[derive(Clone, Debug, Serialize)]
pub struct ReplayReport {
    pub target_ms: u64,
    pub snapshot_head: u64,
    pub last_replayed_seq: u64,
    pub applied: usize,
    pub stopped_at_ms: Option<u64>,
}

/// WAL writer for the running daemon.
pub struct WalWriter {
    dir: PathBuf,
    file: File,
    path: PathBuf,
    start_seq: u64,
    last_seq: u64,
    bytes: u64,
    segment_bytes: u64,
}

impl WalWriter {
    /// Opens (or creates) a WAL directory and positions the writer after the
    /// last complete record. A partial crash tail in the newest segment is
    /// truncated before appending continues.
    pub fn open(dir: impl AsRef<Path>, segment_bytes: u64) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)
            .map_err(|e| Error::Query(format!("create WAL directory {}: {e}", dir.display())))?;
        let segment_bytes = segment_bytes.max(1024);
        let mut segments = scan_segments(&dir)?;
        segments.sort_by_key(|segment| (segment.start_seq, segment.created_at_ms));
        if let Some(last) = segments.last() {
            if last.valid_bytes > 0 {
                let len = fs::metadata(&last.path)
                    .map_err(|e| Error::Query(format!("stat WAL segment: {e}")))?
                    .len();
                if last.valid_bytes < len {
                    let file = OpenOptions::new()
                        .write(true)
                        .open(&last.path)
                        .map_err(|e| Error::Query(format!("open WAL segment for repair: {e}")))?;
                    file.set_len(last.valid_bytes)
                        .map_err(|e| Error::Query(format!("truncate WAL crash tail: {e}")))?;
                    file.sync_all()
                        .map_err(|e| Error::Query(format!("sync repaired WAL segment: {e}")))?;
                }
                let file = OpenOptions::new()
                    .append(true)
                    .open(&last.path)
                    .map_err(|e| Error::Query(format!("open WAL segment: {e}")))?;
                let start_seq = last.start_seq;
                let last_seq = last.last_seq;
                let mut writer = Self {
                    dir,
                    file,
                    path: last.path.clone(),
                    start_seq,
                    last_seq,
                    bytes: last.valid_bytes,
                    segment_bytes,
                };
                if writer.bytes >= writer.segment_bytes {
                    writer.rotate(last_seq.saturating_add(1))?;
                }
                return Ok(writer);
            }
            // Empty or invalid newest header. It was never usable.
            let _ = fs::remove_file(&last.path);
        }
        let start_seq = segments
            .iter()
            .map(|segment| segment.last_seq)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let (path, file, bytes) = create_segment(&dir, start_seq)?;
        Ok(Self {
            dir,
            file,
            path,
            start_seq,
            last_seq: start_seq.saturating_sub(1),
            bytes,
            segment_bytes,
        })
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn rotate(&mut self, start_seq: u64) -> Result<()> {
        let (path, file, bytes) = create_segment(&self.dir, start_seq)?;
        self.file = file;
        self.path = path;
        self.start_seq = start_seq;
        self.last_seq = start_seq.saturating_sub(1);
        self.bytes = bytes;
        Ok(())
    }

    /// Appends one transaction. Older or duplicate sequences are skipped;
    /// gaps are rejected so a missing WAL entry can never be mistaken for a
    /// clean history.
    pub fn append(&mut self, txn: &ReplicationTxn) -> Result<bool> {
        if txn.seq <= self.last_seq {
            return Ok(false);
        }
        let expected = self.last_seq.saturating_add(1);
        if txn.seq != expected {
            return Err(Error::Query(format!(
                "WAL sequence gap: expected {expected}, got {}",
                txn.seq
            )));
        }
        let payload = serde_json::to_vec(txn)?;
        let digest = hex_digest(&payload);
        let record = WalRecord {
            sha256: digest,
            txn: txn.clone(),
        };
        let mut line = serde_json::to_vec(&record)?;
        line.push(b'\n');
        if self.bytes.saturating_add(line.len() as u64) > self.segment_bytes && self.bytes > 0 {
            self.rotate(txn.seq)?;
        }
        self.file
            .write_all(&line)
            .map_err(|e| Error::Query(format!("append WAL record: {e}")))?;
        self.bytes = self.bytes.saturating_add(line.len() as u64);
        self.last_seq = txn.seq;
        Ok(true)
    }

    pub fn append_batch(&mut self, txns: &[ReplicationTxn]) -> Result<usize> {
        let mut appended = 0;
        for txn in txns {
            if self.append(txn)? {
                appended += 1;
            }
        }
        Ok(appended)
    }

    /// Rotates to a fresh segment without changing the logical head. Used
    /// when retention wants to delete a fully-covered active segment.
    pub fn rotate_after_current(&mut self) -> Result<()> {
        self.rotate(self.last_seq.saturating_add(1))
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file
            .sync_data()
            .map_err(|e| Error::Query(format!("sync WAL: {e}")))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn start_seq(&self) -> u64 {
        self.start_seq
    }
}

/// Reads WAL transactions after `after` and stops at the first transaction
/// newer than `target_ms`.
pub fn read_through(
    dir: impl AsRef<Path>,
    after: u64,
    target_ms: u64,
    limit: usize,
) -> Result<Vec<ReplicationTxn>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let dir = dir.as_ref();
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut segments = scan_segments(dir)?;
    segments.sort_by_key(|segment| (segment.start_seq, segment.created_at_ms));
    let mut out = Vec::new();
    for segment in segments {
        if segment.last_seq <= after {
            continue;
        }
        let file = File::open(&segment.path)
            .map_err(|e| Error::Query(format!("open WAL segment: {e}")))?;
        let mut reader = BufReader::new(file);
        let mut line = String::new();
        let mut header_seen = false;
        loop {
            line.clear();
            let read = reader
                .read_line(&mut line)
                .map_err(|e| Error::Query(format!("read WAL segment: {e}")))?;
            if read == 0 {
                break;
            }
            if line.trim().is_empty() {
                continue;
            }
            if !header_seen {
                header_seen = true;
                let header: WalHeader = serde_json::from_str(line.trim())?;
                if header.format != WAL_FORMAT || header.version != WAL_VERSION {
                    return Err(Error::Query(format!(
                        "unsupported WAL format in {}",
                        segment.path.display()
                    )));
                }
                continue;
            }
            let record: WalRecord = serde_json::from_str(line.trim())?;
            let expected = record.txn.seq.to_string();
            let actual = hex_digest(&serde_json::to_vec(&record.txn)?);
            if record.sha256 != actual {
                return Err(Error::Query(format!(
                    "WAL checksum mismatch in {} at sequence {expected}",
                    segment.path.display()
                )));
            }
            if record.txn.seq <= after {
                continue;
            }
            if record.txn.created_at_ms > target_ms {
                return Ok(out);
            }
            out.push(record.txn);
            if out.len() >= limit {
                return Ok(out);
            }
        }
    }
    Ok(out)
}

/// Replays WAL transactions into a restored store until `target_ms`.
pub fn replay_to(store: &Store, dir: impl AsRef<Path>, target_ms: u64) -> Result<ReplayReport> {
    let snapshot_head = store.replication_head()?;
    let mut expected = snapshot_head.saturating_add(1);
    let mut last_replayed_seq = snapshot_head;
    let mut applied = 0;
    let mut stopped_at_ms = None;
    // PITR windows are bounded by WAL size, but this loop also bounds memory
    // usage for a large retained WAL.
    loop {
        let batch = read_through(dir.as_ref(), last_replayed_seq, target_ms, 512)?;
        if batch.is_empty() {
            break;
        }
        let batch_len = batch.len();
        for txn in batch {
            if txn.seq > expected {
                return Err(Error::Query(format!(
                    "PITR WAL gap: expected sequence {expected}, found {}",
                    txn.seq
                )));
            }
            if txn.seq < expected {
                continue;
            }
            if store.apply_pitr_txn(&txn)? {
                applied += 1;
            }
            expected = txn.seq.saturating_add(1);
            last_replayed_seq = txn.seq;
            stopped_at_ms = Some(txn.created_at_ms);
        }
        if batch_len < 512 {
            break;
        }
    }
    Ok(ReplayReport {
        target_ms,
        snapshot_head,
        last_replayed_seq,
        applied,
        stopped_at_ms,
    })
}

/// Removes sealed WAL segments fully covered by `through`. The newest segment
/// is always kept even when covered because it may still be open for appends.
/// Returns the number of bytes removed.
pub fn prune_through(dir: impl AsRef<Path>, through: u64) -> Result<u64> {
    let dir = dir.as_ref();
    if !dir.is_dir() {
        return Ok(0);
    }
    let mut segments = scan_segments(dir)?;
    segments.sort_by_key(|segment| (segment.start_seq, segment.created_at_ms));
    let Some(newest) = segments.last().map(|segment| segment.path.clone()) else {
        return Ok(0);
    };
    let mut removed: u64 = 0;
    for segment in segments {
        if segment.path == newest || segment.last_seq > through {
            continue;
        }
        let len = fs::metadata(&segment.path)
            .map(|meta| meta.len())
            .unwrap_or(0);
        match fs::remove_file(&segment.path) {
            Ok(()) => removed = removed.saturating_add(len),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(Error::Query(format!(
                    "remove WAL segment {}: {error}",
                    segment.path.display()
                )))
            }
        }
    }
    Ok(removed)
}

/// Total bytes used by WAL files.
pub fn bytes_used(dir: impl AsRef<Path>) -> u64 {
    let Ok(entries) = fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|entry| is_wal_file(&entry.path()))
        .filter_map(|entry| entry.metadata().ok().map(|meta| meta.len()))
        .sum()
}

/// Timestamp of the oldest WAL segment, used by age alerts.
pub fn oldest_created_at_ms(dir: impl AsRef<Path>) -> Option<u64> {
    let mut segments = scan_segments(dir.as_ref()).ok()?;
    segments.sort_by_key(|segment| (segment.start_seq, segment.created_at_ms));
    segments.first().map(|segment| segment.created_at_ms)
}

/// Returns the highest complete sequence in the WAL, or zero.
pub fn last_seq(dir: impl AsRef<Path>) -> u64 {
    scan_segments(dir.as_ref())
        .ok()
        .and_then(|segments| segments.iter().map(|segment| segment.last_seq).max())
        .unwrap_or(0)
}

/// Returns the timestamp of the first WAL transaction at or after `seq`.
pub fn first_txn_at_or_after(dir: impl AsRef<Path>, seq: u64) -> Result<Option<u64>> {
    let dir = dir.as_ref();
    if !dir.is_dir() {
        return Ok(None);
    }
    let mut segments = scan_segments(dir)?;
    segments.sort_by_key(|segment| (segment.start_seq, segment.created_at_ms));
    for segment in segments {
        if segment.last_seq < seq {
            continue;
        }
        let file = File::open(&segment.path)
            .map_err(|e| Error::Query(format!("open WAL segment: {e}")))?;
        let mut reader = BufReader::new(file);
        let mut line = String::new();
        let mut header_seen = false;
        loop {
            line.clear();
            if reader
                .read_line(&mut line)
                .map_err(|e| Error::Query(format!("read WAL segment: {e}")))?
                == 0
            {
                break;
            }
            if line.trim().is_empty() {
                continue;
            }
            if !header_seen {
                header_seen = true;
                continue;
            }
            let record: WalRecord = serde_json::from_str(line.trim())?;
            if record.txn.seq >= seq {
                return Ok(Some(record.txn.created_at_ms));
            }
        }
    }
    Ok(None)
}

/// Path of the sidecar metadata for a `data-<unix-ms>.mdb` snapshot.
pub fn snapshot_meta_path(snapshot: &Path) -> PathBuf {
    snapshot.with_extension("json")
}

/// Writes snapshot metadata and fsyncs it. This is the verification marker:
/// WAL pruning only uses snapshots whose metadata was written successfully.
pub fn write_snapshot_meta(snapshot: &Path, database: &str, replication_head: u64) -> Result<()> {
    let created_at_ms = snapshot_timestamp(snapshot).unwrap_or_else(now_ms);
    let meta = SnapshotMeta {
        format: SNAPSHOT_META_FORMAT.into(),
        version: SNAPSHOT_META_VERSION,
        database: database.to_string(),
        created_at_ms,
        replication_head,
    };
    let path = snapshot_meta_path(snapshot);
    let tmp = path.with_extension(format!("json.tmp.{}", std::process::id()));
    let bytes = serde_json::to_vec_pretty(&meta)?;
    let mut file =
        File::create(&tmp).map_err(|e| Error::Query(format!("create snapshot metadata: {e}")))?;
    file.write_all(&bytes)
        .map_err(|e| Error::Query(format!("write snapshot metadata: {e}")))?;
    file.write_all(b"\n")
        .map_err(|e| Error::Query(format!("write snapshot metadata: {e}")))?;
    file.sync_all()
        .map_err(|e| Error::Query(format!("sync snapshot metadata: {e}")))?;
    fs::rename(&tmp, &path).map_err(|e| Error::Query(format!("install snapshot metadata: {e}")))?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

/// Reads snapshot metadata, returning `None` for legacy snapshots.
pub fn read_snapshot_meta(snapshot: &Path) -> Result<Option<SnapshotMeta>> {
    let path = snapshot_meta_path(snapshot);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(Error::Query(format!(
                "read snapshot metadata {}: {error}",
                path.display()
            )))
        }
    };
    let meta: SnapshotMeta = serde_json::from_slice(&bytes)?;
    if meta.format != SNAPSHOT_META_FORMAT || meta.version != SNAPSHOT_META_VERSION {
        return Err(Error::Query(format!(
            "unsupported snapshot metadata format in {}",
            path.display()
        )));
    }
    Ok(Some(meta))
}

/// Removes snapshot metadata alongside a pruned snapshot.
pub fn remove_snapshot_meta(snapshot: &Path) {
    let _ = fs::remove_file(snapshot_meta_path(snapshot));
}

fn scan_segments(dir: &Path) -> Result<Vec<SegmentInfo>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(Error::Query(format!(
                "read WAL directory {}: {error}",
                dir.display()
            )))
        }
    };
    let mut segments = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !is_wal_file(&path) {
            continue;
        }
        match inspect_segment(&path) {
            Ok(Some(segment)) => segments.push(segment),
            Ok(None) => {}
            Err(error) => {
                return Err(Error::Query(format!(
                    "invalid WAL segment {}: {error}",
                    path.display()
                )))
            }
        }
    }
    Ok(segments)
}

fn inspect_segment(path: &Path) -> std::result::Result<Option<SegmentInfo>, String> {
    let mut file = File::open(path).map_err(|e| e.to_string())?;
    let len = file.metadata().map_err(|e| e.to_string())?.len();
    let mut reader = BufReader::new(file.try_clone().map_err(|e| e.to_string())?);
    let mut header_line = String::new();
    if reader
        .read_line(&mut header_line)
        .map_err(|e| e.to_string())?
        == 0
    {
        return Ok(None);
    }
    let header: WalHeader = serde_json::from_str(header_line.trim()).map_err(|e| e.to_string())?;
    if header.format != WAL_FORMAT || header.version != WAL_VERSION {
        return Err(format!("unsupported format/version: {header:?}"));
    }
    let mut last_seq = header.start_seq.saturating_sub(1);
    let mut valid_bytes = header_line.len() as u64;
    let mut line = String::new();
    loop {
        line.clear();
        let read = reader.read_line(&mut line).map_err(|e| e.to_string())?;
        if read == 0 {
            break;
        }
        let next = valid_bytes.saturating_add(read as u64);
        if line.trim().is_empty() {
            valid_bytes = next;
            continue;
        }
        let record: WalRecord = match serde_json::from_str(line.trim()) {
            Ok(record) => record,
            Err(_) => break,
        };
        let expected = hex_digest(&serde_json::to_vec(&record.txn).map_err(|e| e.to_string())?);
        if record.sha256 != expected {
            break;
        }
        if record.txn.seq <= last_seq {
            break;
        }
        last_seq = record.txn.seq;
        valid_bytes = next;
    }
    // A complete final line may not include a trailing newline if the file was
    // cut exactly after the JSON. Treat it as a valid tail only when the bytes
    // were consumed; otherwise the next append starts a new line safely.
    if valid_bytes == len {
        file.seek(SeekFrom::Start(valid_bytes.saturating_sub(1)))
            .map_err(|e| e.to_string())?;
        let mut last = [0u8; 1];
        if file.read_exact(&mut last).is_ok() && last[0] != b'\n' {
            // Complete record without newline: add the separator before the
            // next append. This is rare and only affects externally edited
            // files; our writer always terminates records.
            let mut append = OpenOptions::new()
                .append(true)
                .open(path)
                .map_err(|e| e.to_string())?;
            append.write_all(b"\n").map_err(|e| e.to_string())?;
            valid_bytes = valid_bytes.saturating_add(1);
        }
    }
    Ok(Some(SegmentInfo {
        path: path.to_path_buf(),
        start_seq: header.start_seq,
        last_seq,
        created_at_ms: header.created_at_ms,
        valid_bytes,
    }))
}

fn create_segment(dir: &Path, start_seq: u64) -> Result<(PathBuf, File, u64)> {
    let created_at_ms = now_ms();
    let path = dir.join(format!(
        "{WAL_PREFIX}{start_seq:020}-{created_at_ms:013}{WAL_SUFFIX}"
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(&path)
        .map_err(|e| Error::Query(format!("create WAL segment {}: {e}", path.display())))?;
    let header = WalHeader {
        format: WAL_FORMAT.into(),
        version: WAL_VERSION,
        start_seq,
        created_at_ms,
    };
    let mut line = serde_json::to_vec(&header)?;
    line.push(b'\n');
    file.write_all(&line)
        .map_err(|e| Error::Query(format!("write WAL header: {e}")))?;
    file.sync_data()
        .map_err(|e| Error::Query(format!("sync WAL header: {e}")))?;
    sync_dir(dir)?;
    Ok((path, file, line.len() as u64))
}

fn is_wal_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with(WAL_PREFIX) && name.ends_with(WAL_SUFFIX))
}

fn snapshot_timestamp(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let value = name.strip_prefix("data-")?.strip_suffix(".mdb")?;
    value.parse::<u64>().ok()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn hex_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let file =
            File::open(dir).map_err(|e| Error::Query(format!("open directory for fsync: {e}")))?;
        file.sync_all()
            .map_err(|e| Error::Query(format!("sync directory: {e}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::{ReplicationChange, ReplicationTxn};

    fn txn(seq: u64, created_at_ms: u64, id: &str) -> ReplicationTxn {
        let mut txn = ReplicationTxn::new(
            seq,
            "test",
            vec![ReplicationChange {
                collection: "docs".into(),
                op: "insert".into(),
                id: id.into(),
                before: None,
                after: Some(serde_json::json!({"_id": id})),
                vault: false,
                ttl_at: None,
            }],
        );
        txn.created_at_ms = created_at_ms;
        txn
    }

    #[test]
    fn wal_segments_roll_and_reader_stops_at_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = WalWriter::open(dir.path(), 1024).unwrap();
        for seq in 1..=20 {
            writer
                .append(&txn(seq, seq * 1000, &format!("d{seq}")))
                .unwrap();
        }
        writer.sync().unwrap();
        assert!(scan_segments(dir.path()).unwrap().len() > 1);
        let rows = read_through(dir.path(), 0, 10_500, 100).unwrap();
        assert_eq!(rows.len(), 10);
        assert_eq!(rows.last().unwrap().seq, 10);
        assert_eq!(last_seq(dir.path()), 20);
    }

    #[test]
    fn reopening_truncates_a_partial_crash_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = WalWriter::open(dir.path(), 1 << 20).unwrap();
        writer.append(&txn(1, 1000, "a")).unwrap();
        writer.append(&txn(2, 2000, "b")).unwrap();
        writer.sync().unwrap();
        let path = writer.path().to_path_buf();
        drop(writer);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"{\"sha256\":\"broken\"").unwrap();
        file.sync_all().unwrap();
        drop(file);

        let mut writer = WalWriter::open(dir.path(), 1 << 20).unwrap();
        assert_eq!(writer.last_seq(), 2);
        writer.append(&txn(3, 3000, "c")).unwrap();
        writer.sync().unwrap();
        let rows = read_through(dir.path(), 0, 10_000, 10).unwrap();
        assert_eq!(
            rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn prune_keeps_the_newest_and_uncovered_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = WalWriter::open(dir.path(), 700).unwrap();
        for seq in 1..=12 {
            writer
                .append(&txn(seq, seq * 1000, &format!("d{seq}")))
                .unwrap();
        }
        writer.sync().unwrap();
        let before = bytes_used(dir.path());
        let removed = prune_through(dir.path(), 8).unwrap();
        assert!(removed > 0);
        assert!(bytes_used(dir.path()) < before);
        let rows = read_through(dir.path(), 8, 100_000, 100).unwrap();
        assert!(rows.iter().all(|row| row.seq > 8));
        assert_eq!(last_seq(dir.path()), 12);
    }

    #[test]
    fn snapshot_metadata_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = dir.path().join("data-1234.mdb");
        fs::write(&snapshot, b"x").unwrap();
        assert!(read_snapshot_meta(&snapshot).unwrap().is_none());
        write_snapshot_meta(&snapshot, "app", 42).unwrap();
        let meta = read_snapshot_meta(&snapshot).unwrap().unwrap();
        assert_eq!(meta.replication_head, 42);
        assert_eq!(meta.created_at_ms, 1234);
        remove_snapshot_meta(&snapshot);
        assert!(read_snapshot_meta(&snapshot).unwrap().is_none());
    }
}
