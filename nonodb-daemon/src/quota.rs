//! Per-database storage quotas and usage accounting.
//!
//! Limits are persisted in `<database>/quota.json` so they survive restarts and
//! are independent of the LMDB environment. Usage is measured from LMDB
//! page allocation plus the immutable search/vector sidecar directories.
//! Writers reject requests that would push an exhausted resource past its
//! limit with the `quota_exceeded` error. All other writes keep working.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::protocol::{ApiError, WriteReq};

const QUOTA_FILE: &str = "quota.json";
const QUOTA_VERSION: u32 = 1;
const DEFAULT_SOFT_PERCENT: u64 = 80;
const WARNING_LOG_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaLimits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sidecar_bytes: Option<u64>,
}

impl QuotaLimits {
    fn normalize(mut self) -> Self {
        self.data_bytes = self.data_bytes.filter(|value| *value > 0);
        self.index_bytes = self.index_bytes.filter(|value| *value > 0);
        self.history_bytes = self.history_bytes.filter(|value| *value > 0);
        self.sidecar_bytes = self.sidecar_bytes.filter(|value| *value > 0);
        self
    }

    pub fn is_unlimited(&self) -> bool {
        self.data_bytes.is_none()
            && self.index_bytes.is_none()
            && self.history_bytes.is_none()
            && self.sidecar_bytes.is_none()
    }

    fn limit(&self, resource: Resource) -> Option<u64> {
        match resource {
            Resource::Data => self.data_bytes,
            Resource::Index => self.index_bytes,
            Resource::History => self.history_bytes,
            Resource::Sidecar => self.sidecar_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct QuotaUsage {
    pub data_bytes: u64,
    pub index_bytes: u64,
    pub history_bytes: u64,
    pub sidecar_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct QuotaWarning {
    pub resource: &'static str,
    pub used_bytes: u64,
    pub limit_bytes: u64,
    pub used_percent: u64,
    pub severity: &'static str,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct QuotaReport {
    pub database: String,
    pub limits: QuotaLimits,
    pub usage: QuotaUsage,
    pub status: &'static str,
    pub soft_limit_percent: u64,
    pub warnings: Vec<QuotaWarning>,
}

#[derive(Serialize, Deserialize)]
struct PersistedQuota {
    version: u32,
    limits: QuotaLimits,
}

#[derive(Clone, Copy)]
enum Resource {
    Data,
    Index,
    History,
    Sidecar,
}

impl Resource {
    fn name(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Index => "index",
            Self::History => "history",
            Self::Sidecar => "sidecar",
        }
    }

    fn used(self, usage: &QuotaUsage) -> u64 {
        match self {
            Self::Data => usage.data_bytes,
            Self::Index => usage.index_bytes,
            Self::History => usage.history_bytes,
            Self::Sidecar => usage.sidecar_bytes,
        }
    }
}

/// Per-database quota configuration and a small warning-log rate limiter.
pub struct Quota {
    database: String,
    path: PathBuf,
    limits: RwLock<QuotaLimits>,
    last_warning: Mutex<Option<Instant>>,
}

impl Quota {
    pub fn load(database: &str, database_path: &Path) -> Result<Self, String> {
        let path = database_path.join(QUOTA_FILE);
        let limits = if path.is_file() {
            let raw = fs::read(&path)
                .map_err(|error| format!("read quota config {}: {error}", path.display()))?;
            let persisted: PersistedQuota = serde_json::from_slice(&raw)
                .map_err(|error| format!("parse quota config {}: {error}", path.display()))?;
            if persisted.version != QUOTA_VERSION {
                return Err(format!(
                    "unsupported quota config version {} in {}",
                    persisted.version,
                    path.display()
                ));
            }
            persisted.limits.normalize()
        } else {
            QuotaLimits {
                data_bytes: env_limit("NONODB_QUOTA_DATA_BYTES"),
                index_bytes: env_limit("NONODB_QUOTA_INDEX_BYTES"),
                history_bytes: env_limit("NONODB_QUOTA_HISTORY_BYTES"),
                sidecar_bytes: env_limit("NONODB_QUOTA_SIDECAR_BYTES"),
            }
            .normalize()
        };
        Ok(Self {
            database: database.to_string(),
            path,
            limits: RwLock::new(limits),
            last_warning: Mutex::new(None),
        })
    }

    pub fn limits(&self) -> QuotaLimits {
        self.limits
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Replace the configuration and persist it before making it visible to
    /// writers. A zero limit means unlimited.
    pub fn set_limits(&self, limits: QuotaLimits) -> Result<QuotaLimits, ApiError> {
        let limits = limits.normalize();
        let persisted = PersistedQuota {
            version: QUOTA_VERSION,
            limits: limits.clone(),
        };
        let encoded = serde_json::to_vec_pretty(&persisted)
            .map_err(|error| ApiError::internal(format!("encode quota config: {error}")))?;
        write_atomic(&self.path, &encoded)
            .map_err(|error| ApiError::internal(format!("write quota config: {error}")))?;
        if let Ok(mut guard) = self.limits.write() {
            *guard = limits.clone();
        }
        Ok(limits)
    }

    pub fn report(&self, store: &nonodb_core::Store) -> Result<QuotaReport, ApiError> {
        let usage = self.usage(store)?;
        Ok(self.evaluate(usage))
    }

    fn evaluate(&self, usage: QuotaUsage) -> QuotaReport {
        let limits = self.limits();
        let soft = soft_percent();
        let mut warnings = Vec::new();
        let mut exceeded = false;
        for resource in [
            Resource::Data,
            Resource::Index,
            Resource::History,
            Resource::Sidecar,
        ] {
            let Some(limit) = limits.limit(resource) else {
                continue;
            };
            let used = resource.used(&usage);
            let percent = used.saturating_mul(100).checked_div(limit).unwrap_or(100);
            if used >= limit {
                exceeded = true;
                warnings.push(warning(resource, used, limit, percent, "exceeded"));
            } else if percent >= soft {
                warnings.push(warning(resource, used, limit, percent, "warning"));
            }
        }
        let status = if exceeded {
            "exceeded"
        } else if warnings.is_empty() {
            "ok"
        } else {
            "warning"
        };
        QuotaReport {
            database: self.database.clone(),
            limits,
            usage,
            status,
            soft_limit_percent: soft,
            warnings,
        }
    }

    fn usage(&self, store: &nonodb_core::Store) -> Result<QuotaUsage, ApiError> {
        let stats = store.storage_stats()?;
        let mut data = 0u64;
        let mut index = 0u64;
        let mut history = 0u64;
        for collection in &stats.collections {
            data = data
                .saturating_add(collection.data_bytes)
                .saturating_add(collection.ttl_bytes);
            index = index.saturating_add(collection.index_bytes);
            history = history.saturating_add(collection.history_bytes);
        }
        let root = self
            .path
            .parent()
            .ok_or_else(|| ApiError::internal("quota path has no parent directory"))?;
        let sidecar = directory_bytes(&root.join("search"))
            .saturating_add(directory_bytes(&root.join("vector-ann")));
        Ok(QuotaUsage {
            data_bytes: data,
            index_bytes: index,
            history_bytes: history,
            sidecar_bytes: sidecar,
        })
    }

    /// Reject a writer batch when a resource is exhausted or the request
    /// estimate would cross a hard limit. Destructive-only batches always pass
    /// so an operator can free space after a quota was hit.
    pub fn check_write(
        &self,
        store: &nonodb_core::Store,
        jobs: &[&WriteReq],
    ) -> Result<(), ApiError> {
        let limits = self.limits();
        if limits.is_unlimited() {
            return Ok(());
        }
        let mut growth = Growth::default();
        let mut estimate = 0u64;
        for req in jobs {
            let request_growth = Growth::for_request(req);
            if request_growth.any() {
                growth.union(request_growth);
                estimate = estimate.saturating_add(format!("{req:?}").len() as u64);
            }
        }
        if !growth.any() {
            return Ok(());
        }
        let usage = self.usage(store)?;
        let report = self.evaluate(usage);
        if report.status != "ok" {
            self.log_warning(&report);
        }
        for resource in [
            Resource::Data,
            Resource::Index,
            Resource::History,
            Resource::Sidecar,
        ] {
            if !growth.grows(resource) {
                continue;
            }
            let Some(limit) = limits.limit(resource) else {
                continue;
            };
            let used = resource.used(&usage);
            if used >= limit || used.saturating_add(estimate) > limit {
                let message = format!(
                    "database `{}` {} quota exceeded: {} of {} bytes used, request needs up to {} more bytes",
                    self.database,
                    resource.name(),
                    used,
                    limit,
                    estimate
                );
                tracing::error!(
                    database = %self.database,
                    resource = resource.name(),
                    used,
                    limit,
                    estimate,
                    "write rejected by quota"
                );
                return Err(ApiError {
                    code: "quota_exceeded".to_string(),
                    message,
                });
            }
        }
        Ok(())
    }

    fn log_warning(&self, report: &QuotaReport) {
        let now = Instant::now();
        let should_log = {
            let Ok(mut last) = self.last_warning.lock() else {
                return;
            };
            let should_log = last
                .map(|previous| now.duration_since(previous) >= WARNING_LOG_INTERVAL)
                .unwrap_or(true);
            if should_log {
                *last = Some(now);
            }
            should_log
        };
        if should_log {
            tracing::warn!(
                database = %self.database,
                status = report.status,
                warnings = report.warnings.len(),
                "database storage quota is near or at its limit"
            );
        }
    }
}

fn warning(
    resource: Resource,
    used: u64,
    limit: u64,
    percent: u64,
    severity: &'static str,
) -> QuotaWarning {
    QuotaWarning {
        resource: resource.name(),
        used_bytes: used,
        limit_bytes: limit,
        used_percent: percent,
        severity,
        message: format!(
            "{} usage is {} of {} bytes ({}%)",
            resource.name(),
            used,
            limit,
            percent
        ),
    }
}

#[derive(Clone, Copy, Default)]
struct Growth {
    data: bool,
    index: bool,
    history: bool,
    sidecar: bool,
}

impl Growth {
    fn any(self) -> bool {
        self.data || self.index || self.history || self.sidecar
    }

    fn union(&mut self, other: Self) {
        self.data |= other.data;
        self.index |= other.index;
        self.history |= other.history;
        self.sidecar |= other.sidecar;
    }

    fn grows(self, resource: Resource) -> bool {
        match resource {
            Resource::Data => self.data,
            Resource::Index => self.index,
            Resource::History => self.history,
            Resource::Sidecar => self.sidecar,
        }
    }

    fn for_request(req: &WriteReq) -> Self {
        use WriteReq::*;
        let document = Self {
            data: true,
            index: true,
            history: true,
            sidecar: false,
        };
        match req {
            Insert { .. }
            | InsertMany { .. }
            | ExportGraphResult { .. }
            | Update { .. }
            | Sql { .. }
            | Aggregate { .. } => document,
            FindAndModify { remove, .. } => {
                if *remove {
                    Self::default()
                } else {
                    document
                }
            }
            Delete { .. }
            | DropCollection { .. }
            | DropIndex { .. }
            | DropTextIndex { .. }
            | DropFuzzyIndex { .. }
            | DropVectorIndex { .. }
            | DropSearchIndex { .. }
            | DropGraph { .. }
            | CancelGraphRebuild { .. }
            | PruneSubscriptions
            | PruneHistory { .. }
            | Expire { .. }
            | DeleteSubscription { .. }
            | DeleteWebhook { .. }
            | RetainTimeSeries { .. } => Self::default(),
            CreateCollection { .. }
            | SetGraph { .. }
            | ReindexGraph { .. }
            | StartGraphRebuild { .. }
            | GraphRebuildStep { .. }
            | SetCollectionTtl { .. }
            | SetCollectionTtlAfter { .. }
            | SetCollectionTimeSeries { .. }
            | SetCollectionColumnar { .. }
            | BackfillCollectionTtl { .. } => Self {
                data: true,
                ..Self::default()
            },
            SetCollectionHistory { max_versions, .. } => Self {
                history: *max_versions > 0,
                ..Self::default()
            },
            CreateStream { .. } => Self {
                index: true,
                ..Self::default()
            },
            DropStream { .. }
            | CreateStreamGroup { .. }
            | ResetStreamGroup { .. }
            | DeleteStreamGroup { .. }
            | CommitStreamGroup { .. }
            | ClaimStreamGroup { .. }
            | NackStreamGroup { .. }
            | PruneStream { .. }
            | RecoverStreamLeases { .. }
            | RetryStreamDead { .. }
            | DropStreamDead { .. } => Self::default(),
            SetCollectionEvents { .. }
            | CreateSubscription { .. }
            | RenewSubscription { .. }
            | CreateWebhook { .. }
            | RetryWebhookDelivery { .. } => Self::default(),
            CreateIndex { .. } | CreateIndexes { .. } | Reindex { .. } => Self {
                index: true,
                ..Self::default()
            },
            CreateSearchIndex { .. }
            | CreateTextIndex { .. }
            | CreateFuzzyIndex { .. }
            | CreateVectorIndex { .. } => Self {
                index: true,
                sidecar: true,
                ..Self::default()
            },
            Batch { ops } => {
                let mut growth = Self::default();
                for op in ops {
                    growth.union(Self::for_request(op));
                }
                growth
            }
        }
    }
}

fn soft_percent() -> u64 {
    std::env::var("NONODB_QUOTA_SOFT_PERCENT")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| (1..=100).contains(value))
        .unwrap_or(DEFAULT_SOFT_PERCENT)
}

fn env_limit(name: &str) -> Option<u64> {
    let raw = std::env::var(name).ok()?;
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let parsed = nonodb_core::parse_size(raw).map(|value| value as u64);
    if parsed.is_none() {
        tracing::warn!(variable = name, value = raw, "ignoring invalid quota size");
    }
    parsed.filter(|value| *value > 0)
}

fn write_atomic(path: &Path, contents: &[u8]) -> Result<(), String> {
    let parent = path.parent().ok_or("quota path has no parent")?;
    fs::create_dir_all(parent).map_err(|error| format!("create {}: {error}", parent.display()))?;
    let tmp = path.with_extension(format!("json.tmp.{}", std::process::id()));
    {
        let mut file =
            fs::File::create(&tmp).map_err(|error| format!("create {}: {error}", tmp.display()))?;
        use std::io::Write;
        file.write_all(contents)
            .map_err(|error| format!("write {}: {error}", tmp.display()))?;
        file.sync_all()
            .map_err(|error| format!("sync {}: {error}", tmp.display()))?;
    }
    if let Err(first) = fs::rename(&tmp, path) {
        if path.exists() {
            let _ = fs::remove_file(path);
            fs::rename(&tmp, path).map_err(|second| format!("{first}; retry: {second}"))
        } else {
            Err(first.to_string())
        }
    } else {
        Ok(())
    }
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(path) else {
        return 0;
    };
    let mut total = 0u64;
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.is_dir() {
            total = total.saturating_add(directory_bytes(&entry.path()));
        } else if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (tempfile::TempDir, Quota) {
        let dir = tempfile::tempdir().unwrap();
        let quota = Quota::load("test", dir.path()).unwrap();
        (dir, quota)
    }

    #[test]
    fn limits_persist_and_zero_clears() {
        let (dir, quota) = setup();
        quota
            .set_limits(QuotaLimits {
                data_bytes: Some(1024),
                index_bytes: Some(0),
                history_bytes: None,
                sidecar_bytes: Some(2 * 1024),
            })
            .unwrap();
        let loaded = Quota::load("test", dir.path()).unwrap();
        assert_eq!(loaded.limits().data_bytes, Some(1024));
        assert_eq!(loaded.limits().index_bytes, None);
        assert_eq!(loaded.limits().sidecar_bytes, Some(2 * 1024));
        assert!(dir.path().join(QUOTA_FILE).is_file());

        quota
            .set_limits(QuotaLimits {
                data_bytes: Some(0),
                ..QuotaLimits::default()
            })
            .unwrap();
        assert!(quota.limits().is_unlimited());
    }

    #[test]
    fn report_warns_at_soft_limit_and_flags_exhaustion() {
        let (_dir, quota) = setup();
        quota
            .set_limits(QuotaLimits {
                data_bytes: Some(100),
                ..QuotaLimits::default()
            })
            .unwrap();
        let warning = quota.evaluate(QuotaUsage {
            data_bytes: 85,
            ..QuotaUsage::default()
        });
        assert_eq!(warning.status, "warning");
        assert_eq!(warning.warnings.len(), 1);
        assert_eq!(warning.warnings[0].resource, "data");
        assert_eq!(warning.warnings[0].severity, "warning");

        let exceeded = quota.evaluate(QuotaUsage {
            data_bytes: 100,
            ..QuotaUsage::default()
        });
        assert_eq!(exceeded.status, "exceeded");
        assert_eq!(exceeded.warnings[0].severity, "exceeded");
    }

    #[test]
    fn sidecar_usage_walks_both_sidecar_directories() {
        let (dir, _quota) = setup();
        for (directory, name, bytes) in [("search", "a.seg", 7u64), ("vector-ann", "b.ann", 11u64)]
        {
            let path = dir.path().join(directory).join("nested");
            fs::create_dir_all(&path).unwrap();
            fs::write(path.join(name), vec![0u8; bytes as usize]).unwrap();
        }
        assert_eq!(directory_bytes(&dir.path().join("search")), 7);
        assert_eq!(directory_bytes(&dir.path().join("vector-ann")), 11);
        // The report still needs a store, but the directory walk itself is the
        // part that distinguishes sidecar from LMDB page accounting.
        assert!(dir.path().join("search").is_dir());
    }

    #[test]
    fn destructive_requests_do_not_grow_any_resource() {
        let growth = Growth::for_request(&WriteReq::Delete {
            coll: "docs".into(),
            filter: serde_json::json!({}),
            multi: true,
            vault: None,
        });
        assert!(!growth.any());
        let insert = Growth::for_request(&WriteReq::Insert {
            coll: "docs".into(),
            doc: serde_json::json!({"x": 1}),
            vault: None,
        });
        assert!(insert.data && insert.index && insert.history && !insert.sidecar);
    }
}
