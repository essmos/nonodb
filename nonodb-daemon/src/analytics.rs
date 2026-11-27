//! Background graph-analytics jobs.
//!
//! Jobs are deliberately in-memory and database-scoped: they are cancelled and
//! joined when the database runtime shuts down. Results that must survive a
//! restart belong in a collection (see `export`), not in this manager.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use nonodb_core::graph::{GraphAlgorithmObserver, GraphAlgorithmOptions};
use nonodb_core::Store;
use serde_json::{json, Value};

use crate::protocol::{ApiError, Durability, WriteReq};
use crate::writer::WriteJob;

const MAX_CONCURRENT_JOBS: usize = 2;
const MAX_TERMINAL_JOBS: usize = 100;
const MAX_JOB_RESULT_BYTES: usize = 64 * 1024 * 1024;
const MAX_RETAINED_RESULT_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobState {
    Queued,
    Running,
    Cancelling,
    Exporting,
    Completed,
    Failed,
    Cancelled,
}

impl JobState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Cancelling => "cancelling",
            Self::Exporting => "exporting",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone)]
struct Progress {
    phase: String,
    done: usize,
    total: Option<usize>,
}

impl Default for Progress {
    fn default() -> Self {
        Self {
            phase: "queued".into(),
            done: 0,
            total: None,
        }
    }
}

struct Job {
    id: String,
    owner: String,
    coll: String,
    kind: String,
    state: JobState,
    progress: Arc<Mutex<Progress>>,
    cancel: Arc<AtomicBool>,
    result: Option<Arc<Value>>,
    result_bytes: usize,
    error: Option<ApiError>,
    created_ms: u128,
    started_ms: Option<u128>,
    finished_ms: Option<u128>,
    handle: Option<std::thread::JoinHandle<()>>,
}

struct Inner {
    jobs: HashMap<String, Job>,
    order: VecDeque<String>,
    next_id: u64,
    running: usize,
}

pub struct AnalyticsManager {
    inner: Mutex<Inner>,
    stop: Arc<AtomicBool>,
    database: String,
    write_tx: mpsc::Sender<WriteJob>,
}

impl AnalyticsManager {
    pub fn new(
        database: impl Into<String>,
        stop: Arc<AtomicBool>,
        write_tx: mpsc::Sender<WriteJob>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                jobs: HashMap::new(),
                order: VecDeque::new(),
                next_id: 0,
                running: 0,
            }),
            stop,
            database: database.into(),
            write_tx,
        })
    }

    pub fn start(
        self: &Arc<Self>,
        owner: &str,
        coll: &str,
        options: GraphAlgorithmOptions,
        store: Arc<Store>,
    ) -> Result<Value, ApiError> {
        if self.stop.load(Ordering::Relaxed) {
            return Err(ApiError {
                code: "unavailable".into(),
                message: "database is shutting down".into(),
            });
        }
        options.validate().map_err(ApiError::from)?;
        let kind = match options.kind {
            nonodb_core::graph::GraphAlgorithmKind::Pagerank => "pagerank",
            nonodb_core::graph::GraphAlgorithmKind::DegreeCentrality => "degree_centrality",
            nonodb_core::graph::GraphAlgorithmKind::ConnectedComponents => "connected_components",
            nonodb_core::graph::GraphAlgorithmKind::LabelPropagation => "label_propagation",
            nonodb_core::graph::GraphAlgorithmKind::KCore => "k_core",
            nonodb_core::graph::GraphAlgorithmKind::DegreeDistribution => "degree_distribution",
        }
        .to_string();
        let id = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| ApiError::internal("job lock"))?;
            inner.reap();
            if inner.running >= MAX_CONCURRENT_JOBS {
                return Err(ApiError {
                    code: "too_many_jobs".into(),
                    message: format!(
                        "analytics pool is full ({MAX_CONCURRENT_JOBS} concurrent jobs)"
                    ),
                });
            }
            inner.next_id = inner.next_id.saturating_add(1);
            let id = format!("job-{}-{}", self.database, inner.next_id);
            let progress = Arc::new(Mutex::new(Progress::default()));
            let cancel = Arc::new(AtomicBool::new(false));
            inner.jobs.insert(
                id.clone(),
                Job {
                    id: id.clone(),
                    owner: owner.to_string(),
                    coll: coll.to_string(),
                    kind,
                    state: JobState::Queued,
                    progress: progress.clone(),
                    cancel: cancel.clone(),
                    result: None,
                    result_bytes: 0,
                    error: None,
                    created_ms: now_ms(),
                    started_ms: None,
                    finished_ms: None,
                    handle: None,
                },
            );
            inner.order.push_back(id.clone());
            inner.running += 1;
            id
        };

        let manager = self.clone();
        let thread_id = id.clone();
        let thread_coll = coll.to_string();
        let observer = JobObserver {
            cancel: self
                .job_cancel(&id)
                .unwrap_or_else(|| Arc::new(AtomicBool::new(false))),
            stop: self.stop.clone(),
            progress: self
                .job_progress(&id)
                .unwrap_or_else(|| Arc::new(Mutex::new(Progress::default()))),
        };
        let spawned = std::thread::Builder::new()
            .name(format!("nonodb-analytics-{thread_id}"))
            .spawn(move || {
                let started = now_ms();
                manager.mark_running(&thread_id, started);
                let outcome = store
                    .collection(&thread_coll)
                    .map_err(ApiError::from)
                    .and_then(|collection| {
                        collection
                            .graph_algorithm_with_observer(options, Some(&observer))
                            .map_err(ApiError::from)
                    });
                manager.finish(&thread_id, outcome);
            });
        match spawned {
            Ok(handle) => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| ApiError::internal("job lock"))?;
                if let Some(job) = inner.jobs.get_mut(&id) {
                    job.handle = Some(handle);
                }
                Ok(json!({"job_id": id, "state": "queued"}))
            }
            Err(err) => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| ApiError::internal("job lock"))?;
                inner.jobs.remove(&id);
                inner.order.retain(|candidate| candidate != &id);
                inner.running = inner.running.saturating_sub(1);
                Err(ApiError::internal(format!(
                    "failed to start analytics job: {err}"
                )))
            }
        }
    }

    pub fn status(&self, owner: &str, id: &str) -> Result<Value, ApiError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("job lock"))?;
        let job = inner.jobs.get(id).ok_or_else(not_found)?;
        if job.owner != owner {
            return Err(ApiError::forbidden());
        }
        let progress = job
            .progress
            .lock()
            .map_err(|_| ApiError::internal("progress lock"))?
            .clone();
        Ok(json!({
            "job_id": job.id,
            "collection": job.coll,
            "state": job.state.as_str(),
            "progress": {
                "phase": progress.phase,
                "done": progress.done,
                "total": progress.total,
            },
            "created_ms": job.created_ms,
            "started_ms": job.started_ms,
            "finished_ms": job.finished_ms,
            "result_available": job.result.is_some(),
            "error": job.error,
        }))
    }

    pub fn cancel(&self, owner: &str, id: &str) -> Result<Value, ApiError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("job lock"))?;
        let job = inner.jobs.get_mut(id).ok_or_else(not_found)?;
        if job.owner != owner {
            return Err(ApiError::forbidden());
        }
        if !job.state.terminal() {
            job.cancel.store(true, Ordering::SeqCst);
            if job.state == JobState::Queued || job.state == JobState::Running {
                job.state = JobState::Cancelling;
            }
        }
        Ok(json!({
            "job_id": job.id,
            "state": job.state.as_str(),
        }))
    }

    pub fn export(&self, owner: &str, id: &str, target: &str) -> Result<Value, ApiError> {
        let (kind, result) = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| ApiError::internal("job lock"))?;
            let job = inner.jobs.get_mut(id).ok_or_else(not_found)?;
            if job.owner != owner {
                return Err(ApiError::forbidden());
            }
            if job.state != JobState::Completed {
                return Err(ApiError {
                    code: "job_not_ready".into(),
                    message: "analytics job has not completed".into(),
                });
            }
            let result = job.result.take().ok_or_else(|| ApiError {
                code: "not_found".into(),
                message: "analytics result was already exported".into(),
            })?;
            job.result_bytes = 0;
            job.state = JobState::Exporting;
            (job.kind.clone(), result)
        };
        let retained = result.clone();
        let (resp, rx) = tokio::sync::oneshot::channel();
        let sent = self.write_tx.send(WriteJob {
            req: WriteReq::ExportGraphResult {
                coll: target.to_string(),
                kind,
                result: (*result).clone(),
            },
            resp,
            isolated: true,
            durability: Durability::Full,
        });
        if sent.is_err() {
            self.restore_result(id, retained);
            return Err(ApiError::internal("writer unavailable"));
        }
        let value = match rx.blocking_recv() {
            Ok(Ok(value)) => value,
            Ok(Err(err)) => {
                self.restore_result(id, retained);
                return Err(err);
            }
            Err(_) => {
                self.restore_result(id, retained);
                return Err(ApiError::internal("writer dropped export"));
            }
        };
        if let Ok(mut inner) = self.inner.lock() {
            if let Some(job) = inner.jobs.get_mut(id) {
                job.result = None;
                job.result_bytes = 0;
                job.state = JobState::Completed;
            }
        }
        Ok(json!({
            "job_id": id,
            "collection": target,
            "exported": value["exported"].clone(),
        }))
    }

    pub fn result(&self, owner: &str, id: &str) -> Result<Value, ApiError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("job lock"))?;
        let job = inner.jobs.get(id).ok_or_else(not_found)?;
        if job.owner != owner {
            return Err(ApiError::forbidden());
        }
        match job.state {
            JobState::Completed => job
                .result
                .as_ref()
                .map(|result| (**result).clone())
                .ok_or_else(|| ApiError {
                    code: "not_found".into(),
                    message: "analytics result was already exported".into(),
                }),
            JobState::Failed | JobState::Cancelled => {
                Err(job.error.clone().unwrap_or_else(|| ApiError {
                    code: "cancelled".into(),
                    message: "analytics job was cancelled".into(),
                }))
            }
            _ => Err(ApiError {
                code: "job_not_ready".into(),
                message: "analytics job has not completed".into(),
            }),
        }
    }

    fn restore_result(&self, id: &str, result: Arc<Value>) {
        if let Ok(mut inner) = self.inner.lock() {
            if let Some(job) = inner.jobs.get_mut(id) {
                job.result_bytes = serde_json::to_vec(result.as_ref()).map_or(0, |raw| raw.len());
                job.result = Some(result);
                job.state = JobState::Completed;
            }
        }
    }

    fn job_cancel(&self, id: &str) -> Option<Arc<AtomicBool>> {
        self.inner
            .lock()
            .ok()?
            .jobs
            .get(id)
            .map(|job| job.cancel.clone())
    }

    fn job_progress(&self, id: &str) -> Option<Arc<Mutex<Progress>>> {
        self.inner
            .lock()
            .ok()?
            .jobs
            .get(id)
            .map(|job| job.progress.clone())
    }

    fn mark_running(&self, id: &str, started: u128) {
        if let Ok(mut inner) = self.inner.lock() {
            if let Some(job) = inner.jobs.get_mut(id) {
                job.state = JobState::Running;
                job.started_ms = Some(started);
            }
        }
    }

    fn finish(&self, id: &str, outcome: Result<Value, ApiError>) {
        if let Ok(mut inner) = self.inner.lock() {
            if let Some(job) = inner.jobs.get_mut(id) {
                let cancelled = job.cancel.load(Ordering::SeqCst);
                match outcome {
                    Ok(value) if !cancelled => {
                        let bytes = serde_json::to_vec(&value).map_or(usize::MAX, |raw| raw.len());
                        if bytes > MAX_JOB_RESULT_BYTES {
                            job.state = JobState::Failed;
                            job.error = Some(ApiError {
                                code: "result_too_large".into(),
                                message: format!(
                                    "analytics result is {bytes} bytes (limit {MAX_JOB_RESULT_BYTES}); lower the limit or export from Cypher"
                                ),
                            });
                        } else {
                            job.state = JobState::Completed;
                            job.result = Some(Arc::new(value));
                            job.result_bytes = bytes;
                        }
                    }
                    Ok(_) => {
                        job.state = JobState::Cancelled;
                        job.error = Some(ApiError {
                            code: "cancelled".into(),
                            message: "analytics job was cancelled".into(),
                        });
                    }
                    Err(err) if cancelled || err.message.contains("cancelled") => {
                        job.state = JobState::Cancelled;
                        job.error = Some(ApiError {
                            code: "cancelled".into(),
                            message: "analytics job was cancelled".into(),
                        });
                    }
                    Err(err) => {
                        job.state = JobState::Failed;
                        job.error = Some(err);
                    }
                }
                job.finished_ms = Some(now_ms());
                job.progress = Arc::new(Mutex::new(if job.state == JobState::Completed {
                    Progress {
                        phase: "completed".into(),
                        done: 1,
                        total: Some(1),
                    }
                } else {
                    Progress {
                        phase: job.state.as_str().into(),
                        done: 1,
                        total: Some(1),
                    }
                }));
                inner.running = inner.running.saturating_sub(1);
            }
            inner.reap();
        }
    }

    /// Cancels every running job and joins the worker threads.
    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::SeqCst);
        let handles: Vec<_> = {
            let Ok(mut inner) = self.inner.lock() else {
                return;
            };
            for job in inner.jobs.values_mut() {
                if !job.state.terminal() {
                    job.cancel.store(true, Ordering::SeqCst);
                }
            }
            inner
                .jobs
                .values_mut()
                .filter_map(|job| job.handle.take())
                .collect()
        };
        for handle in handles {
            let _ = handle.join();
        }
    }
}

impl Drop for AnalyticsManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct JobObserver {
    cancel: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    progress: Arc<Mutex<Progress>>,
}

impl GraphAlgorithmObserver for JobObserver {
    fn progress(&self, phase: &str, done: usize, total: Option<usize>) {
        if let Ok(mut progress) = self.progress.lock() {
            progress.phase = phase.to_string();
            progress.done = done;
            progress.total = total;
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst) || self.stop.load(Ordering::SeqCst)
    }
}

impl Inner {
    fn reap(&mut self) {
        loop {
            let mut terminal = self
                .jobs
                .values()
                .filter(|job| job.state.terminal())
                .map(|job| (job.created_ms, job.id.clone()))
                .collect::<Vec<_>>();
            terminal.sort();
            let retained_bytes = self
                .jobs
                .values()
                .map(|job| job.result_bytes)
                .sum::<usize>();
            if terminal.len() <= MAX_TERMINAL_JOBS && retained_bytes <= MAX_RETAINED_RESULT_BYTES {
                break;
            }
            let Some((_, id)) = terminal.first() else {
                break;
            };
            let id = id.clone();
            self.jobs.remove(&id);
            self.order.retain(|candidate| candidate != &id);
        }
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

fn not_found() -> ApiError {
    ApiError {
        code: "not_found".into(),
        message: "analytics job does not exist".into(),
    }
}
