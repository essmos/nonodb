//! Per-IP failed authentication lockout.
//!
//! The guard is intentionally process-local: nonodb is a single-host daemon, so
//! an in-memory sliding window avoids extra dependencies and cannot be reset by
//! a database restore.

use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::metrics;

const DEFAULT_MAX_FAILURES: u64 = 10;
const DEFAULT_FAILURE_WINDOW_SECS: u64 = 60;
const DEFAULT_BLOCK_SECS: u64 = 300;
const MAX_TRACKED_CLIENTS: usize = 16_384;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockInfo {
    pub failures: u32,
    pub retry_after: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FailureInfo {
    pub failures: u32,
    pub blocked_for: Option<Duration>,
}

struct Entry {
    failures: VecDeque<Instant>,
    attempts: u32,
    blocked_until: Option<Instant>,
}

impl Entry {
    fn new() -> Self {
        Self {
            failures: VecDeque::new(),
            attempts: 0,
            blocked_until: None,
        }
    }
}

pub struct LoginGuard {
    max_failures: u64,
    window: Duration,
    block_for: Duration,
    entries: Mutex<HashMap<IpAddr, Entry>>,
}

impl LoginGuard {
    pub fn from_env() -> Result<Self, String> {
        let max_failures = env_u64("NONODB_AUTH_MAX_FAILURES", DEFAULT_MAX_FAILURES)?;
        let window_secs = env_u64("NONODB_AUTH_FAILURE_WINDOW", DEFAULT_FAILURE_WINDOW_SECS)?;
        let block_secs = env_u64("NONODB_AUTH_BLOCK_SECS", DEFAULT_BLOCK_SECS)?;
        if window_secs == 0 {
            return Err("NONODB_AUTH_FAILURE_WINDOW must be greater than zero".into());
        }
        let guard = Self {
            max_failures,
            window: Duration::from_secs(window_secs),
            block_for: Duration::from_secs(block_secs),
            entries: Mutex::new(HashMap::new()),
        };
        metrics::set(&metrics::AUTH_BLOCKED_CLIENTS, 0);
        Ok(guard)
    }

    #[cfg(test)]
    fn new(max_failures: u64, window: Duration, block_for: Duration) -> Self {
        Self {
            max_failures,
            window,
            block_for,
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub fn enabled(&self) -> bool {
        self.max_failures > 0 && !self.block_for.is_zero()
    }

    pub fn block_info(&self, ip: IpAddr, now: Instant) -> Option<BlockInfo> {
        if !self.enabled() {
            return None;
        }
        let mut entries = self.entries.lock().expect("login guard lock");
        let entry = entries.get_mut(&ip)?;
        match entry.blocked_until {
            Some(until) if until > now => Some(BlockInfo {
                failures: entry.attempts,
                retry_after: until.saturating_duration_since(now),
            }),
            Some(_) => {
                entries.remove(&ip);
                metrics::set(&metrics::AUTH_BLOCKED_CLIENTS, blocked_count(&entries, now));
                None
            }
            None => None,
        }
    }

    pub fn record_failure(&self, ip: IpAddr, now: Instant) -> FailureInfo {
        if !self.enabled() {
            return FailureInfo {
                failures: 0,
                blocked_for: None,
            };
        }
        let mut entries = self.entries.lock().expect("login guard lock");
        if entries.len() >= MAX_TRACKED_CLIENTS {
            entries.retain(|_, entry| {
                entry.blocked_until.is_some_and(|until| until > now)
                    || entry
                        .failures
                        .back()
                        .is_some_and(|last| now.duration_since(*last) < self.window)
            });
        }
        let entry = entries.entry(ip).or_insert_with(Entry::new);
        if let Some(until) = entry.blocked_until {
            if until > now {
                return FailureInfo {
                    failures: entry.attempts,
                    blocked_for: Some(until.saturating_duration_since(now)),
                };
            }
            entry.blocked_until = None;
            entry.failures.clear();
            entry.attempts = 0;
        }

        while let Some(failure) = entry.failures.front() {
            if now.duration_since(*failure) >= self.window {
                entry.failures.pop_front();
            } else {
                break;
            }
        }
        entry.failures.push_back(now);
        entry.attempts = entry.failures.len() as u32;
        metrics::inc(&metrics::AUTH_FAILURES);

        if entry.attempts as u64 >= self.max_failures {
            let failures = entry.attempts;
            entry.blocked_until = Some(now + self.block_for);
            metrics::inc(&metrics::AUTH_BLOCKS);
            metrics::set(&metrics::AUTH_BLOCKED_CLIENTS, blocked_count(&entries, now));
            return FailureInfo {
                failures,
                blocked_for: Some(self.block_for),
            };
        }
        FailureInfo {
            failures: entry.attempts,
            blocked_for: None,
        }
    }

    pub fn blocked_count(&self) -> usize {
        let now = Instant::now();
        let mut entries = self.entries.lock().expect("login guard lock");
        entries.retain(|_, entry| {
            entry.blocked_until.is_some_and(|until| until > now)
                || entry
                    .failures
                    .back()
                    .is_some_and(|last| now.duration_since(*last) < self.window)
        });
        let count = blocked_count(&entries, now);
        metrics::set(&metrics::AUTH_BLOCKED_CLIENTS, count);
        count as usize
    }
}

fn blocked_count(entries: &HashMap<IpAddr, Entry>, now: Instant) -> u64 {
    entries
        .values()
        .filter(|entry| entry.blocked_until.is_some_and(|until| until > now))
        .count() as u64
}

fn env_u64(name: &str, default: u64) -> Result<u64, String> {
    match std::env::var(name) {
        Ok(raw) if !raw.trim().is_empty() => raw
            .trim()
            .parse()
            .map_err(|e| format!("invalid {name}: {e}")),
        _ => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn ip() -> IpAddr {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    }

    #[test]
    fn blocks_after_threshold_and_expires() {
        let guard = LoginGuard::new(3, Duration::from_secs(60), Duration::from_millis(30));
        let now = Instant::now();
        assert_eq!(
            guard.record_failure(ip(), now),
            FailureInfo {
                failures: 1,
                blocked_for: None
            }
        );
        assert_eq!(
            guard.record_failure(ip(), now),
            FailureInfo {
                failures: 2,
                blocked_for: None
            }
        );
        let blocked = guard.record_failure(ip(), now);
        assert_eq!(blocked.failures, 3);
        assert_eq!(blocked.blocked_for, Some(Duration::from_millis(30)));
        assert!(guard.block_info(ip(), now).is_some());
        std::thread::sleep(Duration::from_millis(40));
        assert!(guard.block_info(ip(), Instant::now()).is_none());
        assert_eq!(guard.blocked_count(), 0);
    }

    #[test]
    fn failures_age_out_of_the_window() {
        let guard = LoginGuard::new(2, Duration::from_millis(20), Duration::from_secs(30));
        let now = Instant::now();
        guard.record_failure(ip(), now);
        assert_eq!(
            guard.record_failure(ip(), now + Duration::from_millis(30)),
            FailureInfo {
                failures: 1,
                blocked_for: None
            }
        );
    }

    #[test]
    fn disabled_guard_never_blocks() {
        let guard = LoginGuard::new(0, Duration::from_secs(60), Duration::from_secs(30));
        assert!(!guard.enabled());
        assert_eq!(
            guard.record_failure(ip(), Instant::now()),
            FailureInfo {
                failures: 0,
                blocked_for: None
            }
        );
        assert!(guard.block_info(ip(), Instant::now()).is_none());
    }
}
