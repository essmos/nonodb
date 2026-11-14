//! Short-lived replica pairing codes.
//!
//! A code is a one-time bootstrap credential. It never grants data-plane
//! access by itself; the pair endpoint exchanges it for a scoped cluster
//! credential and returns the source configuration. Codes are short-lived,
//! single-use, and rate-limited per peer.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use ring::rand::{SecureRandom, SystemRandom};
use serde::Serialize;

const CODE_LEN: usize = 8;
const CODE_TTL_SECS: u64 = 600;
const MAX_FAILURES: u32 = 5;
const FAILURE_WINDOW_SECS: u64 = 600;
const ALPHABET: &[u8] = b"23456789ABCDEFGHJKLMNPQRSTUVWXYZ";

#[derive(Clone, Debug, Serialize)]
pub struct PairingView {
    pub code: String,
    pub name: String,
    pub databases: Vec<String>,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug)]
pub struct Pending {
    code: String,
    name: String,
    databases: Vec<String>,
    expires_at_ms: u64,
}

#[derive(Default)]
struct FailureState {
    count: u32,
    reset_at_ms: u64,
}

pub struct PairingManager {
    pending: Mutex<HashMap<String, Pending>>,
    failures: Mutex<HashMap<String, FailureState>>,
}

impl PairingManager {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            failures: Mutex::new(HashMap::new()),
        }
    }

    pub fn create(&self, name: &str, databases: Vec<String>) -> Result<PairingView, String> {
        let name = name.trim();
        if name.is_empty() || name.len() > 64 {
            return Err("pairing name must be 1..=64 characters".into());
        }
        if !name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
        {
            return Err("pairing name may contain only letters, digits, '-' and '_'".into());
        }
        let code = generate_code()?;
        let expires_at_ms = now_ms() + CODE_TTL_SECS * 1000;
        let view = PairingView {
            code: format_code(&code),
            name: name.to_string(),
            databases: databases.clone(),
            expires_at_ms,
        };
        let mut pending = self.pending.lock().unwrap_or_else(|p| p.into_inner());
        pending.retain(|_, item| item.expires_at_ms > now_ms());
        pending.insert(
            code.clone(),
            Pending {
                code,
                name: name.to_string(),
                databases,
                expires_at_ms,
            },
        );
        Ok(view)
    }

    pub fn list(&self) -> Vec<PairingView> {
        let mut pending = self.pending.lock().unwrap_or_else(|p| p.into_inner());
        pending.retain(|_, item| item.expires_at_ms > now_ms());
        let mut out: Vec<_> = pending
            .values()
            .map(|item| PairingView {
                code: format_code(&item.code),
                name: item.name.clone(),
                databases: item.databases.clone(),
                expires_at_ms: item.expires_at_ms,
            })
            .collect();
        out.sort_by_key(|item| item.expires_at_ms);
        out
    }

    pub fn revoke(&self, code: &str) -> bool {
        let code = normalize_code(code);
        self.pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&code)
            .is_some()
    }

    /// Redeems a one-time code. A wrong code counts against the peer's failure
    /// budget; a correct code is removed before any credential is issued.
    pub fn redeem(&self, code: &str, peer: &str) -> Result<Pending, String> {
        let now = now_ms();
        {
            let mut failures = self.failures.lock().unwrap_or_else(|p| p.into_inner());
            failures.retain(|_, state| state.reset_at_ms > now);
            if let Some(state) = failures.get(peer) {
                if state.count >= MAX_FAILURES && state.reset_at_ms > now {
                    return Err("too many pairing attempts; retry later".into());
                }
            }
        }
        let normalized = normalize_code(code);
        let mut pending = self.pending.lock().unwrap_or_else(|p| p.into_inner());
        pending.retain(|_, item| item.expires_at_ms > now);
        let Some(item) = pending.remove(&normalized) else {
            let mut failures = self.failures.lock().unwrap_or_else(|p| p.into_inner());
            let state = failures.entry(peer.to_string()).or_default();
            if state.reset_at_ms <= now {
                state.count = 0;
                state.reset_at_ms = now + FAILURE_WINDOW_SECS * 1000;
            }
            state.count = state.count.saturating_add(1);
            return Err("invalid or expired pairing code".into());
        };
        {
            let mut failures = self.failures.lock().unwrap_or_else(|p| p.into_inner());
            failures.remove(peer);
        }
        Ok(item)
    }
}

impl Pending {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn databases(&self) -> &[String] {
        &self.databases
    }
}

fn generate_code() -> Result<String, String> {
    let rng = SystemRandom::new();
    let mut bytes = [0u8; CODE_LEN];
    rng.fill(&mut bytes)
        .map_err(|_| "generate pairing code: entropy unavailable".to_string())?;
    Ok(bytes
        .iter()
        .map(|byte| ALPHABET[*byte as usize % ALPHABET.len()] as char)
        .collect())
}

fn format_code(code: &str) -> String {
    if code.len() == CODE_LEN {
        format!("{}-{}", &code[..4], &code[4..])
    } else {
        code.to_string()
    }
}

fn normalize_code(code: &str) -> String {
    code.chars()
        .filter(|ch| !ch.is_whitespace() && *ch != '-')
        .flat_map(char::to_uppercase)
        .collect()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_one_time_and_formatted() {
        let manager = PairingManager::new();
        let view = manager.create("replica-1", vec!["default".into()]).unwrap();
        assert_eq!(view.code.len(), 9);
        assert!(view.code.contains('-'));
        let pending = manager.redeem(&view.code, "127.0.0.1").unwrap();
        assert_eq!(pending.name(), "replica-1");
        assert!(manager.redeem(&view.code, "127.0.0.1").is_err());
    }
}
