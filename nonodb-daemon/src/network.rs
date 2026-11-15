//! Network listener and host-firewall management.
//!
//! Firewall changes are deliberately limited to a dedicated nftables table for
//! NonoDB's own TCP ports. The daemon never edits the host's SSH or general
//! input policy. Applying rules is opt-in with `NONODB_FIREWALL_MANAGE=1` and
//! requires root (or `NONODB_FIREWALL_SUDO=1` with passwordless sudo).

use std::io::Write;
use std::process::{Command, Stdio};

use ipnet::IpNet;
use serde::{Deserialize, Serialize};

use crate::node_state;
use crate::server_info;

const CONFIG_KEY: &str = "network:firewall";
const TABLE: &str = "inet nonodb_firewall";
const MAX_ALLOWED: usize = 128;
const MAX_PORTS: usize = 64;
const MAX_OUTPUT: usize = 16 * 1024;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FirewallConfig {
    #[serde(default)]
    pub allowed: Vec<String>,
    #[serde(default)]
    pub ports: Vec<u16>,
}

#[derive(Clone, Debug, Serialize)]
pub struct FirewallState {
    pub available: bool,
    pub backend: &'static str,
    pub apply_enabled: bool,
    pub managed: bool,
    pub status: Option<String>,
    pub error: Option<String>,
    pub table: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct NetworkView {
    pub listeners: server_info::ServerInfo,
    pub firewall: FirewallState,
    pub allowed: Vec<String>,
    pub ports: Vec<u16>,
    pub preview: String,
}

fn enabled_var(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

pub fn apply_enabled() -> bool {
    enabled_var("NONODB_FIREWALL_MANAGE")
}

fn sudo_enabled() -> bool {
    enabled_var("NONODB_FIREWALL_SUDO")
}

fn nft_command() -> Command {
    if sudo_enabled() {
        let mut command = Command::new("sudo");
        command.arg("-n").arg("nft");
        command
    } else {
        Command::new("nft")
    }
}

fn run_nft(args: &[&str]) -> Result<String, String> {
    let output = nft_command()
        .args(args)
        .output()
        .map_err(|error| format!("run nft: {error}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(if stderr.is_empty() {
            format!("nft exited with {}", output.status)
        } else {
            stderr
        });
    }
    let mut stdout = String::from_utf8_lossy(&output.stdout).to_string();
    if stdout.len() > MAX_OUTPUT {
        stdout.truncate(MAX_OUTPUT);
        stdout.push_str("\n... output truncated ...\n");
    }
    Ok(stdout)
}

fn run_nft_stdin(args: &[&str], script: &str) -> Result<String, String> {
    let mut child = nft_command()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("run nft: {error}"))?;
    child
        .stdin
        .as_mut()
        .ok_or("nft stdin unavailable")?
        .write_all(script.as_bytes())
        .map_err(|error| format!("write nft rules: {error}"))?;
    let output = child
        .wait_with_output()
        .map_err(|error| format!("wait for nft: {error}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(if stderr.is_empty() {
            format!("nft exited with {}", output.status)
        } else {
            stderr
        });
    }
    let mut stdout = String::from_utf8_lossy(&output.stdout).to_string();
    if stdout.len() > MAX_OUTPUT {
        stdout.truncate(MAX_OUTPUT);
        stdout.push_str("\n... output truncated ...\n");
    }
    Ok(stdout)
}

/// Normalizes `10.0.0.0/8` and bare addresses (`10.0.0.5` -> `/32`).
fn parse_net(value: &str) -> Result<IpNet, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("empty address".into());
    }
    if value.contains('/') {
        value
            .parse::<IpNet>()
            .map_err(|error| format!("invalid CIDR `{value}`: {error}"))
    } else {
        let ip = value
            .parse::<std::net::IpAddr>()
            .map_err(|error| format!("invalid IP address `{value}`: {error}"))?;
        Ok(match ip {
            std::net::IpAddr::V4(ip) => IpNet::new(ip.into(), 32),
            std::net::IpAddr::V6(ip) => IpNet::new(ip.into(), 128),
        }
        .map_err(|error| format!("invalid address `{value}`: {error}"))?)
    }
}

pub fn validate(mut config: FirewallConfig) -> Result<FirewallConfig, String> {
    if config.allowed.len() > MAX_ALLOWED {
        return Err(format!(
            "at most {MAX_ALLOWED} allowed addresses are supported"
        ));
    }
    let mut nets = Vec::new();
    for value in &config.allowed {
        let net = parse_net(value)?;
        if !nets.contains(&net) {
            nets.push(net);
        }
    }
    nets.sort();
    config.allowed = nets.iter().map(ToString::to_string).collect();
    config.ports.sort_unstable();
    config.ports.dedup();
    if config.ports.len() > MAX_PORTS {
        return Err(format!("at most {MAX_PORTS} ports are supported"));
    }
    if config.ports.contains(&0) {
        return Err("port 0 is not valid".into());
    }
    Ok(config)
}

pub fn load() -> FirewallConfig {
    node_state::get_json::<FirewallConfig>(CONFIG_KEY)
        .ok()
        .flatten()
        .unwrap_or_default()
}

pub fn save(config: &FirewallConfig) -> Result<(), String> {
    node_state::put_json(CONFIG_KEY, config)
}

pub fn default_ports() -> Vec<u16> {
    server_info::snapshot()
        .tcp
        .and_then(|tcp| {
            tcp.bound
                .rsplit_once(':')
                .and_then(|(_, port)| port.parse().ok())
        })
        .map(|port| vec![port])
        .unwrap_or_else(|| vec![7878])
}

pub fn preview(config: &FirewallConfig) -> String {
    let mut ipv4 = Vec::new();
    let mut ipv6 = Vec::new();
    for value in &config.allowed {
        if let Ok(net) = value.parse::<IpNet>() {
            match net {
                IpNet::V4(net) => ipv4.push(net.to_string()),
                IpNet::V6(net) => ipv6.push(net.to_string()),
            }
        }
    }
    let ports = if config.ports.is_empty() {
        default_ports()
    } else {
        config.ports.clone()
    };
    let ports = ports
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let mut out = String::new();
    out.push_str(&format!("table {TABLE} {{\n"));
    out.push_str("  set allowed4 { type ipv4_addr; flags interval;");
    if ipv4.is_empty() {
        out.push_str(" }\n");
    } else {
        out.push_str(&format!(" elements = {{ {} }} }}\n", ipv4.join(", ")));
    }
    out.push_str("  set allowed6 { type ipv6_addr; flags interval;");
    if ipv6.is_empty() {
        out.push_str(" }\n");
    } else {
        out.push_str(&format!(" elements = {{ {} }} }}\n", ipv6.join(", ")));
    }
    out.push_str("  chain input {\n");
    out.push_str("    type filter hook input priority filter; policy accept;\n");
    out.push_str("    ct state established,related accept\n");
    out.push_str("    iif \"lo\" accept\n");
    out.push_str(&format!(
        "    ip saddr @allowed4 tcp dport {{ {ports} }} accept\n"
    ));
    out.push_str(&format!(
        "    ip6 saddr @allowed6 tcp dport {{ {ports} }} accept\n"
    ));
    out.push_str(&format!("    tcp dport {{ {ports} }} drop\n"));
    out.push_str("  }\n");
    out.push_str("}\n");
    out
}

pub fn status() -> FirewallState {
    let available = Command::new("nft")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success());
    let mut state = FirewallState {
        available,
        backend: "nftables",
        apply_enabled: apply_enabled(),
        managed: false,
        status: None,
        error: None,
        table: TABLE,
    };
    if !available {
        state.error = Some("nft is not installed or not in PATH".into());
        return state;
    }
    match run_nft(&["list", "table", TABLE]) {
        Ok(output) => {
            state.managed = true;
            state.status = Some(output);
        }
        Err(error) => {
            if error.contains("Operation not permitted") || error.contains("Permission denied") {
                state.error = Some(
                    "nft permission denied; run the daemon as root or set NONODB_FIREWALL_SUDO=1 with passwordless sudo"
                        .into(),
                );
            } else if !error.contains("No such file or directory")
                && !error.contains("does not exist")
                && !error.contains("No such file")
            {
                state.error = Some(error);
            }
        }
    }
    state
}

pub fn apply(config: &FirewallConfig) -> Result<(), String> {
    if !apply_enabled() {
        return Err(
            "firewall apply is disabled; set NONODB_FIREWALL_MANAGE=1 on the daemon".into(),
        );
    }
    let script = preview(config);
    run_nft_stdin(&["--check", "-f", "-"], &script)
        .map_err(|error| format!("nft validation failed: {error}"))?;
    let _ = run_nft(&["delete", "table", TABLE]);
    run_nft_stdin(&["-f", "-"], &script).map_err(|error| format!("nft apply failed: {error}"))?;
    Ok(())
}

pub fn disable() -> Result<(), String> {
    match run_nft(&["delete", "table", TABLE]) {
        Ok(_) => Ok(()),
        Err(error)
            if error.contains("No such file or directory")
                || error.contains("does not exist")
                || error.contains("No such file") =>
        {
            Ok(())
        }
        Err(error) => Err(format!("nft delete failed: {error}")),
    }
}

pub fn view() -> NetworkView {
    let stored = load();
    let ports = if stored.ports.is_empty() {
        default_ports()
    } else {
        stored.ports.clone()
    };
    let config = FirewallConfig {
        allowed: stored.allowed,
        ports,
    };
    NetworkView {
        listeners: server_info::snapshot(),
        firewall: status(),
        preview: preview(&config),
        allowed: config.allowed,
        ports: config.ports,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_and_normalizes_addresses() {
        let config = validate(FirewallConfig {
            allowed: vec!["10.0.0.5".into(), "10.0.0.0/8".into(), "10.0.0.5".into()],
            ports: vec![7902, 7902, 7903],
        })
        .unwrap();
        assert_eq!(config.allowed, vec!["10.0.0.0/8", "10.0.0.5/32"]);
        assert_eq!(config.ports, vec![7902, 7903]);
    }

    #[test]
    fn rejects_bad_cidr() {
        assert!(validate(FirewallConfig {
            allowed: vec!["not-an-ip".into()],
            ports: vec![7902],
        })
        .is_err());
    }
}
