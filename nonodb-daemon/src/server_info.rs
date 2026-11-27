//! Runtime listener information exposed to the CLI and GUI.

use std::sync::{OnceLock, RwLock};

use serde::Serialize;
use serde_json::{json, Value};

#[derive(Clone, Debug, Serialize)]
pub struct TcpListenerInfo {
    pub configured: String,
    pub bound: String,
    pub tls: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ServerInfo {
    pub version: String,
    pub tcp: Option<TcpListenerInfo>,
    pub unix: Option<String>,
    pub advertise_url: Option<String>,
}

static INFO: OnceLock<RwLock<ServerInfo>> = OnceLock::new();

fn slot() -> &'static RwLock<ServerInfo> {
    INFO.get_or_init(|| {
        RwLock::new(ServerInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            ..ServerInfo::default()
        })
    })
}

pub fn set_tcp(configured: &str, bound: &str, tls: bool) {
    let mut info = slot().write().unwrap_or_else(|p| p.into_inner());
    info.tcp = Some(TcpListenerInfo {
        configured: configured.to_string(),
        bound: bound.to_string(),
        tls,
    });
    drop(info);
    crate::instance::set_tcp(configured, bound, tls);
}

pub fn set_unix(path: &std::path::Path) {
    let mut info = slot().write().unwrap_or_else(|p| p.into_inner());
    info.unix = Some(path.display().to_string());
}

pub fn set_advertise_url(value: Option<String>) {
    let mut info = slot().write().unwrap_or_else(|p| p.into_inner());
    info.advertise_url = value.clone();
    drop(info);
    crate::instance::set_advertise_url(value);
}

pub fn snapshot() -> ServerInfo {
    slot().read().unwrap_or_else(|p| p.into_inner()).clone()
}

/// Public, versioned capability description for clients and migration tools.
pub fn capabilities() -> Value {
    json!({
        "schema_version": 1,
        "server_version": env!("CARGO_PKG_VERSION"),
        "api": {
            "http_json": true,
            "mongodb_wire": true,
            "sql": true,
            "native_clients": ["python", "node", "rust", "go"],
        },
        "features": {
            "search": {
                "search_all": true,
                "modes": ["lexical", "fuzzy", "vector", "hybrid"],
                "atlas_pipeline_subset": true,
            },
            "aggregation": {
                "convert": true,
                "conversion_aliases": [
                    "$toString", "$toInt", "$toLong", "$toDouble",
                    "$toDecimal", "$toBool", "$toDate"
                ],
                "lookup_pipeline_let": true,
            },
            "ttl": {
                "insertion_based": true,
                "date_field": true,
                "backfill": true,
                "strict_reads_default": true,
            },
            "events": {
                "collection_events": true,
                "watch": true,
                "webhooks": true,
                "webhook_thresholds": ["count", "sum", "avg"],
            },
            "graph": {
                "traverse": true,
                "shortest_path": true,
                "adjacency": true,
                "compact_adjacency": true,
                "projection": true,
                "edge_filter": true,
                "node_filter": true,
                "neighbors": true,
                "hybrid_rerank": true,
                "temporal_as_of": true,
                "edge_scope_required": true,
                "online_rebuild": true,
                "graph_lookup_fast_path": true,
                "vector_seed": true,
                "algorithms": ["bfs", "dfs", "dijkstra"],
                "directions": ["out", "in", "both"],
            },
            "storage": {
                "history": true,
                "vault": true,
                "branching": true,
                "backups": true,
                "replication": true,
                "timeseries": {
                    "profile": "timeseries",
                    "codecs": [
                        "delta", "delta_delta", "gorilla_xor", "dictionary",
                        "for_bitpack", "bool_bits", "byte_shuffle", "deflate"
                    ],
                    "granularities": ["seconds", "minutes", "hours"],
                    "compression_levels": ["fast", "balanced", "max"],
                    "aggregation": true,
                    "retention": true,
                    "point_lookup": "synthetic_id",
                },
                "columnar": {
                    "profile": "columnar",
                    "codecs": [
                        "delta", "delta_delta", "gorilla_xor", "dictionary",
                        "for_bitpack", "bool_bits", "byte_shuffle", "deflate"
                    ],
                    "compression_levels": ["fast", "balanced", "max"],
                    "bucket_by": "id_hash",
                    "operation_parity": "standard",
                    "point_lookup": "id_locator",
                    "update_overlay": true,
                    "compaction": true,
                },
            },
            "migration": {
                "mongodb_ddl": true,
                "migrate_script": true,
                "shadow_mode": true,
                "restore_to": true,
            },
        },
        "limits": {
            "document_bytes": nonodb_core::MAX_DOCUMENT_BYTES,
            "aggregate_docs": nonodb_core::MAX_AGGREGATE_DOCS,
            "search_results": 1000,
            "lookup_values": nonodb_core::MAX_LOOKUP_VALUES,
            "graph_start_ids": 10_000,
            "graph_depth": nonodb_core::graph::MAX_GRAPH_DEPTH,
            "graph_nodes": nonodb_core::graph::DEFAULT_GRAPH_NODES,
            "graph_edges": nonodb_core::graph::DEFAULT_GRAPH_EDGES,
            "graph_bytes": nonodb_core::graph::DEFAULT_GRAPH_BYTES,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_are_versioned_and_secret_free() {
        let value = capabilities();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["features"]["ttl"]["date_field"], true);
        assert_eq!(value["features"]["aggregation"]["convert"], true);
        assert_eq!(value["features"]["graph"]["vector_seed"], true);
        assert!(value.get("token").is_none());
        let text = value.to_string();
        assert!(!text.contains("token"));
        assert!(!text.contains("secret"));
    }
}
