//! Client-certificate revocation checks for mutual TLS.
//!
//! Configured CRL files/URLs are refreshed on a timer. OCSP lookups are cached
//! per certificate and fetched only when a client connects. The default when
//! revocation is configured but no source can answer is fail-closed.

use std::collections::{HashMap, HashSet};
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use ring::digest::{digest, SHA1_FOR_LEGACY_USE_ONLY, SHA256};
use x509_parser::prelude::{FromDer, X509Certificate};
use x509_parser::revocation_list::CertificateRevocationList;

#[derive(Clone, Debug)]
enum CrlSource {
    File(PathBuf),
    Url(String),
}

#[derive(Clone)]
struct RevocationConfig {
    sources: Vec<CrlSource>,
    ocsp_url: Option<String>,
    fail_closed: bool,
    refresh: Duration,
    ocsp_ttl: Duration,
    timeout: Duration,
    ca_certs: Vec<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct CrlInfo {
    issuer: Vec<u8>,
    revoked: HashSet<Vec<u8>>,
    this_update: i64,
    next_update: Option<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OcspStatus {
    Good,
    Revoked,
}

#[derive(Clone, Debug)]
struct CachedOcsp {
    status: OcspStatus,
    expires: Instant,
}

#[derive(Default)]
struct RevocationData {
    crls: Vec<CrlInfo>,
    ocsp: HashMap<Vec<u8>, CachedOcsp>,
    last_refresh: Option<Instant>,
    last_error: Option<String>,
}

pub struct RevocationState {
    config: RevocationConfig,
    data: RwLock<RevocationData>,
    http: reqwest::Client,
}

impl RevocationState {
    /// Builds the configured revocation checker. Returns `None` when no CRL or
    /// OCSP source is configured. With fail-closed enabled (the default) an
    /// initial source that cannot be loaded prevents TLS startup.
    pub fn from_env(client_ca: Option<&str>) -> Result<Option<Arc<Self>>, String> {
        let mut sources = Vec::new();
        for path in env_list("NONODB_TLS_CRL_FILE") {
            sources.push(CrlSource::File(PathBuf::from(path)));
        }
        for url in env_list("NONODB_TLS_CRL_URL") {
            sources.push(CrlSource::Url(url));
        }
        let ocsp_url = std::env::var("NONODB_TLS_OCSP_URL")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if sources.is_empty() && ocsp_url.is_none() {
            return Ok(None);
        }
        let client_ca = client_ca.ok_or(
            "NONODB_TLS_CRL_FILE/URL and NONODB_TLS_OCSP_URL require NONODB_TLS_CLIENT_CA",
        )?;
        let ca_certs = read_ca_certs(client_ca)?;
        let fail_closed = match std::env::var("NONODB_TLS_REVOCATION_FAIL_MODE")
            .unwrap_or_else(|_| "closed".into())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "closed" | "fail-closed" | "fail_closed" => true,
            "open" | "fail-open" | "fail_open" => false,
            other => {
                return Err(format!(
                    "NONODB_TLS_REVOCATION_FAIL_MODE must be open or closed, got `{other}`"
                ))
            }
        };
        let refresh = Duration::from_secs(env_u64("NONODB_TLS_REVOCATION_REFRESH_SECS", 300));
        let ocsp_ttl = Duration::from_secs(env_u64("NONODB_TLS_OCSP_CACHE_SECS", 300));
        let timeout = Duration::from_secs(env_u64("NONODB_TLS_REVOCATION_TIMEOUT_SECS", 5));
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| format!("build revocation HTTP client: {error}"))?;
        let config = RevocationConfig {
            sources,
            ocsp_url,
            fail_closed,
            refresh,
            ocsp_ttl,
            timeout,
            ca_certs,
        };
        let state = Arc::new(Self {
            config,
            data: RwLock::new(RevocationData::default()),
            http,
        });
        state.load_initial()?;
        Ok(Some(state))
    }

    pub fn refresh_interval(&self) -> Duration {
        self.config.refresh
    }

    fn load_initial(&self) -> Result<(), String> {
        let mut loaded = Vec::new();
        let mut errors = Vec::new();
        for source in &self.config.sources {
            match self.fetch_sync(source) {
                Ok(mut crls) => loaded.append(&mut crls),
                Err(error) => errors.push(error),
            }
        }
        let error = (!errors.is_empty()).then(|| errors.join("; "));
        {
            let mut data = self
                .data
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            data.crls = loaded;
            data.last_refresh = Some(Instant::now());
            data.last_error = error.clone();
        }
        if let Some(error) = error {
            crate::metrics::inc(&crate::metrics::TLS_REVOCATION_REFRESH_ERRORS);
            if self.config.fail_closed {
                return Err(format!("revocation source unavailable: {error}"));
            }
            tracing::warn!(%error, "revocation source unavailable; fail-open is configured");
        }
        crate::metrics::inc(&crate::metrics::TLS_REVOCATION_REFRESH_TOTAL);
        Ok(())
    }

    /// Refreshes CRLs and drops expired OCSP cache entries.
    pub async fn refresh(&self) {
        let mut loaded = Vec::new();
        let mut errors = Vec::new();
        for source in &self.config.sources {
            match self.fetch(source).await {
                Ok(mut crls) => loaded.append(&mut crls),
                Err(error) => errors.push(error),
            }
        }
        let error = (!errors.is_empty()).then(|| errors.join("; "));
        {
            let mut data = self
                .data
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !loaded.is_empty() || errors.len() < self.config.sources.len() {
                data.crls = loaded;
            }
            let now = Instant::now();
            data.ocsp.retain(|_, cached| cached.expires > now);
            data.last_refresh = Some(now);
            data.last_error = error.clone();
        }
        crate::metrics::inc(&crate::metrics::TLS_REVOCATION_REFRESH_TOTAL);
        if let Some(error) = error {
            crate::metrics::inc(&crate::metrics::TLS_REVOCATION_REFRESH_ERRORS);
            tracing::warn!(%error, "CRL refresh failed");
        }
    }

    async fn fetch(&self, source: &CrlSource) -> Result<Vec<CrlInfo>, String> {
        let bytes = match source {
            CrlSource::File(path) => std::fs::read(path)
                .map_err(|error| format!("read CRL {}: {error}", path.display()))?,
            CrlSource::Url(url) => {
                let response = self
                    .http
                    .get(url)
                    .send()
                    .await
                    .map_err(|error| format!("fetch CRL {url}: {error}"))?
                    .error_for_status()
                    .map_err(|error| format!("fetch CRL {url}: {error}"))?;
                let limit = 8 * 1024 * 1024;
                if response.content_length().is_some_and(|len| len > limit) {
                    return Err(format!("CRL {url} is larger than {limit} bytes"));
                }
                response
                    .bytes()
                    .await
                    .map_err(|error| format!("read CRL {url}: {error}"))?
                    .to_vec()
            }
        };
        parse_crls(&bytes, &self.config.ca_certs)
    }

    fn fetch_sync(&self, source: &CrlSource) -> Result<Vec<CrlInfo>, String> {
        let bytes = match source {
            CrlSource::File(path) => std::fs::read(path)
                .map_err(|error| format!("read CRL {}: {error}", path.display()))?,
            CrlSource::Url(url) => {
                // `reqwest::blocking` owns a runtime and must not be created or
                // dropped inside the async startup context.
                let url = url.clone();
                let label = url.clone();
                let timeout = self.config.timeout;
                std::thread::spawn(move || {
                    reqwest::blocking::Client::builder()
                        .timeout(timeout)
                        .build()
                        .map_err(|error| format!("build CRL client: {error}"))?
                        .get(&url)
                        .send()
                        .and_then(|response| response.error_for_status())
                        .map_err(|error| format!("fetch CRL {url}: {error}"))?
                        .bytes()
                        .map(|bytes| bytes.to_vec())
                        .map_err(|error| format!("read CRL {url}: {error}"))
                })
                .join()
                .map_err(|_| format!("CRL fetch thread for {label} panicked"))?
                .map_err(|error| format!("fetch CRL {label}: {error}"))?
            }
        };
        parse_crls(&bytes, &self.config.ca_certs)
    }

    /// Checks one peer certificate. Configured sources are consulted; when none
    /// can provide a definitive answer, `NONODB_TLS_REVOCATION_FAIL_MODE`
    /// decides whether the connection is allowed.
    pub async fn check_peer(&self, cert_der: &[u8]) -> Result<(), String> {
        let (_, cert) = X509Certificate::from_der(cert_der)
            .map_err(|error| format!("parse client certificate for revocation: {error}"))?;
        let issuer = cert.issuer().as_raw().to_vec();
        let serial = canonical_serial(cert.raw_serial());
        let fingerprint = digest(&SHA256, cert_der).as_ref().to_vec();
        let now = unix_now();

        let mut revoked_by = None;
        let mut good = false;
        let mut unavailable = Vec::new();
        {
            let data = self
                .data
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for crl in &data.crls {
                if crl.issuer != issuer {
                    continue;
                }
                if crl.next_update.is_some_and(|next| next < now) {
                    unavailable.push("CRL is past nextUpdate".to_string());
                    continue;
                }
                if crl.this_update > now.saturating_add(300) {
                    unavailable.push("CRL thisUpdate is in the future".to_string());
                    continue;
                }
                if crl.revoked.contains(&serial) {
                    revoked_by = Some("CRL");
                    break;
                }
                good = true;
            }
        }
        if let Some(source) = revoked_by {
            crate::metrics::inc(&crate::metrics::TLS_REVOCATION_REVOKED_TOTAL);
            return Err(format!("client certificate is revoked by {source}"));
        }

        if let Some(url) = &self.config.ocsp_url {
            let cached = {
                let data = self
                    .data
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                data.ocsp.get(&fingerprint).cloned()
            };
            if let Some(cached) = cached.filter(|cached| cached.expires > Instant::now()) {
                match cached.status {
                    OcspStatus::Good => good = true,
                    OcspStatus::Revoked => {
                        crate::metrics::inc(&crate::metrics::TLS_REVOCATION_REVOKED_TOTAL);
                        return Err("client certificate is revoked by OCSP".into());
                    }
                }
            } else {
                match self.fetch_ocsp(url, cert_der, &issuer, &serial).await {
                    Ok(status) => {
                        let expires = Instant::now() + self.config.ocsp_ttl;
                        let mut data = self
                            .data
                            .write()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        data.ocsp
                            .insert(fingerprint, CachedOcsp { status, expires });
                        match status {
                            OcspStatus::Good => good = true,
                            OcspStatus::Revoked => {
                                crate::metrics::inc(&crate::metrics::TLS_REVOCATION_REVOKED_TOTAL);
                                return Err("client certificate is revoked by OCSP".into());
                            }
                        }
                    }
                    Err(error) => unavailable.push(error),
                }
            }
        }

        if good {
            return Ok(());
        }
        if unavailable.is_empty() {
            unavailable.push("no CRL/OCSP source covers this certificate".into());
        }
        let reason = unavailable.join("; ");
        crate::metrics::inc(&crate::metrics::TLS_REVOCATION_UNAVAILABLE_TOTAL);
        if self.config.fail_closed {
            Err(format!("revocation status unavailable: {reason}"))
        } else {
            tracing::warn!(%reason, "revocation status unavailable; fail-open is configured");
            Ok(())
        }
    }

    async fn fetch_ocsp(
        &self,
        url: &str,
        cert_der: &[u8],
        issuer: &[u8],
        serial: &[u8],
    ) -> Result<OcspStatus, String> {
        let (_, issuer_cert) = self
            .config
            .ca_certs
            .iter()
            .find_map(|der| {
                let parsed = X509Certificate::from_der(der).ok()?;
                (parsed.1.subject().as_raw() == issuer).then_some(parsed)
            })
            .ok_or_else(|| "issuer certificate not found in NONODB_TLS_CLIENT_CA".to_string())?;
        let request = build_ocsp_request(&issuer_cert, serial);
        let response = self
            .http
            .post(url)
            .header("content-type", "application/ocsp-request")
            .header("accept", "application/ocsp-response")
            .body(request)
            .send()
            .await
            .map_err(|error| format!("OCSP request to {url}: {error}"))?
            .error_for_status()
            .map_err(|error| format!("OCSP response from {url}: {error}"))?;
        if response
            .content_length()
            .is_some_and(|len| len > 1024 * 1024)
        {
            return Err("OCSP response is larger than 1 MiB".into());
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|error| format!("read OCSP response: {error}"))?;
        parse_ocsp_response(&bytes, cert_der)
    }
}

fn env_list(name: &str) -> Vec<String> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn read_ca_certs(path: &str) -> Result<Vec<Vec<u8>>, String> {
    let mut reader = BufReader::new(
        std::fs::File::open(path).map_err(|error| format!("open client CA {path}: {error}"))?,
    );
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("parse client CA {path}: {error}"))?;
    if certs.is_empty() {
        return Err(format!("no CA certificates in {path}"));
    }
    Ok(certs
        .into_iter()
        .map(|cert| cert.as_ref().to_vec())
        .collect())
}

fn parse_crls(bytes: &[u8], ca_certs: &[Vec<u8>]) -> Result<Vec<CrlInfo>, String> {
    let blocks = pem_blocks(bytes, "X509 CRL");
    let candidates: Vec<Vec<u8>> = if blocks.is_empty() {
        vec![bytes.to_vec()]
    } else {
        blocks
    };
    let mut out = Vec::new();
    for der in candidates {
        let (_, crl) = CertificateRevocationList::from_der(&der)
            .map_err(|error| format!("parse CRL: {error}"))?;
        let issuer = crl.issuer().as_raw().to_vec();
        let ca = ca_certs
            .iter()
            .find_map(|cert| {
                let parsed = X509Certificate::from_der(cert).ok()?;
                (parsed.1.subject().as_raw() == issuer.as_slice()).then_some(parsed)
            })
            .ok_or_else(|| "CRL issuer is not in NONODB_TLS_CLIENT_CA".to_string())?;
        crl.verify_signature(ca.1.public_key())
            .map_err(|error| format!("verify CRL signature: {error}"))?;
        let revoked = crl
            .iter_revoked_certificates()
            .map(|entry| entry.serial().to_bytes_be())
            .collect();
        out.push(CrlInfo {
            issuer,
            revoked,
            this_update: crl.last_update().timestamp(),
            next_update: crl.next_update().map(|time| time.timestamp()),
        });
    }
    Ok(out)
}

fn pem_blocks(bytes: &[u8], label: &str) -> Vec<Vec<u8>> {
    let text = String::from_utf8_lossy(bytes);
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let mut out = Vec::new();
    let mut rest = text.as_ref();
    while let Some(start) = rest.find(&begin) {
        let after = &rest[start + begin.len()..];
        let Some(finish) = after.find(&end) else {
            break;
        };
        let payload: String = after[..finish]
            .chars()
            .filter(|ch| !ch.is_ascii_whitespace())
            .collect();
        if let Ok(der) = STANDARD.decode(payload) {
            out.push(der);
        }
        rest = &after[finish + end.len()..];
    }
    out
}

fn canonical_serial(raw: &[u8]) -> Vec<u8> {
    let mut start = 0;
    while start + 1 < raw.len() && raw[start] == 0 {
        start += 1;
    }
    let mut out = raw[start..].to_vec();
    if out.is_empty() {
        out.push(0);
    }
    out
}

fn build_ocsp_request(issuer: &X509Certificate<'_>, serial: &[u8]) -> Vec<u8> {
    let name_hash = digest(&SHA1_FOR_LEGACY_USE_ONLY, issuer.subject().as_raw());
    let key_hash = digest(
        &SHA1_FOR_LEGACY_USE_ONLY,
        &issuer.public_key().subject_public_key.data,
    );
    let sha1 = oid(&[0x2b, 0x0e, 0x03, 0x02, 0x1a]);
    let alg = sequence(&[sha1, null()]);
    let serial = integer(serial);
    let cert_id = sequence(&[
        alg,
        octet(name_hash.as_ref()),
        octet(key_hash.as_ref()),
        serial,
    ]);
    let request = sequence(&[cert_id]);
    let request_list = sequence(&[request]);
    let tbs = sequence(&[request_list]);
    sequence(&[tbs])
}

fn parse_ocsp_response(bytes: &[u8], peer_cert_der: &[u8]) -> Result<OcspStatus, String> {
    let peer_serial = {
        let (_, cert) = X509Certificate::from_der(peer_cert_der)
            .map_err(|error| format!("parse peer certificate: {error}"))?;
        canonical_serial(cert.raw_serial())
    };
    let mut outer = bytes;
    let response = expect(&mut outer, 0x30)?;
    let mut fields = response.value;
    let status = take(&mut fields)?;
    if status.tag != 0x0a {
        return Err("OCSP responseStatus is missing".into());
    }
    let status_value = status.value.first().copied().unwrap_or(255);
    if status_value != 0 {
        return Err(format!("OCSP responder returned status {status_value}"));
    }
    let response_bytes = take(&mut fields)?;
    if response_bytes.tag != 0xa0 {
        return Err("OCSP responseBytes are missing".into());
    }
    let mut rb = response_bytes.value;
    let rb_seq = expect(&mut rb, 0x30)?;
    let mut rb_fields = rb_seq.value;
    let kind = take(&mut rb_fields)?;
    if kind.tag != 0x06 || kind.value != [0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x30, 0x01, 0x01] {
        return Err("OCSP response is not id-pkix-ocsp-basic".into());
    }
    let basic = expect(&mut rb_fields, 0x04)?;
    let mut basic_data = basic.value;
    let basic_seq = expect(&mut basic_data, 0x30)?;
    let mut basic_fields = basic_seq.value;
    let tbs = expect(&mut basic_fields, 0x30)?;
    let mut data = tbs.value;
    // version [0] EXPLICIT, responderID [1]/[2], producedAt
    if data.first().is_some_and(|tag| *tag == 0xa0) {
        take(&mut data)?;
    }
    let responder = take(&mut data)?;
    if !matches!(responder.tag, 0xa1 | 0xa2) {
        return Err("OCSP responderID is malformed".into());
    }
    let produced_at = take(&mut data)?;
    if produced_at.tag != 0x18 {
        return Err("OCSP producedAt is malformed".into());
    }
    let responses = expect(&mut data, 0x30)?;
    let mut remaining = responses.value;
    while !remaining.is_empty() {
        let single = expect(&mut remaining, 0x30)?;
        let mut single_fields = single.value;
        let cert_id = expect(&mut single_fields, 0x30)?;
        let mut cert_fields = cert_id.value;
        let _alg = expect(&mut cert_fields, 0x30)?;
        let _issuer_name = expect(&mut cert_fields, 0x04)?;
        let _issuer_key = expect(&mut cert_fields, 0x04)?;
        let serial = expect(&mut cert_fields, 0x02)?;
        let status = take(&mut single_fields)?;
        if canonical_serial(serial.value) != peer_serial {
            continue;
        }
        return match status.tag {
            0x80 => Ok(OcspStatus::Good),
            0x81 => Ok(OcspStatus::Revoked),
            0x82 => Err("OCSP responder does not know this certificate".into()),
            other => Err(format!("unexpected OCSP certificate status tag {other:#x}")),
        };
    }
    Err("OCSP response does not contain this certificate".into())
}

struct Tlv<'a> {
    tag: u8,
    value: &'a [u8],
}

fn take<'a>(data: &mut &'a [u8]) -> Result<Tlv<'a>, String> {
    let Some((&tag, rest)) = data.split_first() else {
        return Err("truncated DER".into());
    };
    let Some((&first, rest)) = rest.split_first() else {
        return Err("truncated DER length".into());
    };
    let (length, rest) = if first & 0x80 == 0 {
        (first as usize, rest)
    } else {
        let count = (first & 0x7f) as usize;
        if count == 0 || count > 4 || rest.len() < count {
            return Err("unsupported DER length".into());
        }
        let mut length = 0usize;
        for byte in &rest[..count] {
            length = (length << 8) | *byte as usize;
        }
        (length, &rest[count..])
    };
    if rest.len() < length {
        return Err("truncated DER value".into());
    }
    let (value, rest) = rest.split_at(length);
    *data = rest;
    Ok(Tlv { tag, value })
}

fn expect<'a>(data: &mut &'a [u8], tag: u8) -> Result<Tlv<'a>, String> {
    let item = take(data)?;
    if item.tag != tag {
        return Err(format!("expected DER tag {tag:#x}, got {:#x}", item.tag));
    }
    Ok(item)
}

fn sequence(items: &[Vec<u8>]) -> Vec<u8> {
    tlv(0x30, &items.concat())
}

fn oid(value: &[u8]) -> Vec<u8> {
    tlv(0x06, value)
}

fn octet(value: &[u8]) -> Vec<u8> {
    tlv(0x04, value)
}

fn null() -> Vec<u8> {
    vec![0x05, 0x00]
}

fn integer(value: &[u8]) -> Vec<u8> {
    let mut bytes = canonical_serial(value);
    if bytes[0] & 0x80 != 0 {
        bytes.insert(0, 0);
    }
    tlv(0x02, &bytes)
}

fn tlv(tag: u8, value: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = value.len();
    if len < 0x80 {
        out.push(len as u8);
    } else {
        let bytes = len.to_be_bytes();
        let first = bytes
            .iter()
            .position(|byte| *byte != 0)
            .unwrap_or(bytes.len() - 1);
        let count = bytes.len() - first;
        out.push(0x80 | count as u8);
        out.extend_from_slice(&bytes[first..]);
    }
    out.extend_from_slice(value);
    out
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}
