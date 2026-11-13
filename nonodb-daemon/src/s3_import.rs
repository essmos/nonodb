//! Minimal S3/R2 reader used by the backup import wizard (ListObjectsV2 + GET).

use std::path::Path;

use futures_util::StreamExt;
use ring::hmac;
use serde::Deserialize;
use tokio::io::AsyncWriteExt;

#[derive(Clone, Debug, Deserialize)]
pub struct S3Source {
    pub endpoint: String,
    pub bucket: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default = "default_access_env")]
    pub access_key_env: String,
    #[serde(default = "default_secret_env")]
    pub secret_key_env: String,
    #[serde(default)]
    pub session_token_env: Option<String>,
}

fn default_region() -> String {
    "auto".into()
}
fn default_access_env() -> String {
    "AWS_ACCESS_KEY_ID".into()
}
fn default_secret_env() -> String {
    "AWS_SECRET_ACCESS_KEY".into()
}

pub struct S3Reader {
    source: S3Source,
    access: String,
    secret: String,
    session: Option<String>,
    scheme: String,
    host: String,
    base: String,
}

impl S3Reader {
    pub fn new(source: S3Source) -> Result<Self, String> {
        let endpoint = reqwest::Url::parse(&source.endpoint)
            .map_err(|e| format!("invalid S3 endpoint: {e}"))?;
        let scheme = endpoint.scheme().to_string();
        let host = match (endpoint.host_str(), endpoint.port()) {
            (Some(host), Some(port)) => format!("{host}:{port}"),
            (Some(host), None) => host.to_string(),
            _ => return Err("S3 endpoint has no host".into()),
        };
        let base = endpoint.path().trim_end_matches('/').to_string();
        let access = std::env::var(&source.access_key_env)
            .map_err(|_| format!("missing S3 credential env `{}`", source.access_key_env))?;
        let secret = std::env::var(&source.secret_key_env)
            .map_err(|_| format!("missing S3 credential env `{}`", source.secret_key_env))?;
        let session = source
            .session_token_env
            .as_ref()
            .and_then(|name| std::env::var(name).ok());
        Ok(Self {
            source,
            access,
            secret,
            session,
            scheme,
            host,
            base,
        })
    }

    fn sign(
        &self,
        method: &str,
        canonical_uri: &str,
        canonical_query: &str,
        payload_hash: &str,
    ) -> (String, String, String) {
        let now = time::OffsetDateTime::now_utc();
        let amz_date = format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            now.year(),
            u8::from(now.month()),
            now.day(),
            now.hour(),
            now.minute(),
            now.second()
        );
        let date = amz_date[..8].to_string();
        let mut headers = vec![
            ("host".to_string(), self.host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        if let Some(token) = &self.session {
            headers.push(("x-amz-security-token".to_string(), token.clone()));
        }
        headers.sort_by(|a, b| a.0.cmp(&b.0));
        let canonical_headers = headers
            .iter()
            .map(|(key, value)| format!("{key}:{value}\n"))
            .collect::<String>();
        let signed_headers = headers
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );
        let scope = format!("{date}/{}/s3/aws4_request", self.source.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );
        let k_date = hmac_sha256(format!("AWS4{}", self.secret).as_bytes(), date.as_bytes());
        let k_region = hmac_sha256(&k_date, self.source.region.as_bytes());
        let k_service = hmac_sha256(&k_region, b"s3");
        let k_signing = hmac_sha256(&k_service, b"aws4_request");
        let signature = hex(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access
        );
        (amz_date, authorization, payload_hash.to_string())
    }

    fn request(
        &self,
        method: reqwest::Method,
        canonical_uri: &str,
        canonical_query: &str,
        url: &str,
    ) -> reqwest::RequestBuilder {
        let payload_hash = empty_sha256();
        let (amz_date, authorization, payload_hash) = self.sign(
            method.as_str(),
            canonical_uri,
            canonical_query,
            &payload_hash,
        );
        let mut request = reqwest::Client::new()
            .request(method, url)
            .header("host", &self.host)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("authorization", authorization);
        if let Some(token) = &self.session {
            request = request.header("x-amz-security-token", token);
        }
        request
    }

    pub async fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        let mut keys = Vec::new();
        let mut continuation: Option<String> = None;
        for _ in 0..50 {
            let mut params = vec![
                ("list-type".to_string(), "2".to_string()),
                ("max-keys".to_string(), "1000".to_string()),
                ("prefix".to_string(), prefix.to_string()),
            ];
            if let Some(token) = &continuation {
                params.push(("continuation-token".to_string(), token.clone()));
            }
            params.sort_by(|a, b| a.0.cmp(&b.0));
            let canonical_query = params
                .iter()
                .map(|(key, value)| {
                    format!("{}={}", uri_encode(key, true), uri_encode(value, true))
                })
                .collect::<Vec<_>>()
                .join("&");
            let canonical_uri = format!("{}/{}", self.base, self.source.bucket);
            let url = format!(
                "{}://{}{}?{}",
                self.scheme, self.host, canonical_uri, canonical_query
            );
            let response = self
                .request(reqwest::Method::GET, &canonical_uri, &canonical_query, &url)
                .send()
                .await
                .map_err(|e| format!("S3 list request: {e}"))?;
            let status = response.status();
            let body = response
                .text()
                .await
                .map_err(|e| format!("S3 list response: {e}"))?;
            if !status.is_success() {
                return Err(format!("S3 list failed: HTTP {status}: {body}"));
            }
            keys.extend(
                xml_values(&body, "Key")
                    .into_iter()
                    .map(|v| xml_unescape(&v)),
            );
            if !body.contains("<IsTruncated>true</IsTruncated>") {
                return Ok(keys);
            }
            continuation = xml_values(&body, "NextContinuationToken")
                .into_iter()
                .next()
                .map(|value| xml_unescape(&value));
            if continuation.is_none() {
                return Ok(keys);
            }
        }
        Err("S3 listing exceeded 50 pages; narrow the prefix".into())
    }

    pub async fn get_text(&self, key: &str) -> Result<String, String> {
        let canonical_uri = format!(
            "{}/{}/{}",
            self.base,
            self.source.bucket,
            uri_encode(key, false)
        );
        let url = format!("{}://{}{}", self.scheme, self.host, canonical_uri);
        let response = self
            .request(reqwest::Method::GET, &canonical_uri, "", &url)
            .send()
            .await
            .map_err(|e| format!("S3 GET request: {e}"))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| format!("S3 GET response: {e}"))?;
        if !status.is_success() {
            return Err(format!("S3 GET failed: HTTP {status}: {body}"));
        }
        Ok(body)
    }

    pub async fn get_to_file(&self, key: &str, output: &Path) -> Result<(), String> {
        let canonical_uri = format!(
            "{}/{}/{}",
            self.base,
            self.source.bucket,
            uri_encode(key, false)
        );
        let url = format!("{}://{}{}", self.scheme, self.host, canonical_uri);
        let response = self
            .request(reqwest::Method::GET, &canonical_uri, "", &url)
            .send()
            .await
            .map_err(|e| format!("S3 GET request: {e}"))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(format!("S3 GET failed: HTTP {status}: {body}"));
        }
        let mut file = tokio::fs::File::create(output)
            .await
            .map_err(|e| format!("create {}: {e}", output.display()))?;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| format!("S3 download: {e}"))?;
            file.write_all(&chunk)
                .await
                .map_err(|e| format!("write {}: {e}", output.display()))?;
        }
        file.flush()
            .await
            .map_err(|e| format!("flush {}: {e}", output.display()))?;
        file.sync_all()
            .await
            .map_err(|e| format!("sync {}: {e}", output.display()))?;
        Ok(())
    }
}

fn empty_sha256() -> String {
    sha256_hex(b"")
}

fn xml_values(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let value_start = start + open.len();
        let Some(end) = rest[value_start..].find(&close) else {
            break;
        };
        out.push(rest[value_start..value_start + end].to_string());
        rest = &rest[value_start + end + close.len()..];
    }
    out
}

fn xml_unescape(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn sha256_hex(data: &[u8]) -> String {
    hex(ring::digest::digest(&ring::digest::SHA256, data).as_ref())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&key, data).as_ref().to_vec()
}

fn uri_encode(value: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        let keep = byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'.' | b'~')
            || (!encode_slash && byte == b'/');
        if keep {
            out.push(byte as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}
