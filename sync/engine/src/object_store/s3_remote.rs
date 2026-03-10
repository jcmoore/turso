use std::collections::BTreeMap;

use hmac::{Hmac, Mac};
use reqwest::blocking::{Client, RequestBuilder};
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, ETAG, HOST, IF_MATCH, IF_NONE_MATCH,
};
use reqwest::{Method, StatusCode, Url};
use sha2::{Digest, Sha256};

use super::remote::{ObjectStoreRemote, ObjectStoreRemoteError};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3StaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3ObjectStoreRemoteConfig {
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub credentials: S3StaticCredentials,
}

#[derive(Clone)]
pub struct S3ObjectStoreRemote {
    client: Client,
    config: S3ObjectStoreRemoteConfig,
    endpoint: Url,
}

struct SignedPutRequest {
    url: Url,
    headers: HeaderMap,
    #[cfg(test)]
    canonical_request: String,
    #[cfg(test)]
    canonical_request_hash: String,
}

impl S3ObjectStoreRemote {
    pub fn new(config: S3ObjectStoreRemoteConfig) -> Result<Self, ObjectStoreRemoteError> {
        let endpoint = Url::parse(config.endpoint.trim_end_matches('/')).map_err(|err| {
            ObjectStoreRemoteError::Transport(format!("invalid S3 endpoint: {err}"))
        })?;
        let client = Client::builder()
            .build()
            .map_err(|err| ObjectStoreRemoteError::Transport(err.to_string()))?;
        Ok(Self {
            client,
            config,
            endpoint,
        })
    }

    fn put_object(
        &self,
        key: &str,
        body: &[u8],
        condition: PutCondition<'_>,
    ) -> Result<String, ObjectStoreRemoteError> {
        let signed = self.sign_put_request(key, body, condition, chrono::Utc::now())?;
        let response = self.send(
            self.client
                .request(Method::PUT, signed.url)
                .headers(signed.headers)
                .body(body.to_vec()),
        )?;
        let status = response.status();
        match status {
            status if status.is_success() => etag_from_headers(response.headers()),
            StatusCode::PRECONDITION_FAILED | StatusCode::CONFLICT => match condition {
                PutCondition::IfNoneMatchStar => {
                    Err(ObjectStoreRemoteError::AlreadyExists(key.to_string()))
                }
                PutCondition::IfMatch(_) => Err(ObjectStoreRemoteError::Conflict),
            },
            _ => {
                let body = response.text().unwrap_or_default();
                Err(ObjectStoreRemoteError::Transport(format!(
                    "S3 PUT {key} failed: {status} {body}"
                )))
            }
        }
    }

    fn send(
        &self,
        request: RequestBuilder,
    ) -> Result<reqwest::blocking::Response, ObjectStoreRemoteError> {
        request
            .send()
            .map_err(|err| ObjectStoreRemoteError::Transport(err.to_string()))
    }

    fn sign_put_request(
        &self,
        key: &str,
        body: &[u8],
        condition: PutCondition<'_>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<SignedPutRequest, ObjectStoreRemoteError> {
        let path = self.object_path(key);
        let url = self
            .endpoint
            .join(path.trim_start_matches('/'))
            .map_err(|err| ObjectStoreRemoteError::Transport(err.to_string()))?;
        let payload_hash = hex_sha256(body);
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        let mut signed_headers = BTreeMap::new();
        signed_headers.insert(
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        );
        signed_headers.insert("host".to_string(), host_header_value(&url)?);
        signed_headers.insert("x-amz-content-sha256".to_string(), payload_hash.clone());
        signed_headers.insert("x-amz-date".to_string(), amz_date.clone());
        match condition {
            PutCondition::IfNoneMatchStar => {
                signed_headers.insert("if-none-match".to_string(), "*".to_string());
            }
            PutCondition::IfMatch(etag) => {
                signed_headers.insert("if-match".to_string(), etag.to_string());
            }
        }
        if let Some(token) = &self.config.credentials.session_token {
            signed_headers.insert("x-amz-security-token".to_string(), token.clone());
        }

        let canonical_headers = signed_headers
            .iter()
            .map(|(name, value)| format!("{name}:{}\n", normalize_header_value(value)))
            .collect::<String>();
        let signed_header_names = signed_headers
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(";");
        let canonical_request =
            format!("PUT\n{path}\n\n{canonical_headers}\n{signed_header_names}\n{payload_hash}");
        let scope = format!("{date_stamp}/{}/s3/aws4_request", self.config.region);
        let canonical_request_hash = hex_sha256(canonical_request.as_bytes());
        let string_to_sign =
            format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{canonical_request_hash}");
        let signing_key = derive_signing_key(
            &self.config.credentials.secret_access_key,
            &date_stamp,
            &self.config.region,
        )?;
        let signature = hex::encode(hmac_sha256(&signing_key, &string_to_sign)?);
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_header_names}, Signature={signature}",
            self.config.credentials.access_key_id
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("application/octet-stream"),
        );
        headers.insert(HOST, header_value(&host_header_value(&url)?)?);
        headers.insert(
            HeaderName::from_static("x-amz-content-sha256"),
            header_value(&payload_hash)?,
        );
        headers.insert(
            HeaderName::from_static("x-amz-date"),
            header_value(&amz_date)?,
        );
        headers.insert(AUTHORIZATION, header_value(&authorization)?);
        match condition {
            PutCondition::IfNoneMatchStar => {
                headers.insert(IF_NONE_MATCH, HeaderValue::from_static("*"));
            }
            PutCondition::IfMatch(etag) => {
                headers.insert(IF_MATCH, header_value(etag)?);
            }
        }
        if let Some(token) = &self.config.credentials.session_token {
            headers.insert(
                HeaderName::from_static("x-amz-security-token"),
                header_value(token)?,
            );
        }

        Ok(SignedPutRequest {
            url,
            headers,
            #[cfg(test)]
            canonical_request,
            #[cfg(test)]
            canonical_request_hash,
        })
    }

    fn object_path(&self, key: &str) -> String {
        let mut pieces = Vec::new();
        pieces.push(encode_path_segment(trim_slashes(&self.config.bucket)));
        let prefix = encode_object_key(&self.config.prefix);
        if !prefix.is_empty() {
            pieces.push(prefix);
        }
        let key = encode_object_key(key);
        if !key.is_empty() {
            pieces.push(key);
        }
        format!("/{}", pieces.join("/"))
    }

    pub fn database_id(&self) -> String {
        format!(
            "s3_page_v1:{}/{}/{}",
            self.endpoint,
            trim_slashes(&self.config.bucket),
            trim_slashes(&self.config.prefix)
        )
    }
}

impl ObjectStoreRemote for S3ObjectStoreRemote {
    fn put_immutable(&self, key: &str, body: &[u8]) -> Result<(), ObjectStoreRemoteError> {
        self.put_object(key, body, PutCondition::IfNoneMatchStar)
            .map(|_| ())
    }

    fn put_head_if_absent(&self, body: &[u8]) -> Result<String, ObjectStoreRemoteError> {
        self.put_object("refs/head", body, PutCondition::IfNoneMatchStar)
    }

    fn cas_head(&self, body: &[u8], etag: &str) -> Result<String, ObjectStoreRemoteError> {
        self.put_object("refs/head", body, PutCondition::IfMatch(etag))
    }
}

#[derive(Clone, Copy)]
enum PutCondition<'a> {
    IfNoneMatchStar,
    IfMatch(&'a str),
}

fn header_value(value: &str) -> Result<HeaderValue, ObjectStoreRemoteError> {
    HeaderValue::from_str(value)
        .map_err(|err| ObjectStoreRemoteError::Transport(format!("invalid header value: {err}")))
}

fn etag_from_headers(headers: &HeaderMap) -> Result<String, ObjectStoreRemoteError> {
    let etag = headers
        .get(ETAG)
        .ok_or_else(|| ObjectStoreRemoteError::Transport("S3 response missing ETag".to_string()))?
        .to_str()
        .map_err(|err| ObjectStoreRemoteError::Transport(err.to_string()))?;
    Ok(etag.trim_matches('"').to_string())
}

fn host_header_value(url: &Url) -> Result<String, ObjectStoreRemoteError> {
    let host = url.host_str().ok_or_else(|| {
        ObjectStoreRemoteError::Transport("S3 endpoint host is missing".to_string())
    })?;
    Ok(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

fn hex_sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn hmac_sha256(key: &[u8], value: &str) -> Result<Vec<u8>, ObjectStoreRemoteError> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|err| ObjectStoreRemoteError::Transport(err.to_string()))?;
    mac.update(value.as_bytes());
    Ok(mac.finalize().into_bytes().to_vec())
}

fn derive_signing_key(
    secret_access_key: &str,
    date_stamp: &str,
    region: &str,
) -> Result<Vec<u8>, ObjectStoreRemoteError> {
    let k_date = hmac_sha256(format!("AWS4{secret_access_key}").as_bytes(), date_stamp)?;
    let k_region = hmac_sha256(&k_date, region)?;
    let k_service = hmac_sha256(&k_region, "s3")?;
    hmac_sha256(&k_service, "aws4_request")
}

fn trim_slashes(value: &str) -> &str {
    value.trim_matches('/')
}

fn encode_object_key(value: &str) -> String {
    trim_slashes(value)
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(encode_path_segment)
        .collect::<Vec<_>>()
        .join("/")
}

fn encode_path_segment(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(*byte as char)
            }
            _ => encoded.push_str(&format!("%{:02X}", byte)),
        }
    }
    encoded
}

fn normalize_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::{
        derive_signing_key, encode_object_key, encode_path_segment, hmac_sha256,
        normalize_header_value, PutCondition, S3ObjectStoreRemote, S3ObjectStoreRemoteConfig,
        S3StaticCredentials,
    };
    use reqwest::header::AUTHORIZATION;

    #[test]
    fn encodes_object_keys_without_collapsing_slashes() {
        assert_eq!(encode_path_segment("db name"), "db%20name");
        assert_eq!(
            encode_object_key("db/main/refs head"),
            "db/main/refs%20head"
        );
    }

    #[test]
    fn normalizes_header_whitespace() {
        assert_eq!(normalize_header_value(" a   b\tc "), "a b c");
    }

    #[test]
    fn derives_stable_database_identity() {
        let remote = S3ObjectStoreRemote::new(S3ObjectStoreRemoteConfig {
            endpoint: "http://localhost:8333/".to_string(),
            bucket: "/suite-a/".to_string(),
            prefix: "/db/main/".to_string(),
            region: "us-east-1".to_string(),
            credentials: S3StaticCredentials {
                access_key_id: "a".to_string(),
                secret_access_key: "b".to_string(),
                session_token: None,
            },
        })
        .unwrap();

        assert_eq!(
            remote.database_id(),
            "s3_page_v1:http://localhost:8333//suite-a/db/main"
        );
    }

    #[test]
    fn derives_signing_key() {
        let key = derive_signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20260310",
            "us-east-1",
        )
        .unwrap();
        assert_eq!(
            hex::encode(key),
            "8c9c0370f0c0f085964f69ae5d7f5d6c63a0bd885a30402ca0067c9004a6a7d6"
        );
    }

    #[test]
    fn matches_javascript_authorization_header_for_segment_put() {
        let signing_key = derive_signing_key("admin_secret_key", "20260310", "us-east-1").unwrap();
        assert_eq!(
            hex::encode(&signing_key),
            "b277177d978a8395b1740cce0722a554910ad1b55a728b1262bb712b03f550c6"
        );
        let js_string_to_sign = "AWS4-HMAC-SHA256\n20260310T123456Z\n20260310/us-east-1/s3/aws4_request\n26ee417d44ec000f114f33c1e38ec2d4670004413e72060fd8557dfb0dae5561";
        assert_eq!(
            hex::encode(hmac_sha256(&signing_key, js_string_to_sign).unwrap()),
            "b5b18ca8e6b8e085da1bdeb7c03b78ea63ac9a673f726e9df064fe12c50cf334"
        );

        let remote = S3ObjectStoreRemote::new(S3ObjectStoreRemoteConfig {
            endpoint: "http://127.0.0.1:8333".to_string(),
            bucket: "runtime-123".to_string(),
            prefix: "db/main".to_string(),
            region: "us-east-1".to_string(),
            credentials: S3StaticCredentials {
                access_key_id: "admin".to_string(),
                secret_access_key: "admin_secret_key".to_string(),
                session_token: None,
            },
        })
        .unwrap();
        let signed = remote
            .sign_put_request(
                "segments/seq-1-native-runtime-abc-1-segment-0",
                &[1, 2, 3],
                PutCondition::IfNoneMatchStar,
                chrono::Utc
                    .with_ymd_and_hms(2026, 3, 10, 12, 34, 56)
                    .single()
                    .unwrap(),
            )
            .unwrap();

        assert_eq!(
            signed.url.as_str(),
            "http://127.0.0.1:8333/runtime-123/db/main/segments/seq-1-native-runtime-abc-1-segment-0"
        );
        assert_eq!(
            signed.canonical_request_hash,
            "26ee417d44ec000f114f33c1e38ec2d4670004413e72060fd8557dfb0dae5561",
            "{}",
            signed.canonical_request
        );
        assert_eq!(
            signed.headers.get(AUTHORIZATION).unwrap(),
            "AWS4-HMAC-SHA256 Credential=admin/20260310/us-east-1/s3/aws4_request, SignedHeaders=content-type;host;if-none-match;x-amz-content-sha256;x-amz-date, Signature=b5b18ca8e6b8e085da1bdeb7c03b78ea63ac9a673f726e9df064fe12c50cf334"
        );
    }
}
