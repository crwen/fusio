// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{collections::BTreeMap, io, sync::Arc};

use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use http::{
    header::{AUTHORIZATION, HOST},
    HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode,
};
use http_body::Body;
use http_body_util::{BodyExt, Empty};
use percent_encoding::utf8_percent_encode;
use serde::Deserialize;
use thiserror::Error;
use url::Url;

use super::CHECKSUM_HEADER;
use crate::{
    error::BoxedError,
    remotes::{
        aws::{STRICT_ENCODE_SET, STRICT_PATH_ENCODE_SET},
        http::HttpClient,
    },
};

const EMPTY_SHA256_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
const STREAMING_PAYLOAD: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

#[derive(Debug, Clone)]
pub struct AwsCredential {
    /// AWS_ACCESS_KEY_ID
    pub key_id: String,
    /// AWS_SECRET_ACCESS_KEY
    pub secret_key: String,
    /// AWS_SESSION_TOKEN
    pub token: Option<String>,
}

impl AwsCredential {
    /// Signs a string
    ///
    /// <https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html>
    fn sign(&self, to_sign: &str, date: DateTime<Utc>, region: &str, service: &str) -> String {
        let date_string = date.format("%Y%m%d").to_string();
        let date_hmac = hmac_sha256(format!("AWS4{}", self.secret_key), date_string);
        let region_hmac = hmac_sha256(date_hmac, region);
        let service_hmac = hmac_sha256(region_hmac, service);
        let signing_hmac = hmac_sha256(service_hmac, b"aws4_request");
        hex_encode(hmac_sha256(signing_hmac, to_sign).as_ref())
    }
}

fn hmac_sha256(secret: impl AsRef<[u8]>, bytes: impl AsRef<[u8]>) -> ring::hmac::Tag {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_ref());
    ring::hmac::sign(&key, bytes.as_ref())
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // String writing is infallible
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Authorize a [`Request`] with an [`AwsCredential`] using [AWS SigV4]
///
/// [AWS SigV4]: https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html
#[derive(Debug)]
pub struct AwsAuthorizer<'a> {
    date: Option<DateTime<Utc>>,
    credential: &'a AwsCredential,
    service: &'a str,
    region: &'a str,
    token_header: Option<HeaderName>,
    sign_payload: bool,
}

static DATE_HEADER: HeaderName = HeaderName::from_static("x-amz-date");
static HASH_HEADER: HeaderName = HeaderName::from_static("x-amz-content-sha256");
static TOKEN_HEADER: HeaderName = HeaderName::from_static("x-amz-security-token");
const ALGORITHM: &str = "AWS4-HMAC-SHA256";

impl<'a> AwsAuthorizer<'a> {
    /// Create a new [`AwsAuthorizer`]
    pub fn new(credential: &'a AwsCredential, service: &'a str, region: &'a str) -> Self {
        Self {
            credential,
            service,
            region,
            date: None,
            sign_payload: true,
            token_header: None,
        }
    }

    /// Controls whether this [`AwsAuthorizer`] will attempt to sign the request payload,
    /// the default is `true`
    pub fn with_sign_payload(mut self, signed: bool) -> Self {
        self.sign_payload = signed;
        self
    }

    // /// Overrides the header name for security tokens, defaults to `x-amz-security-token`
    // pub(crate) fn with_token_header(mut self, header: HeaderName) -> Self {
    //     self.token_header = Some(header);
    //     self
    // }

    /// Authorize `request` with an optional pre-calculated SHA256 digest by attaching
    /// the relevant [AWS SigV4] headers
    ///
    /// # Payload Signature
    ///
    /// AWS SigV4 requests must contain the `x-amz-content-sha256` header, it is set as follows:
    ///
    /// * If not configured to sign payloads, it is set to `UNSIGNED-PAYLOAD`
    /// * If a `pre_calculated_digest` is provided, it is set to the hex encoding of it
    /// * If it is a streaming request, it is set to `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`
    /// * Otherwise it is set to the hex encoded SHA256 of the request body
    ///
    /// [AWS SigV4]: https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html
    pub(crate) async fn authorize<B>(&self, request: &mut Request<B>) -> Result<(), AuthorizeError>
    where
        B: Body<Data = Bytes> + Clone + Unpin,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        if let Some(token) = &self.credential.token {
            let header = self.token_header.as_ref().unwrap_or(&TOKEN_HEADER);
            request.headers_mut().insert(header, token.parse()?);
        }

        let host = request
            .uri()
            .authority()
            .ok_or(AuthorizeError::NoHost)?
            .as_str()
            .to_string();
        request.headers_mut().insert(HOST, host.parse()?);

        let date = self.date.unwrap_or_else(Utc::now);
        let date_str = date.format("%Y%m%dT%H%M%SZ").to_string();
        request
            .headers_mut()
            .insert(&DATE_HEADER, date_str.parse()?);

        let digest = match self.sign_payload {
            false => UNSIGNED_PAYLOAD.to_string(),
            true => match request.headers().get(CHECKSUM_HEADER) {
                Some(checksum) => {
                    dbg!(checksum);
                    hex_encode(std::str::from_utf8(checksum.as_bytes()).unwrap().as_bytes())
                }
                None => match request.body().size_hint().exact() {
                    Some(n) => match n {
                        0 => EMPTY_SHA256_HASH.to_string(),
                        _ => {
                            // dbg!("hit");
                            let bytes = request
                                .body()
                                .clone()
                                .collect()
                                .await
                                .map_err(|_| AuthorizeError::BodyNoFrame)?
                                .to_bytes();
                            hex_digest(&bytes)
                        }
                    },
                    None => STREAMING_PAYLOAD.to_string(),
                },
            },
        };
        request.headers_mut().insert(&HASH_HEADER, digest.parse()?);

        let (signed_headers, canonical_headers) = canonicalize_headers(request.headers());

        let scope = self.scope(date);

        let string_to_sign = self.string_to_sign(
            date,
            &scope,
            request.method(),
            &Url::parse(&request.uri().to_string())?,
            &canonical_headers,
            &signed_headers,
            &digest,
        );

        // sign the string
        let signature = self
            .credential
            .sign(&string_to_sign, date, self.region, self.service);

        // build the actual auth header
        let authorisation = format!(
            "{} Credential={}/{}, SignedHeaders={}, Signature={}",
            ALGORITHM, self.credential.key_id, scope, signed_headers, signature
        );
        request
            .headers_mut()
            .insert(AUTHORIZATION, authorisation.parse()?);

        Ok(())
    }

    #[allow(unused)]
    pub(crate) fn sign(&self, method: Method, url: &mut Url, expires_in: u32) {
        let date = self.date.unwrap_or_else(Utc::now);
        let scope = self.scope(date);

        // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
        url.query_pairs_mut()
            .append_pair("X-Amz-Algorithm", ALGORITHM)
            .append_pair(
                "X-Amz-Credential",
                &format!("{}/{}", self.credential.key_id, scope),
            )
            .append_pair("X-Amz-Date", &date.format("%Y%m%dT%H%M%SZ").to_string())
            .append_pair("X-Amz-Expires", &expires_in.to_string())
            .append_pair("X-Amz-SignedHeaders", "host");

        // For S3, you must include the X-Amz-Security-Token query parameter in the URL if
        // using credentials sourced from the STS service.
        if let Some(token) = &self.credential.token {
            url.query_pairs_mut()
                .append_pair("X-Amz-Security-Token", token);
        }

        // We don't have a payload; the user is going to send the payload directly themselves.
        let digest = UNSIGNED_PAYLOAD;

        let host = &url[url::Position::BeforeHost..url::Position::AfterPort].to_string();
        let mut headers = HeaderMap::new();
        let host_val = HeaderValue::from_str(host).unwrap();
        headers.insert("host", host_val);

        let (signed_headers, canonical_headers) = canonicalize_headers(&headers);

        let string_to_sign = self.string_to_sign(
            date,
            &scope,
            &method,
            url,
            &canonical_headers,
            &signed_headers,
            digest,
        );

        let signature = self
            .credential
            .sign(&string_to_sign, date, self.region, self.service);

        url.query_pairs_mut()
            .append_pair("X-Amz-Signature", &signature);
    }

    #[allow(clippy::too_many_arguments)]
    fn string_to_sign(
        &self,
        date: DateTime<Utc>,
        scope: &str,
        request_method: &Method,
        url: &Url,
        canonical_headers: &str,
        signed_headers: &str,
        digest: &str,
    ) -> String {
        // Each path segment must be URI-encoded twice (except for Amazon S3 which only gets
        // URI-encoded once).
        // see https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
        let canonical_uri = match self.service {
            "s3" => url.path().to_string(),
            _ => utf8_percent_encode(url.path(), &STRICT_PATH_ENCODE_SET).to_string(),
        };

        let canonical_query = canonicalize_query(url);

        // https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            request_method.as_str(),
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers,
            digest
        );

        let hashed_canonical_request = hex_digest(canonical_request.as_bytes());

        format!(
            "{}\n{}\n{}\n{}",
            ALGORITHM,
            date.format("%Y%m%dT%H%M%SZ"),
            scope,
            hashed_canonical_request
        )
    }

    fn scope(&self, date: DateTime<Utc>) -> String {
        format!(
            "{}/{}/{}/aws4_request",
            date.format("%Y%m%d"),
            self.region,
            self.service
        )
    }
}

/// Canonicalizes headers into the AWS Canonical Form.
///
/// <https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html>
fn canonicalize_headers(header_map: &HeaderMap) -> (String, String) {
    let mut headers = BTreeMap::<&str, Vec<&str>>::new();
    let mut value_count = 0;
    let mut value_bytes = 0;
    let mut key_bytes = 0;

    for (key, value) in header_map {
        let key = key.as_str();
        if ["authorization", "content-length", "user-agent"].contains(&key) {
            continue;
        }

        let value = std::str::from_utf8(value.as_bytes()).unwrap();
        key_bytes += key.len();
        value_bytes += value.len();
        value_count += 1;
        headers.entry(key).or_default().push(value);
    }

    let mut signed_headers = String::with_capacity(key_bytes + headers.len());
    let mut canonical_headers =
        String::with_capacity(key_bytes + value_bytes + headers.len() + value_count);

    for (header_idx, (name, values)) in headers.into_iter().enumerate() {
        if header_idx != 0 {
            signed_headers.push(';');
        }

        signed_headers.push_str(name);
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        for (value_idx, value) in values.into_iter().enumerate() {
            if value_idx != 0 {
                canonical_headers.push(',');
            }
            canonical_headers.push_str(value.trim());
        }
        canonical_headers.push('\n');
    }

    (signed_headers, canonical_headers)
}

/// Canonicalizes query parameters into the AWS canonical form
///
/// <https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html>
fn canonicalize_query(url: &Url) -> String {
    use std::fmt::Write;

    let capacity = match url.query() {
        Some(q) if !q.is_empty() => q.len(),
        _ => return String::new(),
    };
    let mut encoded = String::with_capacity(capacity + 1);

    let mut headers = url.query_pairs().collect::<Vec<_>>();
    headers.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

    let mut first = true;
    for (k, v) in headers {
        if !first {
            encoded.push('&');
        }
        first = false;
        let _ = write!(
            encoded,
            "{}={}",
            utf8_percent_encode(k.as_ref(), &STRICT_ENCODE_SET),
            utf8_percent_encode(v.as_ref(), &STRICT_ENCODE_SET)
        );
    }
    encoded
}

fn hex_digest(bytes: &[u8]) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, bytes);
    hex_encode(digest.as_ref())
}

#[derive(Debug, Error)]
pub enum AuthorizeError {
    #[error("Invalid header value: {0}")]
    InvalidHeaderValue(#[from] http::header::InvalidHeaderValue),
    #[error("Invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
    #[error("No host in URL")]
    NoHost,
    #[error("Failed to sign request: {0}")]
    SignHashFailed(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Body no frame")]
    BodyNoFrame,
}

/// <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html#instance-metadata-security-credentials>
#[allow(unused)]
async fn instance_creds<'c, C: HttpClient>(
    client: &'c C,
    endpoint: &'c str,
    imdsv1_fallback: bool,
) -> Result<TemporaryToken<Arc<AwsCredential>>, BoxedError> {
    const CREDENTIALS_PATH: &str = "latest/meta-data/iam/security-credentials";
    const AWS_EC2_METADATA_TOKEN_HEADER: &str = "X-aws-ec2-metadata-token";

    let token_url = format!("{endpoint}/latest/api/token");

    let request = Request::builder()
        .method(Method::PUT)
        .uri(token_url)
        .header("X-aws-ec2-metadata-token-ttl-seconds", "600")
        .body(Empty::<Bytes>::new())?;

    let token_result = client
        .send_request(request)
        .await
        .map_err(io::Error::other)?;

    let token = match token_result.status() {
        StatusCode::OK => Some(
            token_result
                .collect()
                .await
                .map_err(io::Error::other)?
                .to_bytes(),
        ),
        StatusCode::FORBIDDEN if imdsv1_fallback => None,
        _ => {
            return Err(format!(
                "Failed to get instance metadata token, status: {}",
                token_result.status()
            )
            .into());
        }
    };

    let role_url = format!("{endpoint}/{CREDENTIALS_PATH}/");
    let mut role_request = Request::builder().method(Method::GET).uri(role_url);

    if let Some(token) = &token {
        role_request = role_request.header(
            AWS_EC2_METADATA_TOKEN_HEADER,
            String::from_utf8(token.to_vec()).unwrap(),
        );
    }

    let role = client
        .send_request(
            role_request
                .body(Empty::<Bytes>::new())
                .map_err(io::Error::other)?,
        )
        .await
        .map_err(io::Error::other)?
        .collect()
        .await
        .map_err(io::Error::other)?
        .to_bytes();
    let role = String::from_utf8(role.to_vec()).map_err(io::Error::other)?;

    let creds_url = format!("{endpoint}/{CREDENTIALS_PATH}/{role}");
    let mut creds_request = Request::builder().uri(creds_url).method(Method::GET);
    if let Some(token) = &token {
        creds_request = creds_request.header(
            AWS_EC2_METADATA_TOKEN_HEADER,
            String::from_utf8(token.to_vec()).map_err(io::Error::other)?,
        );
    }

    let response = client
        .send_request(
            creds_request
                .body(Empty::<Bytes>::new())
                .map_err(io::Error::other)?,
        )
        .await
        .map_err(io::Error::other)?
        .collect()
        .await
        .map_err(io::Error::other)?
        .aggregate()
        .reader();

    let creds: InstanceCredentials = serde_json::from_reader(response).map_err(io::Error::other)?;

    let now = Utc::now();
    let ttl = (creds.expiration - now).to_std().unwrap_or_default();
    Ok(TemporaryToken {
        token: Arc::new(creds.into()),
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InstanceCredentials {
    access_key_id: String,
    secret_access_key: String,
    token: String,
    expiration: DateTime<Utc>,
}

impl From<InstanceCredentials> for AwsCredential {
    fn from(s: InstanceCredentials) -> Self {
        Self {
            key_id: s.access_key_id,
            secret_key: s.secret_access_key,
            token: Some(s.token),
        }
    }
}

#[allow(unused)]
pub(crate) struct TemporaryToken<T> {
    /// The temporary credential
    pub(crate) token: T,
}

#[cfg(test)]
mod tests {

    #[allow(unused)]
    use bytes::Bytes;
    use chrono::{DateTime, Utc};
    #[allow(unused)]
    use http::{header::AUTHORIZATION, Method, Request};
    #[allow(unused)]
    use http_body_util::Empty;
    use url::Url;

    use crate::remotes::aws::credential::{AwsAuthorizer, AwsCredential};

    // Test generated using https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_sign_with_signed_payload() {
        // Test credentials from https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html
        let credential = AwsCredential {
            key_id: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            token: None,
        };

        // method = 'GET'
        // service = 'ec2'
        // host = 'ec2.amazonaws.com'
        // region = 'us-east-1'
        // endpoint = 'https://ec2.amazonaws.com'
        // request_parameters = ''
        let date = DateTime::parse_from_rfc3339("2022-08-06T18:01:34Z")
            .unwrap()
            .with_timezone(&Utc);

        let request = Request::builder()
            .uri("https://ec2.amazon.com/")
            .method(Method::GET);

        let signer = AwsAuthorizer {
            date: Some(date),
            credential: &credential,
            service: "ec2",
            region: "us-east-1",
            sign_payload: true,
            token_header: None,
        };

        let mut request = request.body(Empty::<Bytes>::new()).unwrap();
        signer.authorize(&mut request).await.unwrap();
        assert_eq!(
            request.headers().get(&AUTHORIZATION).unwrap(),
            "AWS4-HMAC-SHA256 \
             Credential=AKIAIOSFODNN7EXAMPLE/20220806/us-east-1/ec2/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
             Signature=a3c787a7ed37f7fdfbfd2d7056a3d7c9d85e6d52a2bfbec73793c0be6e7862d4"
        )
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_sign_with_unsigned_payload() {
        // Test credentials from https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html
        let credential = AwsCredential {
            key_id: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            token: None,
        };

        // method = 'GET'
        // service = 'ec2'
        // host = 'ec2.amazonaws.com'
        // region = 'us-east-1'
        // endpoint = 'https://ec2.amazonaws.com'
        // request_parameters = ''
        let date = DateTime::parse_from_rfc3339("2022-08-06T18:01:34Z")
            .unwrap()
            .with_timezone(&Utc);

        let request = Request::builder()
            .uri("https://ec2.amazon.com/")
            .method(Method::GET);

        let authorizer = AwsAuthorizer {
            date: Some(date),
            credential: &credential,
            service: "ec2",
            region: "us-east-1",
            token_header: None,
            sign_payload: false,
        };

        let mut request = request.body(Empty::<Bytes>::new()).unwrap();
        authorizer.authorize(&mut request).await.unwrap();
        assert_eq!(
            request.headers().get(&AUTHORIZATION).unwrap(),
            "AWS4-HMAC-SHA256 \
             Credential=AKIAIOSFODNN7EXAMPLE/20220806/us-east-1/ec2/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
             Signature=653c3d8ea261fd826207df58bc2bb69fbb5003e9eb3c0ef06e4a51f2a81d8699"
        );
    }

    #[test]
    fn signed_get_url() {
        // Values from https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
        let credential = AwsCredential {
            key_id: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            token: None,
        };

        let date = DateTime::parse_from_rfc3339("2013-05-24T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let authorizer = AwsAuthorizer {
            date: Some(date),
            credential: &credential,
            service: "s3",
            region: "us-east-1",
            token_header: None,
            sign_payload: false,
        };

        let mut url = Url::parse("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        authorizer.sign(Method::GET, &mut url, 86400);

        assert_eq!(
            url,
            Url::parse(
                "https://examplebucket.s3.amazonaws.com/test.txt?\
                X-Amz-Algorithm=AWS4-HMAC-SHA256&\
                X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&\
                X-Amz-Date=20130524T000000Z&\
                X-Amz-Expires=86400&\
                X-Amz-SignedHeaders=host&\
                X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
            )
            .unwrap()
        );
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_sign_port() {
        let credential = AwsCredential {
            key_id: "H20ABqCkLZID4rLe".into(),
            secret_key: "jMqRDgxSsBqqznfmddGdu1TmmZOJQxdM".into(),
            token: None,
        };

        let date = DateTime::parse_from_rfc3339("2022-08-09T13:05:25Z")
            .unwrap()
            .with_timezone(&Utc);

        let request = Request::builder()
            .uri("http://localhost:9000/tsm-schemas?delimiter=%2F&encoding-type=url&list-type=2&prefix=")
            .method(Method::GET);

        let authorizer = AwsAuthorizer {
            date: Some(date),
            credential: &credential,
            service: "s3",
            region: "us-east-1",
            token_header: None,
            sign_payload: true,
        };

        let mut request = request.body(Empty::<Bytes>::new()).unwrap();
        authorizer.authorize(&mut request).await.unwrap();
        assert_eq!(
            request.headers().get(&AUTHORIZATION).unwrap(),
            "AWS4-HMAC-SHA256 Credential=H20ABqCkLZID4rLe/20220809/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
             Signature=9ebf2f92872066c99ac94e573b4e1b80f4dbb8a32b1e8e23178318746e7d1b4d"
        )
    }

    #[cfg(all(feature = "tokio-http", not(feature = "completion-based")))]
    #[tokio::test]
    async fn test_instance_metadata() {
        use std::env;

        use http::StatusCode;

        use crate::remotes::{
            aws::credential::instance_creds,
            http::{tokio::TokioClient, HttpClient},
        };

        if env::var("TEST_INTEGRATION").is_err() {
            eprintln!("skipping AWS integration test");
            return;
        }

        let client = TokioClient::new();
        // For example https://github.com/aws/amazon-ec2-metadata-mock
        let endpoint = env::var("EC2_METADATA_ENDPOINT").unwrap();

        let request = Request::builder()
            .uri(format!("{endpoint}/latest/meta-data/ami-id"))
            .method(Method::GET)
            .body(Empty::<Bytes>::new())
            .unwrap();

        let resp = client.send_request(request).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "Ensure metadata endpoint is set to only allow IMDSv2"
        );

        let creds = instance_creds(&client, &endpoint, false).await.unwrap();

        let id = &creds.token.key_id;
        let secret = &creds.token.secret_key;
        let token = creds.token.token.as_ref().unwrap();

        assert!(!id.is_empty());
        assert!(!secret.is_empty());
        assert!(!token.is_empty())
    }
}
