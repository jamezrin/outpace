//! A minimal HTTP/1.1 GET that writes the request exactly as the official engine does
//! (header order, spelling and values), for AceStream services under the AceStream identity.
//! Plain HTTP only; responses are bounded and may be chunked and/or gzip-encoded.

use crate::http_fetch::{self, FetchPolicy};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Why an engine-style request failed. Carries no URL or response data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineHttpError {
    Resolve,
    Connect,
    Io,
    Timeout,
    TooLarge,
    Response,
}

/// `GET path_and_query` from `host:port` with the engine's request head and `user_agent`.
/// Only globally routable addresses are dialed. The whole exchange is bounded by `timeout`
/// and the raw response by `limit` bytes.
pub async fn get(
    host: &str,
    port: u16,
    path_and_query: &str,
    user_agent: &str,
    limit: usize,
    timeout: Duration,
) -> Result<Vec<u8>, EngineHttpError> {
    tokio::time::timeout(timeout, async {
        let (_cancel_tx, cancel) = tokio::sync::watch::channel(false);
        let addr = http_fetch::resolve_addr(host, port, FetchPolicy::default(), cancel)
            .await
            .map_err(|_| EngineHttpError::Resolve)?;
        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|_| EngineHttpError::Connect)?;
        let host_header = if port == 80 {
            host.to_owned()
        } else {
            format!("{host}:{port}")
        };
        let head =
            crate::engine_profile::engine_request_head(path_and_query, &host_header, user_agent);
        stream
            .write_all(head.as_bytes())
            .await
            .map_err(|_| EngineHttpError::Io)?;
        let mut response = Vec::new();
        let mut buf = [0u8; 8192];
        loop {
            let n = stream
                .read(&mut buf)
                .await
                .map_err(|_| EngineHttpError::Io)?;
            if n == 0 {
                break;
            }
            if response.len() + n > limit {
                return Err(EngineHttpError::TooLarge);
            }
            response.extend_from_slice(&buf[..n]);
        }
        crate::resolve::http_response_body(&response).map_err(|e| match e {
            crate::resolve::ResolveError::Catalog("catalog response too large") => {
                EngineHttpError::TooLarge
            }
            _ => EngineHttpError::Response,
        })
    })
    .await
    .unwrap_or(Err(EngineHttpError::Timeout))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn refuses_non_global_destinations() {
        let err = get("127.0.0.1", 80, "/", "ua", 1024, Duration::from_secs(2))
            .await
            .unwrap_err();
        assert_eq!(err, EngineHttpError::Resolve);
    }
}
