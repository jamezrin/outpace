//! A minimal HTTP/1.1 client for renderer descriptions and SOAP control.
//!
//! One request per connection (`Connection: close`), a single overall deadline, a hard cap on
//! the bytes read, and no redirect handling at all: a 3xx answer is an error. The peer is
//! always an already-validated [`LanUrl`], so there is no DNS and no proxy.

use super::url::LanUrl;
use super::CastError;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Largest response head (status line plus headers) accepted.
const MAX_HEAD: usize = 8 * 1024;
const MAX_HEADERS: usize = 64;

#[derive(Debug, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

pub struct Request<'a> {
    pub method: &'a str,
    pub url: &'a LanUrl,
    /// Extra headers. Values must be engine-built; [`build_request`] rejects CR/LF anyway.
    pub headers: &'a [(&'a str, &'a str)],
    pub body: &'a [u8],
    pub max_body: usize,
    pub timeout: Duration,
}

/// Send one request and read a bounded response before `timeout` elapses.
pub async fn send(request: Request<'_>) -> Result<HttpResponse, CastError> {
    let bytes = build_request(&request)?;
    let max_body = request.max_body;
    tokio::time::timeout(request.timeout, async move {
        let mut stream = tokio::net::TcpStream::connect(request.url.addr)
            .await
            .map_err(|_| CastError::Unreachable)?;
        stream
            .write_all(&bytes)
            .await
            .map_err(|_| CastError::Unreachable)?;
        let cap = MAX_HEAD + max_body;
        let mut buf = Vec::with_capacity(4096.min(cap));
        let mut chunk = [0u8; 4096];
        loop {
            let n = stream
                .read(&mut chunk)
                .await
                .map_err(|_| CastError::Unreachable)?;
            if n == 0 {
                return parse_response(&buf, max_body, true)?
                    .ok_or(CastError::Malformed("truncated http response"));
            }
            if buf.len() + n > cap {
                return Err(CastError::TooLarge);
            }
            buf.extend_from_slice(&chunk[..n]);
            if let Some(response) = parse_response(&buf, max_body, false)? {
                return Ok(response);
            }
        }
    })
    .await
    .map_err(|_| CastError::Timeout)?
}

fn build_request(request: &Request<'_>) -> Result<Vec<u8>, CastError> {
    let mut out = format!(
        "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nUser-Agent: outpace/{} UPnP/1.0 DLNADOC/1.50\r\n",
        request.method,
        request.url.path,
        request.url.addr,
        env!("CARGO_PKG_VERSION"),
    );
    for (name, value) in request.headers {
        if name
            .bytes()
            .chain(value.bytes())
            .any(|b| b == b'\r' || b == b'\n')
        {
            return Err(CastError::Malformed("header injection"));
        }
        out.push_str(name);
        out.push_str(": ");
        out.push_str(value);
        out.push_str("\r\n");
    }
    if !request.body.is_empty() || request.method == "POST" {
        out.push_str(&format!("Content-Length: {}\r\n", request.body.len()));
    }
    out.push_str("\r\n");
    let mut bytes = out.into_bytes();
    bytes.extend_from_slice(request.body);
    Ok(bytes)
}

/// Parse a buffered response. `Ok(None)` means more bytes are needed; `eof` says the peer
/// closed, which completes a body without framing.
pub fn parse_response(
    buf: &[u8],
    max_body: usize,
    eof: bool,
) -> Result<Option<HttpResponse>, CastError> {
    let Some(head_end) = find(buf, b"\r\n\r\n") else {
        if buf.len() > MAX_HEAD {
            return Err(CastError::TooLarge);
        }
        return Ok(None);
    };
    if head_end > MAX_HEAD {
        return Err(CastError::TooLarge);
    }
    let head =
        std::str::from_utf8(&buf[..head_end]).map_err(|_| CastError::Malformed("bad http head"))?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let mut parts = status_line.splitn(3, ' ');
    let version = parts.next().unwrap_or_default();
    if !version.starts_with("HTTP/1.") {
        return Err(CastError::Malformed("bad http status line"));
    }
    let status = parts
        .next()
        .filter(|code| code.len() == 3)
        .and_then(|code| code.parse::<u16>().ok())
        .filter(|code| (100..600).contains(code))
        .ok_or(CastError::Malformed("bad http status"))?;
    if (300..400).contains(&status) {
        return Err(CastError::Redirect);
    }
    let mut content_length = None;
    let mut chunked = false;
    for (index, line) in lines.enumerate() {
        if index >= MAX_HEADERS {
            return Err(CastError::TooLarge);
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(CastError::Malformed("bad http header"));
        };
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            let length: usize = value
                .parse()
                .map_err(|_| CastError::Malformed("bad content-length"))?;
            if content_length.is_some_and(|known| known != length) {
                return Err(CastError::Malformed("conflicting content-length"));
            }
            content_length = Some(length);
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            if !value.eq_ignore_ascii_case("chunked") {
                return Err(CastError::Malformed("unsupported transfer-encoding"));
            }
            chunked = true;
        }
    }
    let body = &buf[head_end + 4..];
    let body = if chunked {
        match decode_chunked(body, max_body)? {
            Some(body) => body,
            None if eof => return Err(CastError::Malformed("truncated chunked body")),
            None => return Ok(None),
        }
    } else if let Some(length) = content_length {
        if length > max_body {
            return Err(CastError::TooLarge);
        }
        if body.len() < length {
            return if eof {
                Err(CastError::Malformed("truncated http body"))
            } else {
                Ok(None)
            };
        }
        body[..length].to_vec()
    } else {
        if body.len() > max_body {
            return Err(CastError::TooLarge);
        }
        if !eof {
            return Ok(None);
        }
        body.to_vec()
    };
    Ok(Some(HttpResponse { status, body }))
}

/// Decode a complete chunked body, `Ok(None)` while incomplete.
fn decode_chunked(mut data: &[u8], max_body: usize) -> Result<Option<Vec<u8>>, CastError> {
    let mut out = Vec::new();
    loop {
        let Some(line_end) = find(data, b"\r\n") else {
            return if data.len() > 64 {
                Err(CastError::Malformed("bad chunk size"))
            } else {
                Ok(None)
            };
        };
        let line = std::str::from_utf8(&data[..line_end])
            .map_err(|_| CastError::Malformed("bad chunk size"))?;
        let size_hex = line.split(';').next().unwrap_or_default().trim();
        if size_hex.is_empty() || size_hex.len() > 8 {
            return Err(CastError::Malformed("bad chunk size"));
        }
        let size = usize::from_str_radix(size_hex, 16)
            .map_err(|_| CastError::Malformed("bad chunk size"))?;
        data = &data[line_end + 2..];
        if size == 0 {
            // Trailers are ignored; a finished body needs only the final CRLF to have arrived,
            // but a peer that closes right after `0\r\n` is accepted at EOF by the caller.
            return Ok(find(data, b"\r\n").map(|_| out));
        }
        if out.len() + size > max_body {
            return Err(CastError::TooLarge);
        }
        if data.len() < size + 2 {
            return Ok(None);
        }
        if &data[size..size + 2] != b"\r\n" {
            return Err(CastError::Malformed("bad chunk terminator"));
        }
        out.extend_from_slice(&data[..size]);
        data = &data[size + 2..];
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete(raw: &[u8]) -> HttpResponse {
        parse_response(raw, 1024, true).unwrap().unwrap()
    }

    #[test]
    fn parses_content_length_and_eof_bodies() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhelloEXTRA";
        assert_eq!(
            parse_response(raw, 1024, false).unwrap(),
            Some(HttpResponse {
                status: 200,
                body: b"hello".to_vec()
            })
        );
        assert_eq!(
            complete(b"HTTP/1.0 500 Internal\r\nServer: x\r\n\r\nfault").body,
            b"fault"
        );
        assert_eq!(
            parse_response(b"HTTP/1.1 200 OK\r\nServer: x\r\n\r\npartial", 1024, false).unwrap(),
            None
        );
        assert_eq!(
            parse_response(b"HTTP/1.1 200 OK\r\nContent-Le", 1024, false).unwrap(),
            None
        );
    }

    #[test]
    fn decodes_chunked_bodies() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2;ext=1\r\nde\r\n0\r\n\r\n";
        assert_eq!(complete(raw).body, b"abcde");
        let partial = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nab";
        assert_eq!(parse_response(partial, 1024, false).unwrap(), None);
        assert!(parse_response(partial, 1024, true).is_err());
        let bad = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n";
        assert!(parse_response(bad, 1024, false).is_err());
    }

    #[test]
    fn rejects_redirects_oversize_and_garbage() {
        assert_eq!(
            parse_response(
                b"HTTP/1.1 302 Found\r\nLocation: http://8.8.8.8/\r\n\r\n",
                1024,
                false
            ),
            Err(CastError::Redirect)
        );
        assert_eq!(
            parse_response(
                b"HTTP/1.1 200 OK\r\nContent-Length: 2048\r\n\r\n",
                1024,
                false
            ),
            Err(CastError::TooLarge)
        );
        let big_chunk = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n800\r\n";
        assert_eq!(
            parse_response(big_chunk, 1024, false),
            Err(CastError::TooLarge)
        );
        let huge_head = vec![b'a'; MAX_HEAD + 1];
        assert_eq!(
            parse_response(&huge_head, 1024, false),
            Err(CastError::TooLarge)
        );
        assert!(parse_response(b"SSH-2.0-x\r\n\r\n", 1024, true).is_err());
        assert!(parse_response(
            b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nab",
            1024,
            true
        )
        .is_err());
        assert!(parse_response(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n",
            1024,
            true
        )
        .is_err());
    }

    #[test]
    fn request_rejects_header_injection() {
        let url = LanUrl::parse("http://192.168.1.4:80/ctl").unwrap();
        let request = Request {
            method: "POST",
            url: &url,
            headers: &[("SOAPACTION", "\"x\"\r\nEvil: 1")],
            body: b"",
            max_body: 16,
            timeout: Duration::from_secs(1),
        };
        assert_eq!(
            build_request(&request),
            Err(CastError::Malformed("header injection"))
        );
    }

    #[tokio::test]
    async fn round_trips_against_a_local_server_and_times_out() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = match listener.local_addr().unwrap() {
            std::net::SocketAddr::V4(addr) => addr,
            _ => unreachable!(),
        };
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 1024];
            let n = socket.read(&mut request).await.unwrap();
            let text = String::from_utf8_lossy(&request[..n]).to_string();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await
                .unwrap();
            // Hold the connection open: the client must finish on Content-Length alone.
            let (_silent, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
            text
        });
        let url = LanUrl {
            addr,
            path: "/ctl".into(),
        };
        let response = send(Request {
            method: "POST",
            url: &url,
            headers: &[("SOAPACTION", "\"a#b\"")],
            body: b"<x/>",
            max_body: 64,
            timeout: Duration::from_secs(2),
        })
        .await
        .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"ok");
        let timed_out = send(Request {
            method: "GET",
            url: &url,
            headers: &[],
            body: b"",
            max_body: 64,
            timeout: Duration::from_millis(200),
        })
        .await;
        assert_eq!(timed_out, Err(CastError::Timeout));
        server.abort();
    }
}
