//! Tiny static HTTP server for serving patched descriptors to swarm peers.
//!
//! Minimal HTTP/1.1: a `GET` for a registered path returns its bytes as
//! `application/octet-stream`; anything else is a `404`. Bound to a configurable
//! address (default `0.0.0.0:7002`). Used in later phases to serve patched
//! `.acelive` descriptors to engine/outpace containers.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

type Files = Arc<Mutex<HashMap<String, Vec<u8>>>>;

/// Handle to a running static HTTP server.
pub struct HttpdHandle {
    /// The actual bound address (useful for ephemeral `:0` binds).
    pub local_addr: SocketAddr,
    files: Files,
    shutdown: Arc<Notify>,
    task: JoinHandle<()>,
}

impl HttpdHandle {
    /// Register (or replace) the bytes served at `path` (e.g. `/stream.acelive`).
    pub fn register(&self, path: impl Into<String>, bytes: Vec<u8>) {
        self.files
            .lock()
            .expect("files mutex")
            .insert(path.into(), bytes);
    }

    /// Base URL of this server, e.g. `http://127.0.0.1:7002`.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.local_addr)
    }

    /// Stop the server task and wait for it to finish.
    pub async fn shutdown(self) {
        self.shutdown.notify_waiters();
        let _ = self.task.await;
    }
}

/// Bind a TCP listener at `addr` and spawn the server task. Returns once bound.
pub async fn start(addr: SocketAddr) -> std::io::Result<HttpdHandle> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let files: Files = Arc::new(Mutex::new(HashMap::new()));
    let shutdown = Arc::new(Notify::new());

    let task = tokio::spawn(serve(listener, Arc::clone(&files), Arc::clone(&shutdown)));

    Ok(HttpdHandle {
        local_addr,
        files,
        shutdown,
        task,
    })
}

async fn serve(listener: TcpListener, files: Files, shutdown: Arc<Notify>) {
    loop {
        let (stream, _) = tokio::select! {
            _ = shutdown.notified() => break,
            r = listener.accept() => match r {
                Ok(v) => v,
                Err(_) => continue,
            },
        };
        let files = Arc::clone(&files);
        tokio::spawn(async move {
            let _ = handle_conn(stream, files).await;
        });
    }
}

async fn handle_conn(mut stream: TcpStream, files: Files) -> std::io::Result<()> {
    // Read until end of request headers (\r\n\r\n) or a sane cap.
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() > 64 * 1024 {
            break;
        }
    }

    let path = parse_request_target(&buf);
    let body = path.and_then(|p| files.lock().expect("files mutex").get(p).cloned());

    match body {
        Some(bytes) => {
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                bytes.len()
            );
            stream.write_all(header.as_bytes()).await?;
            stream.write_all(&bytes).await?;
        }
        None => {
            let body = b"not found";
            let header = format!(
                "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await?;
            stream.write_all(body).await?;
        }
    }
    stream.flush().await?;
    Ok(())
}

/// Extract the request target (path) from a raw HTTP request: the second token of
/// the first line, e.g. `GET /stream.acelive HTTP/1.1`.
fn parse_request_target(buf: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(buf).ok()?;
    let line = text.lines().next()?;
    let mut parts = line.split_whitespace();
    let _method = parts.next()?;
    parts.next()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn serves_registered_blob_and_404s_unknown() {
        let httpd = start("127.0.0.1:0".parse().unwrap()).await.unwrap();
        httpd.register("/stream.acelive", b"hello-descriptor".to_vec());
        let base = httpd.base_url();

        let client = reqwest::Client::new();
        let ok = client
            .get(format!("{base}/stream.acelive"))
            .send()
            .await
            .unwrap();
        assert_eq!(ok.status(), reqwest::StatusCode::OK);
        assert_eq!(
            ok.headers().get(reqwest::header::CONTENT_TYPE).unwrap(),
            "application/octet-stream"
        );
        assert_eq!(ok.bytes().await.unwrap().as_ref(), b"hello-descriptor");

        let missing = client.get(format!("{base}/nope")).send().await.unwrap();
        assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);

        httpd.shutdown().await;
    }
}
