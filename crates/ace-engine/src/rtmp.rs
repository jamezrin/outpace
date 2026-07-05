//! RTMP broadcast ingest listener and handler.

use crate::broadcast_ingest::BroadcastIngest;
use crate::http::BroadcastState;
use crate::rtmp_ts::RtmpTsMuxer;
use rtmp_rs::media::{AacData, H264Data};
use rtmp_rs::protocol::message::{ConnectParams, PublishParams};
use rtmp_rs::server::handler::{AuthResult, MediaDeliveryMode, RtmpHandler};
use rtmp_rs::session::{SessionContext, StreamContext};
use rtmp_rs::{RtmpServer, ServerConfig};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Mutex;

pub async fn serve_rtmp(
    bind: SocketAddr,
    broadcasts: BroadcastState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = ServerConfig::with_addr(bind).disable_gop_buffer();
    let server = RtmpServer::new(config, RtmpIngestHandler::new(broadcasts));
    server.run().await?;
    Ok(())
}

pub struct RtmpIngestHandler {
    broadcasts: BroadcastState,
    streams: Mutex<BTreeMap<String, RtmpStreamIngest>>,
}

struct RtmpStreamIngest {
    ingest: BroadcastIngest,
    muxer: RtmpTsMuxer,
}

impl RtmpIngestHandler {
    pub fn new(broadcasts: BroadcastState) -> Self {
        Self {
            broadcasts,
            streams: Mutex::new(BTreeMap::new()),
        }
    }

    async fn push_media<F>(&self, stream_key: &str, mux: F)
    where
        F: FnOnce(&mut RtmpTsMuxer) -> Vec<u8>,
    {
        let stream = self.streams.lock().unwrap().remove(stream_key);
        let Some(mut stream) = stream else {
            return;
        };
        let bytes = mux(&mut stream.muxer);
        if bytes.is_empty() {
            self.streams
                .lock()
                .unwrap()
                .insert(stream_key.to_string(), stream);
            return;
        }
        stream.ingest.push_bytes(&bytes).await;
        self.streams
            .lock()
            .unwrap()
            .insert(stream_key.to_string(), stream);
    }
}

impl RtmpHandler for RtmpIngestHandler {
    async fn on_connect(&self, _ctx: &SessionContext, params: &ConnectParams) -> AuthResult {
        if params.app == "live" {
            AuthResult::Accept
        } else {
            AuthResult::Reject("unsupported RTMP app; use /live".to_string())
        }
    }

    async fn on_publish(&self, _ctx: &SessionContext, params: &PublishParams) -> AuthResult {
        let name = params.stream_key.trim();
        if name.is_empty() {
            return AuthResult::Reject("empty stream key".to_string());
        }

        let (bc, freshly_minted) = self
            .broadcasts
            .registry
            .start_or_resume(
                name,
                name,
                &self.broadcasts.trackers,
                &self.broadcasts.seed_registry,
                self.broadcasts.store_bytes,
            )
            .await;
        if freshly_minted {
            self.broadcasts.spawn_announce(&bc);
        }

        let ingest = BroadcastIngest::new(bc.store.clone(), bc.auth.clone(), bc.cursor.clone());
        self.streams.lock().unwrap().insert(
            name.to_string(),
            RtmpStreamIngest {
                ingest,
                muxer: RtmpTsMuxer::new(),
            },
        );
        AuthResult::Accept
    }

    async fn on_video_frame(&self, ctx: &StreamContext, frame: &H264Data, timestamp: u32) {
        self.push_media(&ctx.stream_key, |muxer| muxer.push_video(frame, timestamp))
            .await;
    }

    async fn on_audio_frame(&self, ctx: &StreamContext, frame: &AacData, timestamp: u32) {
        self.push_media(&ctx.stream_key, |muxer| muxer.push_audio(frame, timestamp))
            .await;
    }

    async fn on_unpublish(&self, ctx: &StreamContext) {
        let stream = self.streams.lock().unwrap().remove(&ctx.stream_key);
        if let Some(mut stream) = stream {
            stream.ingest.finish().await;
        }
    }

    fn media_delivery_mode(&self) -> MediaDeliveryMode {
        MediaDeliveryMode::ParsedFrames
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::BroadcastState;
    use rtmp_rs::protocol::message::{ConnectParams, PublishParams};
    use rtmp_rs::server::handler::{AuthResult, RtmpHandler};
    use rtmp_rs::session::SessionContext;
    use std::process::Stdio;
    use tokio::process::Command;

    fn state() -> BroadcastState {
        BroadcastState {
            registry: crate::broadcast::BroadcastRegistry::new(),
            seed_registry: ace_swarm::listen::SeedRegistry::new(),
            trackers: vec![],
            store_bytes: 4 << 20,
            inbound_peer_port: None,
        }
    }

    fn session() -> SessionContext {
        SessionContext::new(1, "127.0.0.1:40000".parse().unwrap())
    }

    #[tokio::test]
    async fn connect_accepts_only_live_app() {
        let handler = RtmpIngestHandler::new(state());
        assert!(matches!(
            handler
                .on_connect(
                    &session(),
                    &ConnectParams {
                        app: "live".to_string(),
                        ..Default::default()
                    },
                )
                .await,
            AuthResult::Accept
        ));
        assert!(matches!(
            handler
                .on_connect(
                    &session(),
                    &ConnectParams {
                        app: "vod".to_string(),
                        ..Default::default()
                    },
                )
                .await,
            AuthResult::Reject(_)
        ));
    }

    #[tokio::test]
    async fn publish_mints_stream_key_broadcast() {
        let bs = state();
        let registry = bs.registry.clone();
        let handler = RtmpIngestHandler::new(bs);
        let result = handler
            .on_publish(
                &session(),
                &PublishParams {
                    stream_key: "mychan".to_string(),
                    publish_type: "live".to_string(),
                    stream_id: 1,
                },
            )
            .await;

        assert!(matches!(result, AuthResult::Accept));
        assert!(
            registry.get("mychan").await.is_some(),
            "RTMP publish must mint/resume the broadcast named by stream key"
        );
    }

    async fn wait_for_tcp(addr: std::net::SocketAddr) {
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("RTMP server did not start on {addr}");
    }

    #[tokio::test]
    async fn rtmp_publish_reaches_broadcast_piece_store() {
        let bs = state();
        let registry = bs.registry.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let server = tokio::spawn(async move {
            let _ = serve_rtmp(addr, bs).await;
        });
        wait_for_tcp(addr).await;

        let output = format!("rtmp://{addr}/live/loop");
        let status = Command::new("ffmpeg")
            .args([
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc=size=128x96:rate=10",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=1000:sample_rate=44100",
                "-t",
                "2",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-g",
                "10",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                "-f",
                "flv",
            ])
            .arg(output)
            .stdin(Stdio::null())
            .status()
            .await
            .expect("ffmpeg is installed for RTMP loopback test");
        assert!(status.success(), "ffmpeg RTMP publish failed: {status}");

        let bc = registry.get("loop").await.expect("broadcast was minted");
        for _ in 0..100 {
            if bc.store.lock().await.chunk(0, 0).is_some() {
                server.abort();
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        server.abort();
        panic!("RTMP publish did not reach broadcast piece store");
    }
}
