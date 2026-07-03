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
            announce_broadcast(&self.broadcasts, &bc);
        }

        let ingest = BroadcastIngest::new(bc.store.clone(), bc.auth.clone());
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

fn announce_broadcast(bs: &BroadcastState, bc: &crate::broadcast::Broadcast) {
    if let Some(port) = bs.inbound_peer_port {
        let trackers = bs.trackers.clone();
        tokio::spawn(crate::ace_provider::announce_infohash_periodically(
            trackers.clone(),
            bc.infohash,
            port,
        ));
        tokio::spawn(crate::ace_provider::announce_infohash_periodically(
            trackers,
            bc.content_id,
            port,
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::BroadcastState;
    use rtmp_rs::protocol::message::{ConnectParams, PublishParams};
    use rtmp_rs::server::handler::{AuthResult, RtmpHandler};
    use rtmp_rs::session::SessionContext;

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
}
