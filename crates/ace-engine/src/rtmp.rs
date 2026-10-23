//! RTMP broadcast ingress. Dependency-owned sockets may outlive listener shutdown;
//! publication authority is fenced by publishing session and the broadcast generation.

use crate::broadcast::{cancelled, ProducerKind, ProducerSession};
use crate::broadcast_ingest::BroadcastIngest;
use crate::http::BroadcastState;
use crate::rtmp_ts::RtmpTsMuxer;
use rtmp_rs::media::{AacData, H264Data};
use rtmp_rs::protocol::message::{ConnectParams, PublishParams};
use rtmp_rs::server::handler::{AuthResult, MediaDeliveryMode, RtmpHandler};
use rtmp_rs::session::{SessionContext, StreamContext};
use rtmp_rs::{RtmpServer, ServerConfig};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::{watch, Mutex as AsyncMutex};

pub async fn serve_rtmp(
    bind: SocketAddr,
    broadcasts: BroadcastState,
    mut cancel: watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = ServerConfig::with_addr(bind).disable_gop_buffer();
    let server = RtmpServer::new(config, RtmpIngestHandler::new(broadcasts));
    server
        .run_until(async move { cancelled(&mut cancel).await })
        .await?;
    Ok(())
}

pub struct RtmpIngestHandler {
    broadcasts: BroadcastState,
    streams: Mutex<BTreeMap<String, Arc<RtmpStreamIngest>>>,
    // A canceled publishing connection cannot acquire a later generation for that name:
    // callbacks carry connection/stream IDs, not an independently echoable generation token.
    // Ordinary unpublish is reusable. Tombstones last only until connection disconnect.
    retired: Mutex<BTreeSet<(u64, String)>>,
    #[cfg(test)]
    operation_hook: Mutex<Option<OperationHook>>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RtmpOperation {
    Mux,
    Ingest,
    Finish,
}
#[cfg(test)]
type OperationHook = Arc<
    dyn Fn(RtmpOperation) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

struct RtmpStreamIngest {
    session_id: u64,
    stream_id: u32,
    generation: u64,
    cancel: watch::Receiver<bool>,
    generation_cancel: watch::Receiver<bool>,
    operation: Arc<AsyncMutex<()>>,
    data: AsyncMutex<Option<RtmpProducer>>,
}
struct RtmpProducer {
    session: ProducerSession,
    ingest: BroadcastIngest,
    muxer: RtmpTsMuxer,
}

impl RtmpIngestHandler {
    pub fn new(broadcasts: BroadcastState) -> Self {
        Self {
            broadcasts,
            streams: Mutex::new(BTreeMap::new()),
            retired: Mutex::new(BTreeSet::new()),
            #[cfg(test)]
            operation_hook: Mutex::new(None),
        }
    }
    #[cfg(test)]
    pub(crate) fn set_operation_hook(&self, hook: OperationHook) {
        *self.operation_hook.lock().unwrap() = Some(hook);
    }
    #[cfg(test)]
    async fn before_operation(&self, operation: RtmpOperation) {
        let hook = self.operation_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(operation).await;
        }
    }
    fn find(&self, ctx: &StreamContext) -> Option<Arc<RtmpStreamIngest>> {
        self.streams
            .lock()
            .unwrap()
            .get(&ctx.stream_key)
            .filter(|stream| {
                stream.session_id == ctx.session.session_id && stream.stream_id == ctx.stream_id
            })
            .cloned()
    }
    fn retire(&self, name: &str, stream: &RtmpStreamIngest) {
        self.retired
            .lock()
            .unwrap()
            .insert((stream.session_id, name.to_owned()));
    }
    fn remove_current(&self, name: &str, stream: &Arc<RtmpStreamIngest>) {
        let mut map = self.streams.lock().unwrap();
        if map
            .get(name)
            .is_some_and(|current| Arc::ptr_eq(current, stream))
        {
            map.remove(name);
        }
    }
    async fn push_media<F>(&self, ctx: &StreamContext, mux: F)
    where
        F: FnOnce(&mut RtmpTsMuxer) -> Vec<u8>,
    {
        let Some(stream) = self.find(ctx) else {
            return;
        };
        let _operation = stream.operation.lock().await;
        let mut data = stream.data.lock().await;
        let Some(producer) = data.as_mut() else {
            return;
        };
        if producer.session.generation() != stream.generation || producer.session.check().is_err() {
            self.retire(&ctx.stream_key, &stream);
            data.take();
            self.remove_current(&ctx.stream_key, &stream);
            return;
        }
        #[cfg(test)]
        self.before_operation(RtmpOperation::Mux).await;
        if producer.session.check().is_err() {
            return;
        }
        let bytes = mux(&mut producer.muxer);
        if bytes.is_empty() {
            return;
        }
        #[cfg(test)]
        self.before_operation(RtmpOperation::Ingest).await;
        if let Err(error) = producer.ingest.push_bytes(&bytes).await {
            crate::alog!("[rtmp] {}: {}", ctx.stream_key, error.code());
            self.retire(&ctx.stream_key, &stream);
            data.take();
            self.remove_current(&ctx.stream_key, &stream);
        }
    }
}

impl RtmpHandler for RtmpIngestHandler {
    async fn on_connect(&self, _ctx: &SessionContext, params: &ConnectParams) -> AuthResult {
        if params.app == "live" {
            AuthResult::Accept
        } else {
            AuthResult::Reject("unsupported app; use live".to_string())
        }
    }

    async fn on_publish(&self, ctx: &SessionContext, params: &PublishParams) -> AuthResult {
        let name = &params.stream_key;
        if !crate::broadcast::valid_broadcast_name(name) {
            return AuthResult::Reject("invalid_name".into());
        }
        if self.broadcasts.registry.is_service_stopping() {
            return AuthResult::Reject("broadcast_service_stopping".into());
        }
        if self
            .retired
            .lock()
            .unwrap()
            .contains(&(ctx.session_id, name.clone()))
        {
            return AuthResult::Reject("broadcast_stopping".into());
        }
        {
            let map = self.streams.lock().unwrap();
            if let Some(old) = map.get(name).filter(|old| *old.cancel.borrow()) {
                self.retire(name, old);
                if old.session_id == ctx.session_id {
                    return AuthResult::Reject("broadcast_stopping".into());
                }
            }
        }
        let (_, mut session) = match self
            .broadcasts
            .acquire_producer(name, ProducerKind::Rtmp)
            .await
        {
            Ok(pair) => pair,
            Err(error) => return AuthResult::Reject(error.code().into()),
        };
        let ingest = match session.open_ingest() {
            Ok(ingest) => ingest,
            Err(error) => return AuthResult::Reject(error.code().into()),
        };
        let stream = Arc::new(RtmpStreamIngest {
            session_id: ctx.session_id,
            stream_id: params.stream_id,
            generation: session.generation(),
            cancel: session.cancellation(),
            generation_cancel: session.generation_cancellation(),
            operation: session.operation_barrier(),
            data: AsyncMutex::new(Some(RtmpProducer {
                session,
                ingest,
                muxer: RtmpTsMuxer::new(),
            })),
        });
        let mut map = self.streams.lock().unwrap();
        if *stream.cancel.borrow() {
            return AuthResult::Reject("broadcast_stopping".into());
        }
        if let Some(old) = map.get(name) {
            if !*old.cancel.borrow() {
                return AuthResult::Reject("producer_conflict".into());
            }
            self.retire(name, old);
            if old.session_id == ctx.session_id {
                return AuthResult::Reject("broadcast_stopping".into());
            }
        }
        map.insert(name.clone(), stream);
        AuthResult::Accept
    }

    async fn on_video_frame(&self, ctx: &StreamContext, frame: &H264Data, timestamp: u32) {
        self.push_media(ctx, |muxer| muxer.push_video(frame, timestamp))
            .await;
    }
    async fn on_audio_frame(&self, ctx: &StreamContext, frame: &AacData, timestamp: u32) {
        self.push_media(ctx, |muxer| muxer.push_audio(frame, timestamp))
            .await;
    }
    async fn on_unpublish(&self, ctx: &StreamContext) {
        let Some(stream) = self.find(ctx) else {
            return;
        };
        let _operation = stream.operation.lock().await;
        let mut data = stream.data.lock().await;
        if let Some(mut producer) = data.take() {
            if producer.session.check().is_err() {
                self.retire(&ctx.stream_key, &stream);
            }
            #[cfg(test)]
            self.before_operation(RtmpOperation::Finish).await;
            if producer.session.generation() == stream.generation
                && producer.session.check().is_ok()
            {
                if let Err(error) = producer.ingest.finish().await {
                    crate::alog!("[rtmp] {}: {}", ctx.stream_key, error.code());
                    self.retire(&ctx.stream_key, &stream);
                }
            } else {
                self.retire(&ctx.stream_key, &stream);
            }
        }
        // Unlike producer cancellation on ordinary Drop, this signal identifies deletion
        // racing the final successful unpublish/lease release.
        if *stream.generation_cancel.borrow() {
            self.retire(&ctx.stream_key, &stream);
        }
        self.remove_current(&ctx.stream_key, &stream);
    }
    async fn on_disconnect(&self, ctx: &SessionContext) {
        let streams: Vec<_> = self
            .streams
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, stream)| stream.session_id == ctx.session_id)
            .map(|(name, stream)| (name.clone(), stream.clone()))
            .collect();
        for (name, stream) in streams {
            let _operation = stream.operation.lock().await;
            // A broken connection discards its partial attempt; only unpublish seals a
            // finite tail. The session drop cancels every outstanding authorized operation.
            stream.data.lock().await.take();
            self.remove_current(&name, &stream);
        }
        self.retired
            .lock()
            .unwrap()
            .retain(|(session_id, _)| *session_id != ctx.session_id);
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
            inbound_peer_port: tokio::sync::watch::channel(None).1,
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
        let ffmpeg_available = Command::new("ffmpeg")
            .arg("-version")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .is_ok_and(|status| status.success());
        if !ffmpeg_available {
            eprintln!("skipping RTMP loopback smoke: target-executable ffmpeg is unavailable");
            return;
        }

        let bs = state();
        let registry = bs.registry.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(async move {
            let _ = serve_rtmp(addr, bs, cancel_rx).await;
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
            .expect("ffmpeg preflight succeeded for RTMP loopback test");
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

#[cfg(test)]
mod tests_pull206;
