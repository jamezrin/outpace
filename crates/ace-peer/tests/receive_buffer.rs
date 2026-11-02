//! Deterministic receive/cancel contracts. Synthetic streams only; no network.
use ace_peer::session::PeerSession;
use ace_peer::PeerError;
use ace_wire::message::{PeerMessage, MAX_FRAME_LEN};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Default)]
struct Input {
    bytes: Vec<u8>,
    consumed: usize,
    read_limits: Vec<usize>,
    pending_reads: usize,
    eof: bool,
    error: bool,
    waker: Option<Waker>,
}

#[derive(Clone, Default)]
struct Feed(Arc<Mutex<Input>>);

impl Feed {
    fn append(&self, bytes: &[u8]) {
        let mut input = self.0.lock().unwrap();
        input.bytes.extend_from_slice(bytes);
        if let Some(waker) = input.waker.take() {
            waker.wake();
        }
    }
}

struct ScriptedIo(Feed);

impl AsyncRead for ScriptedIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        target: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut input = self.get_mut().0 .0.lock().unwrap();
        input.read_limits.push(target.remaining());
        if input.error {
            return Poll::Ready(Err(std::io::ErrorKind::ConnectionReset.into()));
        }
        let available = input.bytes.len() - input.consumed;
        if available != 0 {
            let count = available.min(target.remaining());
            target.put_slice(&input.bytes[input.consumed..input.consumed + count]);
            input.consumed += count;
            return Poll::Ready(Ok(()));
        }
        if input.eof {
            return Poll::Ready(Ok(()));
        }
        input.pending_reads += 1;
        input.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl AsyncWrite for ScriptedIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn message() -> PeerMessage {
    PeerMessage::Unknown {
        id: 250,
        payload: b"synthetic receive payload".to_vec(),
    }
}

#[tokio::test]
async fn partial_frame_survives_command_selection_at_every_split() {
    let expected = message();
    let wire = expected.encode();
    for split in 1..wire.len() {
        let feed = Feed::default();
        feed.append(&wire[..split]);
        let mut session = PeerSession::new(ScriptedIo(feed.clone()));
        {
            let mut read = std::pin::pin!(session.read_message());
            poll_fn(|cx| {
                assert!(read.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            // Witness bytes consumed before cancellation, rather than a future
            // dropped before its first poll or a scheduling-dependent sleep.
            {
                let input = feed.0.lock().unwrap();
                assert_eq!(input.consumed, split);
                assert_eq!(input.pending_reads, 1);
            }
            // A ready command branch competes with the already-pending read.
            tokio::select! {
                biased;
                command = std::future::ready(7_u8) => assert_eq!(command, 7),
                _ = &mut read => panic!("partial frame unexpectedly completed"),
            }
        }
        feed.append(&wire[split..]);
        assert_eq!(session.read_message().await.unwrap(), expected);
        assert_eq!(feed.0.lock().unwrap().consumed, wire.len());
    }
}

#[tokio::test]
async fn coalesced_frames_preserve_tail_without_another_read() {
    let messages = [message(), PeerMessage::KeepAlive, message()];
    let wire: Vec<_> = messages.iter().flat_map(PeerMessage::encode).collect();
    let feed = Feed::default();
    feed.append(&wire);
    let mut session = PeerSession::new(ScriptedIo(feed.clone()));
    assert_eq!(session.read_message().await.unwrap(), messages[0]);
    let polls = feed.0.lock().unwrap().read_limits.len();
    for expected in &messages[1..] {
        assert_eq!(&session.read_message().await.unwrap(), expected);
    }
    let input = feed.0.lock().unwrap();
    assert_eq!(input.read_limits.len(), polls);
    assert_eq!(input.consumed, wire.len());
}

#[tokio::test(start_paused = true)]
async fn timeout_preserves_partial_body_for_resumption() {
    let expected = message();
    let wire = expected.encode();
    let feed = Feed::default();
    feed.append(&wire[..7]);
    let mut session =
        PeerSession::new(ScriptedIo(feed.clone())).with_timeout(Duration::from_secs(5));
    assert!(matches!(
        session.read_message().await,
        Err(PeerError::Timeout)
    ));
    assert_eq!(feed.0.lock().unwrap().consumed, 7);
    feed.append(&wire[7..]);
    assert_eq!(session.read_message().await.unwrap(), expected);
}

#[tokio::test]
async fn maximum_frame_body_respects_underlying_read_ceiling() {
    let expected = PeerMessage::Unknown {
        id: 250,
        payload: vec![17; MAX_FRAME_LEN - 1],
    };
    let wire = expected.encode();
    assert_eq!(wire.len(), MAX_FRAME_LEN + 4);
    let feed = Feed::default();
    feed.append(&wire);
    let mut session = PeerSession::new(ScriptedIo(feed.clone()));
    assert_eq!(session.read_message().await.unwrap(), expected);
    let input = feed.0.lock().unwrap();
    assert_eq!(input.consumed, wire.len());
    assert!(input.read_limits.iter().all(|limit| *limit <= 16 * 1024));
}

#[tokio::test]
async fn oversized_declaration_rejects_without_further_body_reads() {
    let feed = Feed::default();
    feed.append(&((MAX_FRAME_LEN as u32) + 1).to_be_bytes());
    let mut session = PeerSession::new(ScriptedIo(feed.clone()));
    assert!(matches!(
        session.read_message().await,
        Err(PeerError::Wire(_))
    ));
    let input = feed.0.lock().unwrap();
    assert_eq!(input.consumed, 4);
    assert_eq!(input.read_limits.len(), 1);
}

#[tokio::test]
async fn eof_and_io_error_are_distinct_after_partial_input() {
    for error in [false, true] {
        let feed = Feed::default();
        feed.append(&message().encode()[..3]);
        let mut session = PeerSession::new(ScriptedIo(feed.clone()));
        {
            let mut read = std::pin::pin!(session.read_message());
            poll_fn(|cx| {
                assert!(read.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        {
            let mut input = feed.0.lock().unwrap();
            assert_eq!(input.consumed, 3);
            input.error = error;
            input.eof = !error;
        }
        let result = session.read_message().await;
        if error {
            assert!(matches!(result, Err(PeerError::Io(_))));
        } else {
            assert!(matches!(result, Err(PeerError::Closed)));
        }
    }
}
