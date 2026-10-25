//! Address-free phase accounting for completed upstream connection attempts.
use super::{PeerConnectFailure, PeerConnectStage};
use ace_peer::session::HandshakeProgress;
use ace_peer::PeerError;
use ace_wire::handshake::HANDSHAKE_LEN;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConnectPhase {
    #[default]
    Connect,
    HandshakeSend,
    HandshakeReply,
    Window,
}

impl ConnectPhase {
    const ALL: [Self; 4] = [
        Self::Connect,
        Self::HandshakeSend,
        Self::HandshakeReply,
        Self::Window,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::HandshakeSend => "handshake_send",
            Self::HandshakeReply => "handshake_reply",
            Self::Window => "window",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BasicReplyCause {
    Valid,
    Timeout,
    Eof,
    Reset,
    IoOther,
    Decode,
    HashMismatch,
}

impl BasicReplyCause {
    const ALL: [Self; 7] = [
        Self::Valid,
        Self::Timeout,
        Self::Eof,
        Self::Reset,
        Self::IoOther,
        Self::Decode,
        Self::HashMismatch,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Valid => "valid",
            Self::Timeout => "timeout",
            Self::Eof => "eof",
            Self::Reset => "reset",
            Self::IoOther => "io_other",
            Self::Decode => "decode",
            Self::HashMismatch => "hash_mismatch",
        }
    }

    pub(super) fn from_error(error: &PeerError) -> Self {
        match error {
            PeerError::Timeout => Self::Timeout,
            PeerError::Io(error) => match error.kind() {
                std::io::ErrorKind::UnexpectedEof => Self::Eof,
                std::io::ErrorKind::ConnectionReset => Self::Reset,
                _ => Self::IoOther,
            },
            PeerError::Wire(_) | PeerError::Protocol(_) => Self::Decode,
            PeerError::InfohashMismatch => Self::HashMismatch,
            PeerError::Closed => Self::Eof,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplyByteState {
    Zero,
    Partial,
    Complete,
}

impl ReplyByteState {
    const ALL: [Self; 3] = [Self::Zero, Self::Partial, Self::Complete];

    fn name(self) -> &'static str {
        match self {
            Self::Zero => "zero",
            Self::Partial => "partial",
            Self::Complete => "complete",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BasicReplyProgress {
    // Bounded by the fixed ACE reply buffer; observe_handshake also caps the snapshot.
    pub(super) received: usize,
    // Pending only during the exchange; completed paths supply a typed cause.
    pub(super) cause: Option<BasicReplyCause>,
}

impl BasicReplyProgress {
    fn byte_state(self) -> ReplyByteState {
        match self.received {
            0 => ReplyByteState::Zero,
            HANDSHAKE_LEN => ReplyByteState::Complete,
            _ => ReplyByteState::Partial,
        }
    }
}

/// Durations include partial work in the terminal phase. Unreached phases are absent,
/// rather than zero, so they cannot lower the median. Zero-duration samples are valid.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PeerConnectTiming {
    pub(super) phase: ConnectPhase,
    pub(super) durations: [Option<Duration>; 4],
    pub(super) reply: Option<BasicReplyProgress>,
}

impl PeerConnectTiming {
    pub(super) fn summary(&self) -> String {
        let mut parts = vec![format!("phase={}", self.phase.name())];
        for (phase, duration) in ConnectPhase::ALL.into_iter().zip(self.durations) {
            if let Some(duration) = duration {
                parts.push(format!("{}_ms={}", phase.name(), duration.as_millis()));
            }
        }
        let total: Duration = self.durations.iter().flatten().copied().sum();
        parts.push(format!("total_ms={}", total.as_millis()));
        if let Some(reply) = self.reply {
            if let Some(cause) = reply.cause {
                parts.push(format!(
                    "reply_bytes={} reply_state={} reply_cause={}",
                    reply.received,
                    reply.byte_state().name(),
                    cause.name()
                ));
            }
        }
        parts.join(" ")
    }
}

/// Owned outside the total-timeout future so cancellation preserves phase and reply bytes.
pub(super) struct PeerConnectProgress {
    timing: PeerConnectTiming,
    phase_started: Instant,
}

impl PeerConnectProgress {
    pub(super) fn new() -> Self {
        Self {
            timing: PeerConnectTiming::default(),
            phase_started: Instant::now(),
        }
    }

    pub(super) fn advance(&mut self, phase: ConnectPhase) {
        let now = Instant::now();
        self.timing.durations[self.timing.phase as usize] = Some(now - self.phase_started);
        self.phase_started = now;
        self.timing.phase = phase;
    }

    pub(super) fn observe_handshake(&mut self, event: HandshakeProgress) {
        match event {
            HandshakeProgress::Sent => {
                self.timing.reply = Some(BasicReplyProgress {
                    received: 0,
                    cause: None,
                });
                self.advance(ConnectPhase::HandshakeReply);
            }
            HandshakeProgress::ReplyBytes { received } => {
                if let Some(reply) = &mut self.timing.reply {
                    reply.received = received.min(HANDSHAKE_LEN);
                }
            }
        }
    }

    pub(super) fn finish_handshake(&mut self, cause: BasicReplyCause) {
        if let Some(reply) = &mut self.timing.reply {
            reply.cause = Some(cause);
        }
    }

    pub(super) fn reply_timeout(&mut self) {
        if self.timing.phase == ConnectPhase::HandshakeReply {
            if let Some(reply) = &mut self.timing.reply {
                if reply.cause.is_none() {
                    reply.cause = Some(BasicReplyCause::Timeout);
                }
            }
        }
    }

    pub(super) fn finish(mut self) -> PeerConnectTiming {
        self.timing.durations[self.timing.phase as usize] = Some(self.phase_started.elapsed());
        self.timing
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct PeerConnectStats {
    pub(super) connected: usize,
    pub(super) tcp: usize,
    pub(super) handshake: usize,
    pub(super) window: usize,
    pub(super) total_timeout: usize,
    pub(super) task: usize,
    timeout_phases: [usize; 4],
    durations: [Vec<Duration>; 4],
    replies: [[usize; 3]; 7],
}

impl PeerConnectStats {
    fn record_timing(&mut self, timing: PeerConnectTiming) {
        if let Some(reply) = timing.reply {
            if let Some(cause) = reply.cause {
                self.replies[cause as usize][reply.byte_state() as usize] += 1;
            }
        }
        for (samples, duration) in self.durations.iter_mut().zip(timing.durations) {
            if let Some(duration) = duration {
                samples.push(duration);
            }
        }
    }

    pub(super) fn record_connected(&mut self, timing: PeerConnectTiming) {
        self.connected += 1;
        self.record_timing(timing);
    }

    pub(super) fn record_failure(&mut self, failure: PeerConnectFailure) {
        self.record_timing(failure.timing);
        match failure.stage {
            PeerConnectStage::Tcp => self.tcp += 1,
            PeerConnectStage::Handshake => self.handshake += 1,
            PeerConnectStage::Window => self.window += 1,
            PeerConnectStage::TotalTimeout => {
                self.total_timeout += 1;
                self.timeout_phases[failure.timing.phase as usize] += 1;
            }
        }
    }

    pub(super) fn record_task_failure(&mut self) {
        self.task += 1;
    }

    pub(super) fn has_observations(&self) -> bool {
        self.connected + self.tcp + self.handshake + self.window + self.total_timeout + self.task
            > 0
    }

    pub(super) fn summary(&self) -> String {
        let attempted = self.connected
            + self.tcp
            + self.handshake
            + self.window
            + self.total_timeout
            + self.task;
        let mut parts = vec![format!("attempted={attempted}")];
        if self.connected > 0 {
            parts.push(format!("connected={}", self.connected));
        }
        if self.tcp > 0 {
            parts.push(format!("tcp={}", self.tcp));
        }
        if self.handshake > 0 {
            parts.push(format!("handshake={}", self.handshake));
        }
        if self.window > 0 {
            parts.push(format!("window={}", self.window));
        }
        if self.total_timeout > 0 {
            parts.push(format!("total_timeout={}", self.total_timeout));
        }
        if self.task > 0 {
            parts.push(format!("task={}", self.task));
        }
        for phase in ConnectPhase::ALL {
            let index = phase as usize;
            let name = phase.name();
            if self.timeout_phases[index] > 0 {
                parts.push(format!("timeout_{name}={}", self.timeout_phases[index]));
            }
            let mut durations = self.durations[index].clone();
            if durations.is_empty() {
                continue;
            }
            durations.sort_unstable();
            let mid = durations.len() / 2;
            let median = if durations.len().is_multiple_of(2) {
                durations[mid - 1] + (durations[mid] - durations[mid - 1]) / 2
            } else {
                durations[mid]
            };
            parts.push(format!("{name}_ms_p50={}", median.as_millis()));
            parts.push(format!(
                "{name}_ms_max={}",
                durations.last().unwrap().as_millis()
            ));
        }
        for cause in BasicReplyCause::ALL {
            for state in ReplyByteState::ALL {
                let count = self.replies[cause as usize][state as usize];
                if count > 0 {
                    parts.push(format!("reply_{}_{}={count}", cause.name(), state.name()));
                }
            }
        }
        parts.join(" ")
    }
}
