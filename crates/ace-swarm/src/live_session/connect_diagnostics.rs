//! Address-free phase accounting for completed upstream connection attempts.
use super::{PeerConnectFailure, PeerConnectStage};
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

/// Durations include partial work in the terminal phase. Unreached phases are absent,
/// rather than zero, so they cannot lower the median. Zero-duration samples are valid.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PeerConnectTiming {
    pub(super) phase: ConnectPhase,
    pub(super) durations: [Option<Duration>; 4],
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
        parts.join(" ")
    }
}

/// Owned outside the total-timeout future so cancellation preserves the current phase.
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
}

impl PeerConnectStats {
    fn record_timing(&mut self, timing: PeerConnectTiming) {
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
        parts.join(" ")
    }
}
