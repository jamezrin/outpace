//! Shared fixed-profile audio process outcomes; HLS keeps its existing aliases.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    Unsupported,
    Deadline,
    InputOverflow,
    Admission,
    ChildExit,
    Pipe,
    Pump,
    OutputInvalid,
    OutputStall,
    RecoveryLimit,
}

impl Failure {
    pub fn code(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::Deadline => "deadline",
            Self::InputOverflow => "input_overflow",
            Self::Admission => "admission",
            Self::ChildExit => "child_exit",
            Self::Pipe => "pipe",
            Self::Pump => "pump",
            Self::OutputInvalid => "output_invalid",
            Self::OutputStall => "output_stall",
            Self::RecoveryLimit => "recovery_limit",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Starting,
    Ready,
    Recovering,
    Failed(Failure),
    Stopped,
}

mod playback;
pub(crate) use playback::PlaybackPublisher;
pub use playback::{PlaybackPhase, PlaybackSnapshot, PlaybackStateHandle, PlaybackWaitError};

pub(crate) mod output;
