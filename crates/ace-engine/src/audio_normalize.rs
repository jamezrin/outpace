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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Starting,
    Ready,
    Recovering,
    Failed(Failure),
    Stopped,
}
