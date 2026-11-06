//! Fixed worker wire contract. Parsing alone never authorizes a lookup.
mod input;
mod isolation;
pub use isolation::{attest_offline, validate_snapshot};
mod record;
pub use input::{parse_environment, parse_role_args, Request, Role};
pub use record::encode_record;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerError {
    Input,
    Namespace,
    Output,
}
impl WorkerError {
    pub fn token(self) -> &'static str {
        match self {
            Self::Input => "input_error",
            Self::Namespace => "namespace_denied",
            Self::Output => "output_error",
        }
    }
}
impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.token())
    }
}
impl std::error::Error for WorkerError {}

/// Fixed offline entry. Environment acquisition and runtime construction happen
/// only after actual namespace admission; parsing cannot authorize networking.
pub fn entry<I, E, V>(args: I, environment: E, output: &mut impl std::io::Write) -> i32
where
    I: IntoIterator<Item = std::ffi::OsString>,
    E: FnOnce() -> V,
    V: IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
{
    let Ok(role) = parse_role_args(args) else {
        return 64;
    };
    if attest_offline().is_err() {
        return 65;
    }
    let Ok(request) = parse_environment(role, environment()) else {
        return 64;
    };
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return 70;
    };
    let candidate = match role {
        Role::Mainline => crate::candidates::Candidate::Mainline,
        Role::N0 => crate::candidates::Candidate::N0,
    };
    let record = runtime.block_on(crate::candidates::run(candidate, request.input()));
    let Ok(bytes) = encode_record(role, &record) else {
        return 74;
    };
    if output
        .write_all(&bytes)
        .and_then(|()| output.flush())
        .is_err()
    {
        return 74;
    }
    0
}
