use super::WorkerError;
use crate::WorkerInput;
use std::{collections::HashMap, ffi::OsString};
const PREFIX: &str = "OUTPACE_DHT_COMPARE_";
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Mainline,
    N0,
}
impl Role {
    pub fn name(self) -> &'static str {
        match self {
            Self::Mainline => "mainline",
            Self::N0 => "n0",
        }
    }
}
#[derive(Debug)]
pub struct Request {
    role: Role,
    input: WorkerInput,
}
impl Request {
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn input(&self) -> &WorkerInput {
        &self.input
    }
}
/// Accept only fixed operation/role arguments, with no identity-bearing argv.
pub fn parse_role_args(args: impl IntoIterator<Item = OsString>) -> Result<Role, WorkerError> {
    let mut args = args.into_iter();
    if args.next().as_deref() != Some(std::ffi::OsStr::new("--offline-worker")) {
        return Err(WorkerError::Input);
    }
    let role = match args.next().as_deref().and_then(std::ffi::OsStr::to_str) {
        Some("mainline") => Role::Mainline,
        Some("n0") => Role::N0,
        _ => return Err(WorkerError::Input),
    };
    if args.next().is_some() {
        return Err(WorkerError::Input);
    }
    Ok(role)
}
/// Parse only the four comparison fields. Namespace admission is separate.
pub fn parse_environment(
    role: Role,
    entries: impl IntoIterator<Item = (OsString, OsString)>,
) -> Result<Request, WorkerError> {
    let mut values = HashMap::with_capacity(4);
    for (key, value) in entries {
        if !key.as_encoded_bytes().starts_with(PREFIX.as_bytes()) {
            continue;
        }
        let key = key.to_str().ok_or(WorkerError::Input)?;
        let field = key.strip_prefix(PREFIX).ok_or(WorkerError::Input)?;
        if !matches!(field, "MODE" | "ROLE" | "TARGET" | "SEEDS") {
            return Err(WorkerError::Input);
        }
        let value = value.to_str().ok_or(WorkerError::Input)?;
        let limit = match field {
            "TARGET" => 40,
            "SEEDS" => 256,
            _ => 16,
        };
        if value.len() > limit || values.insert(field.to_owned(), value.to_owned()).is_some() {
            return Err(WorkerError::Input);
        }
    }
    if values.len() != 4
        || values.get("MODE").map(String::as_str) != Some("offline-v1")
        || values.get("ROLE").map(String::as_str) != Some(role.name())
    {
        return Err(WorkerError::Input);
    }
    let input =
        WorkerInput::parse(&values["TARGET"], &values["SEEDS"]).map_err(|_| WorkerError::Input)?;
    if input.seeds.iter().any(|seed| !seed.ip().is_loopback()) {
        return Err(WorkerError::Input);
    }
    Ok(Request { role, input })
}
