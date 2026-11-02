//! Descriptor content protection, distinct from transport encryption and signatures.
use crate::{bencode::Bencode, Result, WireError};

/// Protection established from a recognized descriptor's authoritative field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protection {
    Public,
    Protected,
}

/// Apply Outpace's supported-format policy without Python-style truthiness.
/// The caller selects the field from the recognized format's authoritative map.
pub fn classify(value: Option<&Bencode>) -> Result<Protection> {
    match value {
        None | Some(Bencode::Int(0)) => Ok(Protection::Public),
        Some(Bencode::Int(1)) => Ok(Protection::Protected),
        Some(_) => Err(WireError::InvalidProtection),
    }
}
