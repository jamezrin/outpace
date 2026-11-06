use super::{Role, WorkerError};
use crate::candidates::{Record, Terminal};
/// Encode one numeric-only, versioned record; reject inconsistent source records.
pub fn encode_record(role: Role, record: &Record) -> Result<Vec<u8>, WorkerError> {
    if record.peer_count > 1024
        || record.batch_sizes.len() != record.batches_observed.min(64)
        || record.batch_history_truncated != (record.batches_observed > 64)
        || record.target_overshoot != record.peer_count.saturating_sub(8)
        || record.first_peer.is_some() != (record.peer_count > 0)
        || record.first_peer.is_some_and(|time| time > record.elapsed)
        || record.elapsed.as_secs_f64() > 30.0
        || (record.terminal == Terminal::Target && record.peer_count < 8)
        || (matches!(record.terminal, Terminal::Deadline | Terminal::Exhausted)
            && record.peer_count >= 8)
        || (record.peer_count > 0 && record.batches_observed == 0)
        || (!record.batch_history_truncated
            && record.batch_sizes.iter().map(|n| *n as u128).sum::<u128>()
                < record.peer_count as u128)
    {
        return Err(WorkerError::Output);
    }
    let terminal = match record.terminal {
        Terminal::Target => "target",
        Terminal::Exhausted => "exhausted",
        Terminal::Deadline => "deadline",
        Terminal::AdapterError => "adapter_error",
    };
    let value = serde_json::json!({
        "schema_version":1,"candidate":role.name(),"terminal":terminal,
        "peer_count":record.peer_count,"first_peer_seconds":record.first_peer.map(|t|t.as_secs_f64()),
        "elapsed_seconds":record.elapsed.as_secs_f64(),"batch_sizes":record.batch_sizes,
        "batches_observed":record.batches_observed,"batch_history_truncated":record.batch_history_truncated,
        "target_overshoot":record.target_overshoot,"nodes_queried":null,"valid_responses":null,
        "diagnostics_unavailable_reason":"library_not_exposed"
    });
    let mut bytes = serde_json::to_vec(&value).map_err(|_| WorkerError::Output)?;
    if bytes.len() > 8192 {
        return Err(WorkerError::Output);
    }
    bytes.push(b'\n');
    Ok(bytes)
}
