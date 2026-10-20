use super::*;
use crate::config::LiveRecoveryConfig;

mod announce;
mod provider;
mod source;
mod vod;
fn default_live_recovery() -> LiveRecoveryConfig {
    LiveRecoveryConfig::default()
}

fn info() -> StreamInfo {
    StreamInfo {
        infohash: [0; 20],
        piece_length: 4,
        chunk_length: 2,
        trackers: vec![],
        metadata: Default::default(),
        sig_len: 0,
        source_pubkey: vec![],
    }
}
