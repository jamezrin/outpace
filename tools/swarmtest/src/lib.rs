//! `swarmtest` — interop test harness for the outpace AceStream reimplementation.
//!
//! This library crate holds the Phase 1 foundation modules; the `swarmtest` binary
//! (`src/main.rs`) is a thin CLI over them. Phase 2+ adds docker orchestration and
//! scenario assertions on top of these building blocks.
//!
//! * [`config`] — resolved run configuration + per-run output directory.
//! * [`engine`] — AceStream engine artifact acquisition (resolve/verify/extract).
//! * [`tracker`] — in-process BEP-15 UDP tracker with an announce journal.
//! * [`httpd`] — tiny static HTTP server for patched descriptors.
//! * [`transport`] — `.acelive` descriptor tracker patcher + infohash helper.
//! * [`compose`] — pure per-scenario `docker-compose.yaml` generation.
//! * [`assertions`] — pure, tested swarm-health / stream-stability / TS verdicts.
//! * [`peers`] — engine/outpace consumer drivers + testable response parsing.
//! * [`scenario`] — docker orchestration state machine (glue over the pure pieces).
//! * [`report`] — scenario reports, stdout table, and `report.json`.

use std::path::PathBuf;

pub mod assertions;
pub mod compose;
pub mod config;
pub mod engine;
pub mod httpd;
pub mod peers;
pub mod report;
pub mod scenario;
pub mod tracker;
pub mod transport;

/// Absolute path to a committed asset under `tools/swarmtest/assets/`.
pub fn asset_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("assets")
        .join(name)
}
