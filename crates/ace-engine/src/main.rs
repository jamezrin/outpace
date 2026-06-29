//! outpace engine daemon entry point.
//!
//! Scaffolding: the HTTP server + session manager are wired once the live piece path is
//! unblocked (node-identity preimage → unchoke → piece loop). For now this reports the
//! available route surface so the binary builds and runs.

fn main() {
    eprintln!(
        "outpace ace-engine (scaffolding). HTTP route surface ready; \
         live data path pending the node-identity preimage (see docs/protocol/notes/16)."
    );
}
