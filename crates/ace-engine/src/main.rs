//! outpace daemon entry point.

// outpace uses jemalloc as its global allocator on 64-bit non-MSVC targets. The default system
// allocator (glibc) never returns the pages freed after a stream ends to the OS, so RSS climbs to
// the playback high-water mark and stays there even with zero clients — the "memory leak" symptom
// that is really allocator retention. jemalloc with a background purge thread and short decay hands
// freed pages back, so idle RSS falls after teardown. 32-bit targets (e.g. armv7) keep the system
// allocator: jemalloc's 64-bit `ffs` helper pulls in a `__ffsdi2` builtin that fails to link
// against static 32-bit musl. Windows/MSVC also keeps the system allocator. `/debug/memstats` reads
// jemalloc's live-heap vs resident counters on the jemalloc path.
#[cfg(all(not(target_env = "msvc"), target_pointer_width = "64"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Purge dirty pages ~1s after they go idle and return muzzy pages immediately, with a background
// thread doing it even while the process is otherwise idle. This is what makes RSS drop back toward
// the live set after a stream tears down.
#[cfg(all(not(target_env = "msvc"), target_pointer_width = "64"))]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:0\0";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ace_engine::cli::run().await
}
