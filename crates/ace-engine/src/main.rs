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

// Purge dirty pages ~1s after they go idle and return muzzy pages immediately. On Linux a
// background thread does it even while the process is otherwise idle, which is what makes RSS drop
// back toward the live set after a stream tears down.
#[cfg(all(not(target_env = "msvc"), target_pointer_width = "64"))]
mod allocator_config {
    // jemalloc excludes Mach-O from its background-thread implementation, so asking for one on
    // Apple only prints `option background_thread currently supports pthread only` at startup.
    // Decay there is driven by allocator activity instead of an idle worker.
    #[cfg(not(target_vendor = "apple"))]
    const CONFIG: &std::ffi::CStr = c"background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:0";
    #[cfg(target_vendor = "apple")]
    const CONFIG: &std::ffi::CStr = c"background_thread:false,dirty_decay_ms:1000,muzzy_decay_ms:0";

    // jemalloc declares `extern const char *malloc_conf`: this must be a thin C pointer. A Rust
    // `&[u8]` is a (pointer, length) pair that only worked by accident of field order.
    #[repr(transparent)]
    struct MallocConf(*const std::ffi::c_char);

    // SAFETY: the pointer refers to immutable, NUL-terminated static bytes for the process lifetime.
    unsafe impl Sync for MallocConf {}

    // tikv-jemalloc-sys keeps prefixed (`_rjem_`) symbols on these targets (its
    // NO_UNPREFIXED_MALLOC_TARGETS), so jemalloc reads `_rjem_malloc_conf` there; exporting plain
    // `malloc_conf` would be silently ignored.
    #[used]
    #[cfg_attr(
        any(
            target_vendor = "apple",
            target_os = "android",
            target_os = "dragonfly"
        ),
        export_name = "_rjem_malloc_conf"
    )]
    #[cfg_attr(
        not(any(
            target_vendor = "apple",
            target_os = "android",
            target_os = "dragonfly"
        )),
        export_name = "malloc_conf"
    )]
    static MALLOC_CONF: MallocConf = MallocConf(CONFIG.as_ptr());

    #[cfg(test)]
    mod tests {
        // These read the allocator's effective options, not the Rust string, so a wrong symbol
        // name or ABI (which leaves jemalloc on its defaults) fails here. They assume no
        // MALLOC_CONF / _RJEM_MALLOC_CONF override in the test environment.
        #[test]
        fn background_thread_matches_platform_support() {
            let expected = cfg!(not(target_vendor = "apple"));
            assert_eq!(
                tikv_jemalloc_ctl::opt::background_thread::read().unwrap(),
                expected
            );
        }

        #[test]
        fn decay_settings_come_from_the_exported_configuration() {
            // SAFETY: both mallctl options are documented as `ssize_t`; the keys are NUL-terminated.
            let (dirty, muzzy) = unsafe {
                (
                    tikv_jemalloc_ctl::raw::read::<isize>(b"opt.dirty_decay_ms\0").unwrap(),
                    tikv_jemalloc_ctl::raw::read::<isize>(b"opt.muzzy_decay_ms\0").unwrap(),
                )
            };
            assert_eq!((dirty, muzzy), (1000, 0));
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ace_engine::cli::run().await
}
