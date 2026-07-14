//! AceStream engine artifact acquisition.
//!
//! The closed-source engine (3.2.11) is never committed. This module resolves the
//! engine binaries in priority order:
//!
//!   1. `--engine-dir <path>` if it already contains `acestreamengine` → use as-is.
//!   2. the cache dir `~/.cache/outpace-swarmtest/engine-3.2.11/` if populated → use.
//!   3. otherwise download `--engine-url`, verify SHA-256 against [`ENGINE_SHA256`],
//!      and extract into the cache dir.
//!
//! A `--engine-dir` override or an already-populated cache is trusted by location:
//! the pinned SHA-256 only guards the fresh Download path (1) and (2) are assumed
//! vetted by whoever placed the binaries there.
//!
//! ## Pinned hash / placeholder
//!
//! [`ENGINE_SHA256`] is a pinned SHA-256 of the official tarball and is enforced on
//! the download path. If AceStream rotates the artifact the download fails loudly;
//! run `swarmtest verify-engine-hash` to recompute it and update the constant. As an
//! escape hatch, resetting the constant to the [`ENGINE_SHA256_PLACEHOLDER`] sentinel
//! makes the download path log a prominent warning and SKIP verification instead.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use sha2::{Digest, Sha256};

/// Engine version this harness targets.
pub const ENGINE_VERSION: &str = "3.2.11";

/// Name of the engine executable inside the tarball / engine dir.
pub const ENGINE_BINARY_NAME: &str = "acestreamengine";

/// Sentinel value meaning "the real hash has not been recorded yet".
pub const ENGINE_SHA256_PLACEHOLDER: &str = "UNPINNED";

/// Pinned SHA-256 (lowercase hex, 64 chars) of the official engine tarball.
///
/// Pinned from `swarmtest verify-engine-hash` against
/// `acestream_3.2.11_ubuntu_22.04_x86_64_py3.10.tar.gz` (confirmed twice via
/// independent downloads). If AceStream rotates the artifact this will fail the
/// download path loudly; re-run `verify-engine-hash` and update this constant, or
/// pass `--engine-dir`/`--engine-url` to override. Reset to
/// [`ENGINE_SHA256_PLACEHOLDER`] to intentionally skip verification.
pub const ENGINE_SHA256: &str = "9b6bbd76a55e5a434641afae3b9cf8e6154ce1cf392152ec3aed5ac265432b2e";

/// Where the engine binaries should be resolved from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineSource {
    /// Use the caller-provided directory verbatim.
    UseDir(PathBuf),
    /// Use the already-populated cache directory.
    UseCache(PathBuf),
    /// Download + verify + extract into the cache directory.
    Download,
}

/// Pure priority decision for where to obtain the engine.
///
/// * `engine_dir` / `engine_dir_has_binary` — the `--engine-dir` override and
///   whether it actually contains [`ENGINE_BINARY_NAME`].
/// * `cache_dir` / `cache_has_binary` — the cache directory and whether it is
///   already populated.
///
/// No I/O: the caller probes the filesystem and passes the booleans, keeping the
/// priority logic unit-testable without a network or disk.
pub fn resolve_engine_source(
    engine_dir: Option<&Path>,
    engine_dir_has_binary: bool,
    cache_dir: &Path,
    cache_has_binary: bool,
) -> EngineSource {
    if let Some(dir) = engine_dir {
        if engine_dir_has_binary {
            return EngineSource::UseDir(dir.to_path_buf());
        }
    }
    if cache_has_binary {
        return EngineSource::UseCache(cache_dir.to_path_buf());
    }
    EngineSource::Download
}

/// The cache directory for the pinned engine version.
pub fn cache_dir() -> Result<PathBuf> {
    let base = dirs::cache_dir().ok_or_else(|| anyhow!("could not resolve a cache directory"))?;
    Ok(base
        .join("outpace-swarmtest")
        .join(format!("engine-{ENGINE_VERSION}")))
}

/// True iff `expected` is the not-yet-pinned placeholder sentinel.
pub fn is_placeholder(expected: &str) -> bool {
    expected == ENGINE_SHA256_PLACEHOLDER
}

/// Verify the SHA-256 of `bytes` equals `expected` (case-insensitive 64-hex).
///
/// Pure and network-free. Returns an error if `expected` is not a valid 64-char
/// hex digest or if the computed digest does not match. Callers that want the
/// placeholder-skip behaviour must check [`is_placeholder`] first.
pub fn verify_sha256(bytes: &[u8], expected: &str) -> Result<()> {
    if expected.len() != 64 || !expected.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("expected SHA-256 must be 64 hex characters, got {expected:?}");
    }
    let got = hex::encode(Sha256::digest(bytes));
    if got.eq_ignore_ascii_case(expected) {
        Ok(())
    } else {
        Err(anyhow!(
            "SHA-256 mismatch: expected {}, computed {got}",
            expected.to_ascii_lowercase()
        ))
    }
}

/// Compute the lowercase-hex SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Recursively locate [`ENGINE_BINARY_NAME`] under `dir`, if present.
pub fn find_engine_binary(dir: &Path) -> Option<PathBuf> {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(cur) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&cur) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().and_then(|n| n.to_str()) == Some(ENGINE_BINARY_NAME) {
                return Some(path);
            }
        }
    }
    None
}

/// Download the tarball at `url` and return its computed SHA-256 (lowercase hex).
///
/// Backs the `swarmtest verify-engine-hash` subcommand. Not unit-tested (network).
pub async fn download_and_hash(url: &str) -> Result<String> {
    let bytes = download_bytes(url).await?;
    Ok(sha256_hex(&bytes))
}

/// Acquire the engine binary, returning the path to `acestreamengine`.
///
/// Thin async wrapper over the pure [`resolve_engine_source`] decision plus
/// [`verify_sha256`] / [`find_engine_binary`]. The download+extract path is not
/// unit-tested; it is exercised by the live Phase 2 run.
pub async fn acquire_engine(engine_dir: Option<&Path>, engine_url: &str) -> Result<PathBuf> {
    let cache = cache_dir()?;
    let engine_dir_has_binary = engine_dir
        .map(|d| find_engine_binary(d).is_some())
        .unwrap_or(false);
    let cache_has_binary = find_engine_binary(&cache).is_some();

    match resolve_engine_source(engine_dir, engine_dir_has_binary, &cache, cache_has_binary) {
        EngineSource::UseDir(dir) => find_engine_binary(&dir)
            .ok_or_else(|| anyhow!("{ENGINE_BINARY_NAME} not found in {}", dir.display())),
        EngineSource::UseCache(dir) => find_engine_binary(&dir)
            .ok_or_else(|| anyhow!("{ENGINE_BINARY_NAME} not found in cache {}", dir.display())),
        EngineSource::Download => {
            let bytes = download_bytes(engine_url).await?;
            if is_placeholder(ENGINE_SHA256) {
                eprintln!(
                    "WARNING: engine SHA-256 is unpinned ({ENGINE_SHA256_PLACEHOLDER}); \
                     SKIPPING integrity verification. Run `swarmtest verify-engine-hash` \
                     and pin ENGINE_SHA256 in engine.rs. Computed hash: {}",
                    sha256_hex(&bytes)
                );
            } else {
                verify_sha256(&bytes, ENGINE_SHA256)
                    .context("engine tarball failed SHA-256 verification")?;
            }
            extract_into_cache_atomic(&bytes, &cache)
                .with_context(|| format!("extracting engine into {}", cache.display()))?;
            find_engine_binary(&cache).ok_or_else(|| {
                anyhow!(
                    "{ENGINE_BINARY_NAME} not found after extracting into {}",
                    cache.display()
                )
            })
        }
    }
}

/// Download `url` fully into memory. Bounded by a connect + overall timeout so a stalled or
/// half-open connection can't hang engine acquisition indefinitely.
async fn download_bytes(url: &str) -> Result<Vec<u8>> {
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(30))
        .timeout(std::time::Duration::from_secs(600))
        .build()
        .context("building download client")?;
    let resp = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?
        .error_for_status()
        .with_context(|| format!("bad status for {url}"))?;
    let bytes = resp.bytes().await.context("reading response body")?;
    Ok(bytes.to_vec())
}

/// Extract a gzip-compressed tar archive into `dest`.
fn extract_tar_gz(bytes: &[u8], dest: &Path) -> Result<()> {
    let decoder = flate2::read::GzDecoder::new(bytes);
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(dest).context("unpacking tar.gz")?;
    Ok(())
}

/// Extract the tarball into a temp sibling dir, then atomically `rename` it into
/// `cache`. A partial or failed extract never poisons the final cache path: work
/// happens in `<parent>/.engine-<ver>.tmp-<rand>` and only a fully-extracted dir is
/// moved into place; the temp dir is removed on any error. So a later run's
/// `UseCache` detection only ever sees a fully-extracted directory.
fn extract_into_cache_atomic(bytes: &[u8], cache: &Path) -> Result<()> {
    let parent = cache
        .parent()
        .ok_or_else(|| anyhow!("cache dir {} has no parent", cache.display()))?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("creating cache parent {}", parent.display()))?;

    let tmp = parent.join(format!(
        ".engine-{ENGINE_VERSION}.tmp-{}",
        rand::random::<u32>()
    ));
    // Clean any stale temp dir from a previously-crashed run.
    let _ = std::fs::remove_dir_all(&tmp);

    let extracted = (|| -> Result<()> {
        std::fs::create_dir_all(&tmp)
            .with_context(|| format!("creating temp extract dir {}", tmp.display()))?;
        extract_tar_gz(bytes, &tmp)
    })();
    if let Err(e) = extracted {
        let _ = std::fs::remove_dir_all(&tmp);
        return Err(e);
    }

    // Replace any partially-populated final dir, then atomically move the temp in.
    let _ = std::fs::remove_dir_all(cache);
    if let Err(e) = std::fs::rename(&tmp, cache) {
        let _ = std::fs::remove_dir_all(&tmp);
        return Err(anyhow!(
            "renaming {} -> {}: {e}",
            tmp.display(),
            cache.display()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_sha256_matches_known_vector() {
        // SHA-256("") is well known.
        let empty_sha = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert!(verify_sha256(b"", empty_sha).is_ok());
        // Case-insensitive.
        assert!(verify_sha256(b"", &empty_sha.to_ascii_uppercase()).is_ok());
    }

    #[test]
    fn verify_sha256_rejects_mismatch() {
        let wrong = "0".repeat(64);
        assert!(verify_sha256(b"hello", &wrong).is_err());
    }

    #[test]
    fn verify_sha256_rejects_malformed_expected() {
        assert!(verify_sha256(b"x", "not-hex").is_err());
        assert!(verify_sha256(b"x", "abc").is_err());
    }

    #[test]
    fn placeholder_is_detected() {
        assert!(is_placeholder(ENGINE_SHA256_PLACEHOLDER));
        // ENGINE_SHA256 is pinned to a real digest, so it must NOT read as the placeholder
        // and must be a well-formed 64-char lowercase hex hash the download path can enforce.
        assert!(!is_placeholder(ENGINE_SHA256));
        assert_eq!(ENGINE_SHA256.len(), 64);
        assert!(ENGINE_SHA256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
        assert!(!is_placeholder(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        ));
    }

    #[test]
    fn resolve_prefers_engine_dir_then_cache_then_download() {
        let dir = Path::new("/opt/engine");
        let cache = Path::new("/cache/engine");

        // engine_dir present + populated => UseDir
        assert_eq!(
            resolve_engine_source(Some(dir), true, cache, true),
            EngineSource::UseDir(dir.to_path_buf())
        );
        // engine_dir present but empty, cache populated => UseCache
        assert_eq!(
            resolve_engine_source(Some(dir), false, cache, true),
            EngineSource::UseCache(cache.to_path_buf())
        );
        // engine_dir present but empty, cache empty => Download
        assert_eq!(
            resolve_engine_source(Some(dir), false, cache, false),
            EngineSource::Download
        );
        // no engine_dir, cache populated => UseCache
        assert_eq!(
            resolve_engine_source(None, false, cache, true),
            EngineSource::UseCache(cache.to_path_buf())
        );
        // no engine_dir, cache empty => Download
        assert_eq!(
            resolve_engine_source(None, false, cache, false),
            EngineSource::Download
        );
    }

    #[test]
    fn find_engine_binary_walks_subdirs() {
        let tmp = tempfile::tempdir().unwrap();
        let nested = tmp.path().join("acestream.engine").join("lib");
        std::fs::create_dir_all(&nested).unwrap();
        let bin = nested.join(ENGINE_BINARY_NAME);
        std::fs::write(&bin, b"#!/bin/sh\n").unwrap();
        assert_eq!(find_engine_binary(tmp.path()), Some(bin));
    }

    #[test]
    fn find_engine_binary_absent_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("other"), b"x").unwrap();
        assert_eq!(find_engine_binary(tmp.path()), None);
    }

    /// Build an in-memory `.tar.gz` from `(name, bytes)` entries.
    fn make_targz(entries: &[(&str, &[u8])]) -> Vec<u8> {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        let mut gz = GzEncoder::new(Vec::new(), Compression::fast());
        {
            let mut ar = tar::Builder::new(&mut gz);
            for (name, data) in entries {
                let mut header = tar::Header::new_gnu();
                header.set_size(data.len() as u64);
                header.set_mode(0o755);
                header.set_cksum();
                ar.append_data(&mut header, name, *data).unwrap();
            }
            ar.finish().unwrap();
        }
        gz.finish().unwrap()
    }

    #[test]
    fn extract_into_cache_atomic_populates_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = tmp.path().join("engine-3.2.11");
        let targz = make_targz(&[("acestream/acestreamengine", b"#!/bin/sh\n")]);

        extract_into_cache_atomic(&targz, &cache).unwrap();
        assert!(find_engine_binary(&cache).is_some());
        // No leftover temp sibling dirs.
        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with(".engine-"))
            .collect();
        assert!(leftovers.is_empty(), "temp extract dir must be gone");
    }

    #[test]
    fn extract_into_cache_atomic_failure_leaves_no_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = tmp.path().join("engine-3.2.11");
        // Not a valid gzip stream => extraction fails.
        assert!(extract_into_cache_atomic(b"not a tarball", &cache).is_err());
        // The final cache dir must NOT exist (so a later run re-downloads, not UseCache).
        assert!(!cache.exists(), "poisoned cache must not be left behind");
        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with(".engine-"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "temp extract dir must be cleaned up on failure"
        );
    }
}
