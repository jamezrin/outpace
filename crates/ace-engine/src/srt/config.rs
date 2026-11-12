//! Strict, default-off SRT listener policy. Secret contents never enter serde or Debug.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct SrtConfig {
    pub bind: Option<SocketAddr>,
    pub passphrase_file: Option<PathBuf>,
    pub latency_ms: u32,
    pub max_connections: usize,
}
impl Default for SrtConfig {
    fn default() -> Self {
        Self {
            bind: None,
            passphrase_file: None,
            latency_ms: 120,
            max_connections: 4,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
    FeatureUnavailable,
    InvalidBounds,
    InvalidBind,
    InvalidEnvironment,
    MissingSecret,
    UnsafeSecret,
    InvalidSecret,
    UnsupportedPlatform,
}
impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::FeatureUnavailable => "srt_feature_unavailable",
            Self::InvalidBounds => "srt_invalid_bounds",
            Self::InvalidBind => "srt_invalid_bind",
            Self::InvalidEnvironment => "srt_invalid_environment",
            Self::MissingSecret => "srt_passphrase_file_required",
            Self::UnsafeSecret => "srt_unsafe_passphrase_file",
            Self::InvalidSecret => "srt_invalid_passphrase",
            Self::UnsupportedPlatform => "srt_unsupported_platform",
        })
    }
}
impl std::error::Error for ConfigError {}

pub struct Passphrase(Vec<u8>);
impl Passphrase {
    /// Borrow only for passing the bounded secret to the native transport.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
impl std::fmt::Debug for Passphrase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Passphrase([redacted])")
    }
}

#[derive(Debug)]
pub struct PreparedConfig {
    pub bind: SocketAddr,
    pub latency_ms: u32,
    pub max_connections: usize,
    pub passphrase: Passphrase,
}
impl SrtConfig {
    /// Parse only the declared SRT settings; this never resolves DNS or reads keys.
    pub fn from_lookup(
        mut lookup: impl FnMut(&str) -> Option<std::ffi::OsString>,
    ) -> Result<Self, ConfigError> {
        let mut config = Self::default();
        if let Some(value) = lookup("OUTPACE_SRT_BIND") {
            config.bind = Some(
                value
                    .to_str()
                    .ok_or(ConfigError::InvalidBind)?
                    .parse()
                    .map_err(|_| ConfigError::InvalidBind)?,
            );
        }
        if let Some(value) = lookup("OUTPACE_SRT_PASSPHRASE_FILE") {
            if value.is_empty() {
                return Err(ConfigError::MissingSecret);
            }
            config.passphrase_file = Some(value.into());
        }
        if let Some(value) = lookup("OUTPACE_SRT_LATENCY_MS") {
            config.latency_ms = value
                .to_str()
                .ok_or(ConfigError::InvalidEnvironment)?
                .parse()
                .map_err(|_| ConfigError::InvalidEnvironment)?;
        }
        if let Some(value) = lookup("OUTPACE_SRT_MAX_CONNECTIONS") {
            config.max_connections = value
                .to_str()
                .ok_or(ConfigError::InvalidEnvironment)?
                .parse()
                .map_err(|_| ConfigError::InvalidEnvironment)?;
        }
        if !(20..=2000).contains(&config.latency_ms) || !(1..=16).contains(&config.max_connections)
        {
            return Err(ConfigError::InvalidBounds);
        }
        Ok(config)
    }

    /// Validate policy and load the secret only for enabled listeners. This
    /// prepares values; it neither initializes libsrt nor opens a listener.
    pub fn prepare(&self, native_available: bool) -> Result<Option<PreparedConfig>, ConfigError> {
        let Some(bind) = self.bind else {
            return Ok(None);
        };
        if !native_available {
            return Err(ConfigError::FeatureUnavailable);
        }
        if !(20..=2000).contains(&self.latency_ms) || !(1..=16).contains(&self.max_connections) {
            return Err(ConfigError::InvalidBounds);
        }
        let path = self
            .passphrase_file
            .as_ref()
            .ok_or(ConfigError::MissingSecret)?;
        let passphrase = read_passphrase(path)?;
        Ok(Some(PreparedConfig {
            bind,
            latency_ms: self.latency_ms,
            max_connections: self.max_connections,
            passphrase,
        }))
    }
}

#[cfg(unix)]
fn read_passphrase(path: &std::path::Path) -> Result<Passphrase, ConfigError> {
    use std::io::Read;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    // O_NONBLOCK prevents a malicious FIFO from blocking before fstat rejects it;
    // all policy checks apply to the descriptor actually read, not a path stat.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| ConfigError::UnsafeSecret)?;
    let metadata = file.metadata().map_err(|_| ConfigError::UnsafeSecret)?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        return Err(ConfigError::UnsafeSecret);
    }
    let mut bytes = Vec::with_capacity(82);
    file.take(82)
        .read_to_end(&mut bytes)
        .map_err(|_| ConfigError::UnsafeSecret)?;
    // Permit one conventional terminal newline; never trim internal bytes/spaces.
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    if !(10..=79).contains(&bytes.len()) || bytes.iter().any(|b| matches!(b, 0 | b'\n' | b'\r')) {
        return Err(ConfigError::InvalidSecret);
    }
    Ok(Passphrase(bytes))
}
#[cfg(not(unix))]
fn read_passphrase(_path: &std::path::Path) -> Result<Passphrase, ConfigError> {
    Err(ConfigError::UnsupportedPlatform)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn disabled_srt_never_opens_a_secret_or_requires_native_support() {
        let config = SrtConfig {
            passphrase_file: Some(PathBuf::from("/missing/secret")),
            ..SrtConfig::default()
        };
        assert!(matches!(config.prepare(false), Ok(None)));
    }

    #[test]
    fn enabled_srt_requires_feature_before_reading_secret() {
        let config = SrtConfig {
            bind: Some("127.0.0.1:9000".parse().unwrap()),
            ..SrtConfig::default()
        };
        assert!(matches!(
            config.prepare(false),
            Err(ConfigError::FeatureUnavailable)
        ));
    }

    #[test]
    fn enabled_srt_rejects_out_of_bounds_before_reading_secret() {
        for (latency_ms, max_connections) in [(19, 4), (2001, 4), (120, 0), (120, 17)] {
            let config = SrtConfig {
                bind: Some("127.0.0.1:9000".parse().unwrap()),
                latency_ms,
                max_connections,
                ..SrtConfig::default()
            };
            assert!(matches!(
                config.prepare(true),
                Err(ConfigError::InvalidBounds)
            ));
        }
    }

    #[test]
    fn environment_policy_requires_numeric_bind_and_strict_bounds() {
        let parse = |pairs: &[(&str, &str)]| {
            SrtConfig::from_lookup(|key| {
                pairs
                    .iter()
                    .find(|(name, _)| *name == key)
                    .map(|(_, value)| std::ffi::OsString::from(value))
            })
        };
        let disabled = parse(&[]).unwrap();
        assert!(disabled.bind.is_none());
        assert_eq!(disabled.latency_ms, 120);
        assert_eq!(disabled.max_connections, 4);
        let enabled = parse(&[
            ("OUTPACE_SRT_BIND", "[::1]:9000"),
            ("OUTPACE_SRT_LATENCY_MS", "200"),
            ("OUTPACE_SRT_MAX_CONNECTIONS", "2"),
            ("OUTPACE_SRT_PASSPHRASE_FILE", "/private/key"),
        ])
        .unwrap();
        assert_eq!(
            enabled.bind.unwrap(),
            "[::1]:9000".parse::<std::net::SocketAddr>().unwrap()
        );
        assert_eq!((enabled.latency_ms, enabled.max_connections), (200, 2));
        for bind in ["", "localhost:9000", "9000", "127.0.0.1:99999"] {
            assert!(parse(&[("OUTPACE_SRT_BIND", bind)]).is_err());
        }
        for (key, value) in [
            ("OUTPACE_SRT_LATENCY_MS", "nan"),
            ("OUTPACE_SRT_LATENCY_MS", "19"),
            ("OUTPACE_SRT_MAX_CONNECTIONS", "0"),
            ("OUTPACE_SRT_MAX_CONNECTIONS", "17"),
        ] {
            assert!(parse(&[(key, value)]).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn fifo_secret_is_rejected_without_blocking_and_child_is_reaped() {
        use std::os::unix::ffi::OsStrExt;
        if let Some(path) = std::env::var_os("OUTPACE_TEST_SRT_FIFO") {
            let config = SrtConfig {
                bind: Some("127.0.0.1:9000".parse().unwrap()),
                passphrase_file: Some(path.into()),
                ..SrtConfig::default()
            };
            assert!(matches!(
                config.prepare(true),
                Err(ConfigError::UnsafeSecret)
            ));
            return;
        }
        let directory = std::env::temp_dir().join(format!(
            "outpace-srt-fifo-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("fifo");
        let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "srt::config::tests::fifo_secret_is_rejected_without_blocking_and_child_is_reaped",
                "--exact",
            ])
            .env("OUTPACE_TEST_SRT_FIFO", &path)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break Some(status);
            }
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                break None;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        std::fs::remove_dir_all(directory).unwrap();
        assert!(
            status.is_some_and(|s| s.success()),
            "FIFO validation must complete before the deadline; owned child reaped"
        );
    }

    #[cfg(unix)]
    #[test]
    fn secret_file_is_bounded_regular_private_and_never_printed() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let directory = std::env::temp_dir().join(format!(
            "outpace-srt-config-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir(&directory).unwrap();
        let file = directory.join("passphrase");
        let secret = b"synthetic-test-passphrase";
        std::fs::write(&file, secret).unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut config = SrtConfig {
            bind: Some("127.0.0.1:9000".parse().unwrap()),
            passphrase_file: Some(file.clone()),
            ..SrtConfig::default()
        };
        let prepared = config.prepare(true).unwrap().unwrap();
        assert_eq!(prepared.passphrase.as_bytes(), secret);
        assert!(!format!("{prepared:?}").contains("synthetic-test-passphrase"));
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            config.prepare(true),
            Err(ConfigError::UnsafeSecret)
        ));
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        for bytes in [vec![b'x'; 9], vec![b'x'; 80], b"secret-with\0nul".to_vec()] {
            std::fs::write(&file, bytes).unwrap();
            assert!(matches!(
                config.prepare(true),
                Err(ConfigError::InvalidSecret)
            ));
        }
        let link = directory.join("link");
        symlink(&file, &link).unwrap();
        config.passphrase_file = Some(link);
        assert!(matches!(
            config.prepare(true),
            Err(ConfigError::UnsafeSecret)
        ));
        config.passphrase_file = Some(directory.clone());
        assert!(matches!(
            config.prepare(true),
            Err(ConfigError::UnsafeSecret)
        ));
        std::fs::remove_dir_all(directory).unwrap();
    }
}
