//! Operator-admitted immutable metadata; request aliases never select filesystem paths.

use ace_swarm::types::{StreamInfo, VodInfo};
use serde::de::{MapAccess, Visitor};
use std::{collections::BTreeMap, fmt, path::PathBuf, sync::Arc};

const MAX_ENTRIES: usize = 64;
const MAX_CONFIG_BYTES: usize = 64 * 1024;
const MAX_DESCRIPTOR_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub enum LocalDescriptor {
    Live(StreamInfo),
    Vod(VodInfo),
}

#[derive(Clone, Default)]
pub struct LocalDescriptors(Arc<BTreeMap<String, LocalDescriptor>>);
impl fmt::Debug for LocalDescriptors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalDescriptors")
            .field("entries", &self.0.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalDescriptorError {
    Configuration,
    File,
    Metadata,
    ProtectedContent,
    InvalidProtection,
}
impl fmt::Display for LocalDescriptorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Configuration => "invalid_local_descriptor_config",
            Self::File => "unsafe_or_unreadable_local_descriptor",
            Self::Metadata => "invalid_local_descriptor",
            Self::ProtectedContent => "encrypted content is not supported",
            Self::InvalidProtection => "invalid content protection metadata",
        })
    }
}
impl std::error::Error for LocalDescriptorError {}

pub(crate) fn alias_valid(alias: &str) -> bool {
    !alias.is_empty()
        && alias.len() <= 64
        && alias
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}
struct Paths;
impl<'de> Visitor<'de> for Paths {
    type Value = BTreeMap<String, PathBuf>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a bounded local descriptor map")
    }
    fn visit_map<M: MapAccess<'de>>(self, mut access: M) -> Result<Self::Value, M::Error> {
        let mut entries = BTreeMap::new();
        while let Some((alias, path)) = access.next_entry::<String, PathBuf>()? {
            if entries.len() >= MAX_ENTRIES
                || !alias_valid(&alias)
                || !path.is_absolute()
                || path.as_os_str().as_encoded_bytes().len() > 4096
                || entries.contains_key(&alias)
            {
                return Err(serde::de::Error::custom("invalid local descriptor map"));
            }
            entries.insert(alias, path);
        }
        Ok(entries)
    }
}

/// Operator declarations only. Deserialization never opens a file.
#[derive(Clone, Default)]
pub struct DescriptorPaths(Arc<BTreeMap<String, PathBuf>>);
impl fmt::Debug for DescriptorPaths {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DescriptorPaths")
            .field("entries", &self.0.len())
            .finish()
    }
}
impl serde::Serialize for DescriptorPaths {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(self.0.as_ref(), serializer)
    }
}
impl<'de> serde::Deserialize<'de> for DescriptorPaths {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        serde::Deserializer::deserialize_map(decoder, Paths).map(|paths| Self(Arc::new(paths)))
    }
}
impl DescriptorPaths {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn from_config(raw: Option<&str>) -> Result<Self, LocalDescriptorError> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        if raw.len() > MAX_CONFIG_BYTES {
            return Err(LocalDescriptorError::Configuration);
        }
        serde_json::from_str(raw).map_err(|_| LocalDescriptorError::Configuration)
    }
    pub fn load(&self) -> Result<LocalDescriptors, LocalDescriptorError> {
        let mut admitted = BTreeMap::new();
        for (alias, path) in self.0.iter() {
            let bytes = read_descriptor(path)?;
            admitted.insert(alias.clone(), decode_descriptor(&bytes)?);
        }
        Ok(LocalDescriptors(Arc::new(admitted)))
    }
}

impl LocalDescriptors {
    /// Read operator-selected files once. This must run before any network workers start.
    pub fn from_config(raw: Option<&str>) -> Result<Self, LocalDescriptorError> {
        DescriptorPaths::from_config(raw)?.load()
    }

    #[cfg(test)]
    pub(crate) fn from_test_bytes(alias: &str, bytes: &[u8]) -> Result<Self, LocalDescriptorError> {
        if !alias_valid(alias) || bytes.len() > MAX_DESCRIPTOR_BYTES {
            return Err(LocalDescriptorError::Configuration);
        }
        Ok(Self(Arc::new(BTreeMap::from([(
            alias.to_owned(),
            decode_descriptor(bytes)?,
        )]))))
    }

    pub fn get(&self, id: &str) -> Option<&LocalDescriptor> {
        let alias = id.strip_prefix("local:")?;
        alias_valid(alias).then(|| self.0.get(alias)).flatten()
    }
}

fn metadata_error(error: ace_swarm::resolve::ResolveError) -> LocalDescriptorError {
    match error {
        ace_swarm::resolve::ResolveError::ProtectedContent => {
            LocalDescriptorError::ProtectedContent
        }
        ace_swarm::resolve::ResolveError::InvalidProtection => {
            LocalDescriptorError::InvalidProtection
        }
        _ => LocalDescriptorError::Metadata,
    }
}
fn decode_descriptor(bytes: &[u8]) -> Result<LocalDescriptor, LocalDescriptorError> {
    use ace_swarm::resolve::{stream_info_from_transport, vod_info_from_transport};
    let live = if ace_wire::infohash::is_transport_file(bytes) {
        ace_wire::transport::decode_transport(bytes)
            .map_err(|error| match error {
                ace_wire::WireError::InvalidProtection => LocalDescriptorError::InvalidProtection,
                _ => LocalDescriptorError::Metadata,
            })?
            .is_live
    } else {
        false
    };
    if live {
        let info = stream_info_from_transport(bytes).map_err(metadata_error)?;
        info.ensure_playable().map_err(metadata_error)?;
        Ok(LocalDescriptor::Live(info))
    } else {
        let info = vod_info_from_transport(bytes).map_err(metadata_error)?;
        info.ensure_playable().map_err(metadata_error)?;
        Ok(LocalDescriptor::Vod(info))
    }
}

#[cfg(unix)]
fn read_descriptor(path: &std::path::Path) -> Result<Vec<u8>, LocalDescriptorError> {
    use std::{io::Read, os::unix::fs::MetadataExt};
    let mut file = open_descriptor(path, || {})?;
    let before = file.metadata().map_err(|_| LocalDescriptorError::File)?;
    if !before.is_file()
        || before.uid() != unsafe { libc::geteuid() }
        || before.mode() & 0o022 != 0
        || before.len() > MAX_DESCRIPTOR_BYTES as u64
    {
        return Err(LocalDescriptorError::File);
    }
    let mut bytes = Vec::with_capacity(before.len() as usize);
    (&mut file)
        .take(MAX_DESCRIPTOR_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| LocalDescriptorError::File)?;
    let after = file.metadata().map_err(|_| LocalDescriptorError::File)?;
    if bytes.len() > MAX_DESCRIPTOR_BYTES
        || before.len() != after.len()
        || before.mtime() != after.mtime()
        || before.mtime_nsec() != after.mtime_nsec()
        || after.len() != bytes.len() as u64
    {
        return Err(LocalDescriptorError::File);
    }
    Ok(bytes)
}
#[cfg(unix)]
fn open_descriptor(
    path: &std::path::Path,
    before_leaf: impl FnOnce(),
) -> Result<std::fs::File, LocalDescriptorError> {
    use std::{
        ffi::CString,
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::{ffi::OsStrExt, fs::OpenOptionsExt},
        },
        path::Component,
    };
    let mut components = path.components().peekable();
    if components.next() != Some(Component::RootDir) || components.peek().is_none() {
        return Err(LocalDescriptorError::File);
    }
    let mut directory = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open("/")
        .map_err(|_| LocalDescriptorError::File)?;
    let mut before_leaf = Some(before_leaf);
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(LocalDescriptorError::File);
        };
        let name = CString::new(name.as_bytes()).map_err(|_| LocalDescriptorError::File)?;
        let leaf = components.peek().is_none();
        if leaf {
            before_leaf.take().expect("one leaf")();
        }
        let flags = libc::O_RDONLY
            | libc::O_NOFOLLOW
            | libc::O_CLOEXEC
            | libc::O_NONBLOCK
            | if leaf { 0 } else { libc::O_DIRECTORY };
        // SAFETY: directory owns a live descriptor and name is NUL-terminated. No creation
        // flags are passed; every component is opened relative to the retained directory.
        let fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
        if fd < 0 {
            return Err(LocalDescriptorError::File);
        }
        // SAFETY: successful openat returned a new descriptor, transferred to exactly one File.
        let opened = unsafe { std::fs::File::from_raw_fd(fd) };
        if leaf {
            return Ok(opened);
        }
        directory = opened;
    }
    Err(LocalDescriptorError::File)
}

#[cfg(not(unix))]
fn read_descriptor(_: &std::path::Path) -> Result<Vec<u8>, LocalDescriptorError> {
    Err(LocalDescriptorError::File)
}

#[cfg(test)]
mod tests {
    use super::*;
    use portable_atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Fixture(std::path::PathBuf);
    impl Fixture {
        fn new(bytes: &[u8]) -> Self {
            let path = std::env::temp_dir().join(format!(
                "outpace-local-descriptor-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            use std::io::Write;
            options.open(&path).unwrap().write_all(bytes).unwrap();
            Self(path)
        }
        fn config(&self) -> String {
            serde_json::json!({"fixture":self.0}).to_string()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn local_map_admits_live_and_vod_without_request_file_reads() {
        let (live_bytes, _) = crate::ace_provider::test_support::synthetic_live_transport(32768);
        let live = Fixture::new(&live_bytes);
        let map = LocalDescriptors::from_config(Some(&live.config())).unwrap();
        std::fs::remove_file(&live.0).unwrap();
        assert!(matches!(
            map.get("local:fixture"),
            Some(LocalDescriptor::Live(_))
        ));
        assert!(map.get("local:/etc/passwd").is_none());
        assert!(map.get("local:../fixture").is_none());
        assert!(map.get("local:missing").is_none());
        assert!(map.get(live.0.to_str().unwrap()).is_none());
        let vod = Fixture::new(&crate::ace_provider::test_support::protection_vod_bytes(
            0, false,
        ));
        let map = LocalDescriptors::from_config(Some(&vod.config())).unwrap();
        assert!(matches!(
            map.get("local:fixture"),
            Some(LocalDescriptor::Vod(_))
        ));
        assert!(!format!("{map:?}").contains(vod.0.to_str().unwrap()));
    }

    #[test]
    fn local_map_rejects_ambiguous_unbounded_or_protected_input() {
        assert!(LocalDescriptors::from_config(None)
            .unwrap()
            .get("local:fixture")
            .is_none());
        for raw in [
            "",
            "[]",
            r#"{"x":"relative"}"#,
            r#"{"../x":"/x"}"#,
            r#"{"x":"/x","x":"/y"}"#,
        ] {
            assert!(LocalDescriptors::from_config(Some(raw)).is_err());
        }
        let protected = Fixture::new(&crate::ace_provider::test_support::protection_vod_bytes(
            1, false,
        ));
        assert!(matches!(
            LocalDescriptors::from_config(Some(&protected.config())),
            Err(LocalDescriptorError::ProtectedContent)
        ));
        let malformed = Fixture::new(b"not metadata");
        assert!(LocalDescriptors::from_config(Some(&malformed.config())).is_err());
        let oversized = Fixture::new(&vec![0; 1024 * 1024 + 1]);
        assert!(LocalDescriptors::from_config(Some(&oversized.config())).is_err());
        let many: std::collections::BTreeMap<_, _> =
            (0..65).map(|i| (format!("f{i}"), "/x")).collect();
        assert!(
            LocalDescriptors::from_config(Some(&serde_json::to_string(&many).unwrap())).is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn local_map_rejects_symlinks_nonregular_and_writable_descriptors() {
        use std::os::unix::fs::PermissionsExt;
        let file = Fixture::new(&crate::ace_provider::test_support::protection_vod_bytes(
            0, false,
        ));
        std::fs::set_permissions(&file.0, std::fs::Permissions::from_mode(0o666)).unwrap();
        assert!(LocalDescriptors::from_config(Some(&file.config())).is_err());
        std::fs::set_permissions(&file.0, std::fs::Permissions::from_mode(0o600)).unwrap();
        let link = Fixture::new(b"");
        std::fs::remove_file(&link.0).unwrap();
        std::os::unix::fs::symlink(&file.0, &link.0).unwrap();
        assert!(LocalDescriptors::from_config(Some(&link.config())).is_err());
        assert!(LocalDescriptors::from_config(Some(r#"{"fixture":"/dev/null"}"#)).is_err());
    }
    #[cfg(unix)]
    #[test]
    fn parent_symlink_swap_cannot_redirect_admitted_file() {
        use std::io::Read;
        let parent = Fixture::new(b"");
        let alternate = Fixture::new(b"");
        std::fs::remove_file(&parent.0).unwrap();
        std::fs::remove_file(&alternate.0).unwrap();
        std::fs::create_dir(&parent.0).unwrap();
        std::fs::create_dir(&alternate.0).unwrap();
        std::fs::write(parent.0.join("descriptor"), b"original").unwrap();
        std::fs::write(alternate.0.join("descriptor"), b"redirected").unwrap();
        let saved = parent.0.with_extension("saved");
        let result = open_descriptor(&parent.0.join("descriptor"), || {
            std::fs::rename(&parent.0, &saved).unwrap();
            std::os::unix::fs::symlink(&alternate.0, &parent.0).unwrap();
        })
        .and_then(|mut file| {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)
                .map_err(|_| LocalDescriptorError::File)?;
            Ok(bytes)
        });
        let _ = std::fs::remove_file(&parent.0);
        let _ = std::fs::remove_dir_all(&saved);
        let _ = std::fs::remove_dir_all(&alternate.0);
        assert_eq!(result.unwrap(), b"original");
    }
}
