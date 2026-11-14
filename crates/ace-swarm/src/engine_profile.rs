//! The engine identity outpace presents to peers, trackers and AceStream services.
//!
//! Two process-wide values live here, both written once at daemon startup:
//!
//! - the peer listening port advertised in the signed extended handshake (`p`);
//! - the optional AceStream identity installed by `OUTPACE_ACESTREAM_IDENTITY=1`. When
//!   installed, every value a remote party can observe (handshake version and platform,
//!   search and catalog request parameters, HTTP headers, UPnP label) comes from one
//!   coherent official-engine profile. Without it outpace keeps its own labels.
//!
//! Values come from captures of the official Linux engine 3.2.11; see
//! `docs/superpowers/specs/2026-11-14-engine-camouflage-design.md`.

use ace_wire::extended::{GeoFields, NodeFields};
use rand::RngExt;
use sha1::{Digest, Sha1};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

/// The engine's default peer port, advertised until the daemon installs its own.
pub const DEFAULT_LISTEN_PORT: u16 = 8621;
/// The official Linux release outpace claims by default.
pub const DEFAULT_VERSION: &str = "3.2.11";

pub const ENV_IDENTITY: &str = "OUTPACE_ACESTREAM_IDENTITY";
pub const ENV_VERSION: &str = "OUTPACE_ACESTREAM_IDENTITY_VERSION";
pub const ENV_PLATFORM: &str = "OUTPACE_ACESTREAM_IDENTITY_PLATFORM";
pub const ENV_DEVICE_ID: &str = "OUTPACE_ACESTREAM_IDENTITY_DEVICE_ID";
pub const ENV_HARDWARE_KEY: &str = "OUTPACE_ACESTREAM_IDENTITY_HARDWARE_KEY";
pub const ENV_ASN: &str = "OUTPACE_ACESTREAM_IDENTITY_ASN";
pub const ENV_COUNTRY: &str = "OUTPACE_ACESTREAM_IDENTITY_COUNTRY";
pub const ENV_USER_AGENT: &str = "OUTPACE_ACESTREAM_IDENTITY_USER_AGENT";

static LISTEN_PORT: AtomicU32 = AtomicU32::new(DEFAULT_LISTEN_PORT as u32);
static IDENTITY: OnceLock<EngineIdentity> = OnceLock::new();

/// Record the local peer-listener port advertised as `p` in signed handshakes.
pub fn set_listen_port(port: u16) {
    LISTEN_PORT.store(port as u32, Ordering::Relaxed);
}

/// The advertised peer-listener port.
pub fn listen_port() -> u16 {
    LISTEN_PORT.load(Ordering::Relaxed) as u16
}

/// Install the AceStream identity for the rest of the process. Returns the identity back
/// when one is already installed.
pub fn install(identity: EngineIdentity) -> Result<(), EngineIdentity> {
    IDENTITY.set(identity)
}

/// The installed AceStream identity, or `None` when outpace presents itself as outpace.
pub fn identity() -> Option<&'static EngineIdentity> {
    IDENTITY.get()
}

/// The claimed engine version: the installed identity's, else the default release.
pub fn version() -> (&'static str, i64) {
    match identity() {
        Some(id) => (id.version_name.as_str(), id.version_code),
        None => (DEFAULT_VERSION, 3_021_100),
    }
}

/// The claimed platform name as sent in `_p` (`linux`).
pub fn platform_name() -> &'static str {
    identity().map_or(Platform::Linux, |id| id.platform).name()
}

/// Node fields for a signed extended handshake: the claimed version and platform, our
/// advertised listen port, host uptime as `ts`, and our network location when known.
pub fn node_fields() -> NodeFields {
    let mut fields = NodeFields {
        ts: uptime_secs(),
        p: listen_port() as i64,
        ..NodeFields::default()
    };
    if let Some(id) = identity() {
        fields.v = id.version_code;
        fields.platform = id.platform.id();
        fields.geo = id.geo();
    }
    fields
}

/// Host uptime in whole seconds, the engine's `ts` handshake value. Read from the kernel
/// once and then advanced with a monotonic clock; process uptime where unavailable.
pub fn uptime_secs() -> i64 {
    static BASE: OnceLock<(Instant, u64)> = OnceLock::new();
    let (at, secs) = BASE.get_or_init(|| (Instant::now(), host_uptime().unwrap_or(0)));
    (secs + at.elapsed().as_secs()) as i64
}

fn host_uptime() -> Option<u64> {
    let raw = std::fs::read_to_string("/proc/uptime").ok()?;
    parse_uptime(&raw)
}

fn parse_uptime(raw: &str) -> Option<u64> {
    let first = raw.split_whitespace().next()?;
    let whole = first.split('.').next()?;
    whole.parse().ok()
}

/// A platform the official engine ships for. Only Linux is captured end to end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Linux,
}

impl Platform {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "linux" => Some(Self::Linux),
            _ => None,
        }
    }
    /// The handshake `platform` id (1 Windows, 2 Linux, 3 Android).
    pub fn id(self) -> i64 {
        match self {
            Self::Linux => 2,
        }
    }
    /// The `_p` value in search and catalog requests.
    pub fn name(self) -> &'static str {
        match self {
            Self::Linux => "linux",
        }
    }
    /// The platform token inside User-Agent strings.
    fn ua_token(self) -> &'static str {
        match self {
            Self::Linux => "Linux",
        }
    }
}

/// Why an identity override was rejected. Names the variable's value class, never the
/// value itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityError {
    Version,
    Platform,
    DeviceId,
    HardwareKey,
    Asn,
    Country,
    UserAgent,
}

/// Parse `X.Y.Z` into the version string and the engine's numeric version code
/// (`X*1_000_000 + Y*10_000 + Z*100`; 3.2.11 is 3021100).
pub fn parse_version(raw: &str) -> Result<(String, i64), IdentityError> {
    let parts: Vec<&str> = raw.split('.').collect();
    let [x, y, z] = parts.as_slice() else {
        return Err(IdentityError::Version);
    };
    let num = |s: &str, max: i64| -> Result<i64, IdentityError> {
        if s.is_empty() || s.len() > 2 || !s.bytes().all(|b| b.is_ascii_digit()) {
            return Err(IdentityError::Version);
        }
        let n: i64 = s.parse().map_err(|_| IdentityError::Version)?;
        if n > max {
            return Err(IdentityError::Version);
        }
        Ok(n)
    };
    let (x, y, z) = (num(x, 9)?, num(y, 99)?, num(z, 99)?);
    if x == 0 {
        return Err(IdentityError::Version);
    }
    Ok((format!("{x}.{y}.{z}"), x * 1_000_000 + y * 10_000 + z * 100))
}

/// Parse a 40-hex identifier (device id or hardware key).
pub fn parse_id(raw: &str, error: IdentityError) -> Result<[u8; 20], IdentityError> {
    let bytes = hex::decode(raw.to_ascii_lowercase()).map_err(|_| error)?;
    bytes.try_into().map_err(|_| error)
}

/// Parse an ISO 3166 alpha-2 country code.
pub fn parse_country(raw: &str) -> Result<[u8; 2], IdentityError> {
    let upper = raw.to_ascii_uppercase();
    match upper.as_bytes() {
        [a, b] if a.is_ascii_uppercase() && b.is_ascii_uppercase() => Ok([*a, *b]),
        _ => Err(IdentityError::Country),
    }
}

/// Parse an autonomous system number, with or without an `AS` prefix.
pub fn parse_asn(raw: &str) -> Result<i64, IdentityError> {
    let digits = raw
        .strip_prefix("AS")
        .or_else(|| raw.strip_prefix("as"))
        .unwrap_or(raw);
    let n: u32 = digits.parse().map_err(|_| IdentityError::Asn)?;
    if n == 0 {
        return Err(IdentityError::Asn);
    }
    Ok(n as i64)
}

/// The official engine's fixed User-Agent for AceStream web hosts and search.
pub fn default_user_agent(platform: Platform, version_name: &str) -> String {
    format!(
        "Mozilla/5.0 ({}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 \
         Safari/537.36 AceStream/{version_name}",
        platform.ua_token()
    )
}

/// Validate a User-Agent override: printable ASCII, bounded, no header injection.
pub fn parse_user_agent(raw: &str) -> Result<String, IdentityError> {
    if raw.is_empty() || raw.len() > 256 || !raw.bytes().all(|b| (0x20..0x7f).contains(&b)) {
        return Err(IdentityError::UserAgent);
    }
    Ok(raw.to_owned())
}

/// The engine's hardware key (`_b`) on Linux: `sha1hex(sha1hex(str(MemTotal_kB * 1024)))`.
/// The engine's disk-id probe never matches on Linux, so memory is the only input.
pub fn hardware_key_from_mem_total_kb(mem_total_kb: u64) -> [u8; 20] {
    let id = hex::encode(Sha1::digest(mem_total_kb.saturating_mul(1024).to_string()));
    Sha1::digest(id.as_bytes()).into()
}

/// This host's hardware key, as the official engine would compute it here.
pub fn host_hardware_key() -> Option<[u8; 20]> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    parse_mem_total_kb(&meminfo).map(hardware_key_from_mem_total_kb)
}

fn parse_mem_total_kb(meminfo: &str) -> Option<u64> {
    meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// The AceStream engine identity presented when `OUTPACE_ACESTREAM_IDENTITY=1`.
#[derive(Debug)]
pub struct EngineIdentity {
    pub version_name: String,
    pub version_code: i64,
    pub platform: Platform,
    /// Random per-install id (`_a` in search, UPnP mapping label).
    pub device_id: [u8; 20],
    /// Host-derived hardware key (`_b` in search).
    pub hardware_key: [u8; 20],
    /// Fixed User-Agent for search and AceStream web hosts.
    pub user_agent: String,
    geo: OnceLock<(i64, [u8; 2])>,
}

impl EngineIdentity {
    pub fn new(
        (version_name, version_code): (String, i64),
        platform: Platform,
        device_id: [u8; 20],
        hardware_key: [u8; 20],
        user_agent: String,
    ) -> Self {
        Self {
            version_name,
            version_code,
            platform,
            device_id,
            hardware_key,
            user_agent,
            geo: OnceLock::new(),
        }
    }

    /// Record our network location once. Later calls are ignored.
    pub fn set_geo(&self, asn: i64, country: [u8; 2]) {
        let _ = self.geo.set((asn, country));
    }

    pub fn has_geo(&self) -> bool {
        self.geo.get().is_some()
    }

    fn geo(&self) -> GeoFields {
        match self.geo.get() {
            Some(&(asn, country)) => GeoFields::Known { asn, country },
            None => GeoFields::Pending,
        }
    }

    /// The UPnP port-mapping description: `AS ` and the first four device-id hex digits.
    pub fn port_mapping_description(&self) -> String {
        format!("AS {}", &hex::encode(self.device_id)[..4])
    }

    /// The randomized User-Agent the engine sends to catalog servers: a Chrome string
    /// with random version components and no `AceStream/` suffix.
    pub fn random_catalog_user_agent(&self) -> String {
        let mut rng = rand::rng();
        format!(
            "Mozilla/5.0 ({}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{}.0.{}.{} \
             Safari/537.36",
            self.platform.ua_token(),
            rng.random_range(60..=117u32),
            rng.random_range(1..=3000u32),
            rng.random_range(1..=100u32),
        )
    }
}

/// A fresh `sentry-trace` value: 32-hex trace id, 16-hex span id, empty sampled flag.
pub fn sentry_trace() -> String {
    let mut rng = rand::rng();
    let (trace, span): (u128, u64) = (rng.random(), rng.random());
    format!("{trace:032x}-{span:016x}-")
}

/// Why an AceStream identity could not be built. The message names the variable, never the value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityConfigError {
    Invalid(&'static str),
    HardwareKey,
    AsnCountry,
}

impl std::fmt::Display for IdentityConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(name) => write!(f, "invalid {name}"),
            Self::HardwareKey => write!(f, "{ENV_HARDWARE_KEY} is required on this host"),
            Self::AsnCountry => write!(
                f,
                "{ENV_ASN} and {ENV_COUNTRY} must both be set or both be unset"
            ),
        }
    }
}

impl std::error::Error for IdentityConfigError {}

/// Operator overrides and host facts used to build one [`EngineIdentity`].
/// Absent overrides take the captured Linux defaults. A device id is generated only when
/// neither an override nor a persisted id is present.
pub struct IdentityInputs<'a> {
    pub version: Option<&'a str>,
    pub platform: Option<&'a str>,
    pub device_id: Option<&'a str>,
    pub hardware_key: Option<&'a str>,
    pub asn: Option<&'a str>,
    pub country: Option<&'a str>,
    pub user_agent: Option<&'a str>,
    pub persisted_device_id: Option<[u8; 20]>,
    pub host_hardware_key: Option<[u8; 20]>,
}

/// An identity ready to install, plus a device id the caller should persist on first use.
#[derive(Debug)]
pub struct ResolvedIdentity {
    pub identity: EngineIdentity,
    pub persist_device_id: Option<[u8; 20]>,
}

/// Check overrides without generating a device id or reading the host.
pub fn validate_identity_inputs(inputs: &IdentityInputs<'_>) -> Result<(), IdentityConfigError> {
    checked(inputs).map(|_| ())
}

/// Build the identity described by `inputs`.
pub fn resolve_identity(
    inputs: IdentityInputs<'_>,
) -> Result<ResolvedIdentity, IdentityConfigError> {
    let checked = checked(&inputs)?;
    let (device_id, persist_device_id) = if let Some(raw) = inputs.device_id {
        (
            parse_id(raw, IdentityError::DeviceId)
                .map_err(|_| IdentityConfigError::Invalid(ENV_DEVICE_ID))?,
            None,
        )
    } else if let Some(id) = inputs.persisted_device_id {
        (id, None)
    } else {
        let id = random_device_id();
        (id, Some(id))
    };
    let identity = EngineIdentity::new(
        (checked.version_name, checked.version_code),
        checked.platform,
        device_id,
        checked.hardware_key,
        checked.user_agent,
    );
    if let Some((asn, country)) = checked.geo {
        identity.set_geo(asn, country);
    }
    Ok(ResolvedIdentity {
        identity,
        persist_device_id,
    })
}

struct CheckedIdentity {
    version_name: String,
    version_code: i64,
    platform: Platform,
    hardware_key: [u8; 20],
    user_agent: String,
    geo: Option<(i64, [u8; 2])>,
}

fn checked(inputs: &IdentityInputs<'_>) -> Result<CheckedIdentity, IdentityConfigError> {
    let (version_name, version_code) = match inputs.version {
        Some(raw) => parse_version(raw).map_err(|_| IdentityConfigError::Invalid(ENV_VERSION))?,
        None => parse_version(DEFAULT_VERSION).expect("default engine version"),
    };
    let platform = match inputs.platform {
        Some(raw) => Platform::parse(raw).ok_or(IdentityConfigError::Invalid(ENV_PLATFORM))?,
        None => Platform::Linux,
    };
    if let Some(raw) = inputs.device_id {
        parse_id(raw, IdentityError::DeviceId)
            .map_err(|_| IdentityConfigError::Invalid(ENV_DEVICE_ID))?;
    }
    let hardware_key = if let Some(raw) = inputs.hardware_key {
        parse_id(raw, IdentityError::HardwareKey)
            .map_err(|_| IdentityConfigError::Invalid(ENV_HARDWARE_KEY))?
    } else {
        inputs
            .host_hardware_key
            .ok_or(IdentityConfigError::HardwareKey)?
    };
    let user_agent = match inputs.user_agent {
        Some(raw) => {
            parse_user_agent(raw).map_err(|_| IdentityConfigError::Invalid(ENV_USER_AGENT))?
        }
        None => default_user_agent(platform, &version_name),
    };
    let geo = match (inputs.asn, inputs.country) {
        (None, None) => None,
        (Some(asn), Some(country)) => Some((
            parse_asn(asn).map_err(|_| IdentityConfigError::Invalid(ENV_ASN))?,
            parse_country(country).map_err(|_| IdentityConfigError::Invalid(ENV_COUNTRY))?,
        )),
        _ => return Err(IdentityConfigError::AsnCountry),
    };
    Ok(CheckedIdentity {
        version_name,
        version_code,
        platform,
        hardware_key,
        user_agent,
        geo,
    })
}

fn random_device_id() -> [u8; 20] {
    rand::random()
}

/// An HTTP/1.1 GET request head in the engine's exact header order and spelling.
/// `host` is the `Host` value (with `:port` when not 80).
pub fn engine_request_head(path_and_query: &str, host: &str, user_agent: &str) -> String {
    let mut head = String::with_capacity(384 + path_and_query.len());
    let _ = write!(
        head,
        "GET {path_and_query} HTTP/1.1\r\n\
         sentry-trace: {}\r\n\
         Host: {host}\r\n\
         User-Agent: {user_agent}\r\n\
         Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\r\n\
         Accept-Language: en-US,en;q=0.5\r\n\
         Accept-Encoding: gzip\r\n\
         Connection: close\r\n\r\n",
        sentry_trace()
    );
    head
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_codes_follow_engine_encoding() {
        assert_eq!(
            parse_version("3.2.11").unwrap(),
            ("3.2.11".into(), 3_021_100)
        );
        assert_eq!(parse_version("3.1.32").unwrap().1, 3_013_200);
        assert_eq!(parse_version("3.2.22").unwrap().1, 3_022_200);
        for bad in [
            "", "3", "3.2", "3.2.11.1", "0.1.0", "3.2.x", "3.100.1", "10.0.0", "3..1",
        ] {
            assert_eq!(parse_version(bad), Err(IdentityError::Version), "{bad}");
        }
    }

    #[test]
    fn only_captured_platforms_parse() {
        assert_eq!(Platform::parse("linux"), Some(Platform::Linux));
        assert_eq!(Platform::Linux.id(), 2);
        for other in ["windows", "android", "Linux", ""] {
            assert_eq!(Platform::parse(other), None);
        }
    }

    #[test]
    fn hardware_key_matches_engine_formula() {
        // sha1hex(sha1hex("1048576")) computed independently for MemTotal 1024 kB.
        let id = hex::encode(Sha1::digest(b"1048576"));
        let expected: [u8; 20] = Sha1::digest(id.as_bytes()).into();
        assert_eq!(hardware_key_from_mem_total_kb(1024), expected);
        assert_eq!(
            parse_mem_total_kb("MemFree: 1 kB\nMemTotal:       16314916 kB\n"),
            Some(16_314_916)
        );
        assert_eq!(parse_mem_total_kb("nothing"), None);
    }

    #[test]
    fn overrides_validate() {
        assert_eq!(parse_asn("AS3352"), Ok(3352));
        assert_eq!(parse_asn("3352"), Ok(3352));
        assert!(parse_asn("0").is_err() && parse_asn("ASx").is_err());
        assert_eq!(parse_country("es"), Ok(*b"ES"));
        assert!(parse_country("ESP").is_err() && parse_country("1A").is_err());
        let placeholder = "0123456789abcdef0123456789abcdef01234567";
        assert!(parse_id(placeholder, IdentityError::DeviceId).is_ok());
        assert!(parse_id("abc", IdentityError::DeviceId).is_err());
        assert!(parse_user_agent("ok/1").is_ok());
        assert!(parse_user_agent("bad\r\nX: y").is_err());
    }

    #[test]
    fn user_agents_match_captured_shapes() {
        assert_eq!(
            default_user_agent(Platform::Linux, "3.2.11"),
            "Mozilla/5.0 (Linux) AppleWebKit/537.36 (KHTML, like Gecko) \
             Chrome/60.0.3112.50 Safari/537.36 AceStream/3.2.11"
        );
        let id = EngineIdentity::new(
            ("3.2.11".into(), 3_021_100),
            Platform::Linux,
            [0xab; 20],
            [0; 20],
            String::new(),
        );
        for _ in 0..200 {
            let ua = id.random_catalog_user_agent();
            let chrome = ua
                .split("Chrome/")
                .nth(1)
                .unwrap()
                .split(' ')
                .next()
                .unwrap();
            let parts: Vec<u32> = chrome.split('.').map(|p| p.parse().unwrap()).collect();
            assert!((60..=117).contains(&parts[0]) && parts[1] == 0);
            assert!((1..=3000).contains(&parts[2]) && (1..=100).contains(&parts[3]));
            assert!(!ua.contains("AceStream"));
        }
        assert_eq!(id.port_mapping_description(), "AS abab");
    }

    #[test]
    fn request_head_uses_engine_header_order() {
        let head = engine_request_head("/a?x=1", "example.invalid", "UA");
        let names: Vec<&str> = head
            .lines()
            .skip(1)
            .filter_map(|l| l.split_once(':').map(|(k, _)| k))
            .collect();
        assert_eq!(
            names,
            [
                "sentry-trace",
                "Host",
                "User-Agent",
                "Accept",
                "Accept-Language",
                "Accept-Encoding",
                "Connection"
            ]
        );
        let trace = head
            .lines()
            .nth(1)
            .unwrap()
            .strip_prefix("sentry-trace: ")
            .unwrap();
        assert_eq!(trace.len(), 50);
        assert!(trace.ends_with('-'));
        assert!(head.ends_with("\r\n\r\n"));
    }

    #[test]
    fn uptime_parses_and_advances() {
        assert_eq!(parse_uptime("1457714.67 21241859.25\n"), Some(1_457_714));
        assert_eq!(parse_uptime(""), None);
        assert!(uptime_secs() >= 0);
    }

    #[test]
    fn default_node_fields_advertise_listen_port_and_uptime() {
        let fields = node_fields();
        assert_eq!(fields.v, 3_021_100);
        assert_eq!(fields.platform, 2);
        assert!(fields.ts >= 0);
    }

    fn sample_inputs() -> IdentityInputs<'static> {
        IdentityInputs {
            version: None,
            platform: None,
            device_id: None,
            hardware_key: Some("0123456789abcdef0123456789abcdef01234567"),
            asn: None,
            country: None,
            user_agent: None,
            persisted_device_id: None,
            host_hardware_key: None,
        }
    }

    #[test]
    fn resolve_uses_captured_linux_defaults_and_a_fresh_device_id() {
        let resolved = resolve_identity(sample_inputs()).unwrap();
        assert_eq!(resolved.identity.version_name, "3.2.11");
        assert_eq!(resolved.identity.version_code, 3_021_100);
        assert_eq!(resolved.identity.platform, Platform::Linux);
        assert_eq!(
            resolved.persist_device_id,
            Some(resolved.identity.device_id)
        );
        assert!(!resolved.identity.has_geo());
        assert!(resolved.identity.user_agent.contains("AceStream/3.2.11"));
        assert!(resolved.identity.user_agent.contains("(Linux)"));
    }

    #[test]
    fn resolve_keeps_a_persisted_device_id_and_records_geo() {
        let mut inputs = sample_inputs();
        inputs.persisted_device_id = Some([0xab; 20]);
        inputs.asn = Some("AS3352");
        inputs.country = Some("es");
        let resolved = resolve_identity(inputs).unwrap();
        assert_eq!(resolved.identity.device_id, [0xab; 20]);
        assert_eq!(resolved.persist_device_id, None);
        assert!(resolved.identity.has_geo());
        let fields = {
            // node_fields reads the process identity, so check geo through a local install path
            // by rebuilding fields the same way node_fields does once geo is set.
            resolved.identity.geo()
        };
        assert!(matches!(
            fields,
            ace_wire::extended::GeoFields::Known {
                asn: 3352,
                country,
            } if &country == b"ES"
        ));
    }

    #[test]
    fn overrides_and_pairs_fail_closed() {
        let mut bad_version = sample_inputs();
        bad_version.version = Some("3.2");
        assert_eq!(
            resolve_identity(bad_version).unwrap_err(),
            IdentityConfigError::Invalid(ENV_VERSION)
        );
        let mut bad_platform = sample_inputs();
        bad_platform.platform = Some("windows");
        assert_eq!(
            validate_identity_inputs(&bad_platform).unwrap_err(),
            IdentityConfigError::Invalid(ENV_PLATFORM)
        );
        let mut half_geo = sample_inputs();
        half_geo.asn = Some("3352");
        assert_eq!(
            validate_identity_inputs(&half_geo).unwrap_err(),
            IdentityConfigError::AsnCountry
        );
        let mut no_key = sample_inputs();
        no_key.hardware_key = None;
        assert_eq!(
            validate_identity_inputs(&no_key).unwrap_err(),
            IdentityConfigError::HardwareKey
        );
        let mut host_key = sample_inputs();
        host_key.hardware_key = None;
        host_key.host_hardware_key = Some([7; 20]);
        assert!(validate_identity_inputs(&host_key).is_ok());
        assert!(format!("{}", IdentityConfigError::HardwareKey).contains(ENV_HARDWARE_KEY));
        assert!(format!("{}", IdentityConfigError::AsnCountry).contains(ENV_ASN));
        assert!(format!("{}", IdentityConfigError::AsnCountry).contains(ENV_COUNTRY));
    }
}
