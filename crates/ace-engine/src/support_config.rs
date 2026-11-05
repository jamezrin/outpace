//! Bounded operator declarations for autonomous live support; parsing performs no I/O.

use serde::de::{MapAccess, Visitor};
use std::{collections::BTreeMap, fmt, sync::Arc};

const MAX_CONFIG_BYTES: usize = 64 * 1024;
const MAX_DECLARATIONS: usize = 8;
const MAX_SELECTOR_BYTES: usize = 4096;

#[derive(Clone, Default, PartialEq, Eq)]
pub struct SupportSources(Arc<BTreeMap<String, String>>);

impl fmt::Debug for SupportSources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SupportSources")
            .field("entries", &self.0.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SupportConfigError;

impl fmt::Display for SupportConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid_support_sources")
    }
}

impl std::error::Error for SupportConfigError {}

impl SupportSources {
    pub fn validate(&self, config: &crate::config::Config) -> Result<(), SupportConfigError> {
        if !self.is_empty()
            && (config.peer_policy.is_private()
                || !config.enable_inbound
                || !config.enable_seeding
                || !config.networks.iter().any(|network| network == "ace"))
        {
            return Err(SupportConfigError);
        }
        Ok(())
    }

    pub fn parse(raw: Option<&str>) -> Result<Self, SupportConfigError> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        if raw.len() > MAX_CONFIG_BYTES {
            return Err(SupportConfigError);
        }
        serde_json::from_str(raw).map_err(|_| SupportConfigError)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(name, id)| (name.as_str(), id.as_str()))
    }
}

struct SourcesVisitor;

impl<'de> Visitor<'de> for SourcesVisitor {
    type Value = SupportSources;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a bounded support declaration map")
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut sources = BTreeMap::new();
        while let Some(name) = map.next_key::<String>()? {
            if sources.len() >= MAX_DECLARATIONS
                || !crate::local_descriptors::alias_valid(&name)
                || sources.contains_key(&name)
            {
                return Err(serde::de::Error::custom(SupportConfigError));
            }
            let raw = map.next_value::<String>()?;
            let id = provider_id(&raw).map_err(serde::de::Error::custom)?;
            sources.insert(name, id);
        }
        Ok(SupportSources(Arc::new(sources)))
    }
}

impl<'de> serde::Deserialize<'de> for SupportSources {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        decoder
            .deserialize_map(SourcesVisitor)
            .map_err(|_| serde::de::Error::custom(SupportConfigError))
    }
}

impl serde::Serialize for SupportSources {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, id) in self.iter() {
            // Persist the URL form so base64 expansion cannot exceed input bounds
            // when an otherwise valid configuration is read back.
            if let Some(url) = crate::transport_url::decode_transport_url(id) {
                map.serialize_entry(name, &url)?;
            } else {
                map.serialize_entry(name, id)?;
            }
        }
        map.end()
    }
}

fn provider_id(raw: &str) -> Result<String, SupportConfigError> {
    if raw.is_empty() || raw.len() > MAX_SELECTOR_BYTES || raw.chars().any(char::is_control) {
        return Err(SupportConfigError);
    }
    let input = raw.trim();
    let id = if let Some(cid) = input.strip_prefix("cid:") {
        ace_swarm::resolve::hex20(cid).map_err(|_| SupportConfigError)?;
        format!("cid:{}", cid.to_ascii_lowercase())
    } else if ace_swarm::resolve::hex20(input).is_ok() {
        input.to_ascii_lowercase()
    } else if let Some(url) = crate::transport_url::decode_transport_url(input) {
        ace_swarm::http_fetch::FetchUrl::parse_transport(&url).map_err(|_| SupportConfigError)?;
        crate::transport_url::encode_transport_url(&url).map_err(|_| SupportConfigError)?
    } else {
        crate::cli::PlaybackTarget::parse(input)
            .map_err(|_| SupportConfigError)?
            .provider_id
    };
    if let Some(url) = crate::transport_url::decode_transport_url(&id) {
        // Syntax only: real resolution still performs DNS/address and redirect guards.
        ace_swarm::http_fetch::FetchUrl::parse_transport(&url).map_err(|_| SupportConfigError)?;
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SYNTHETIC: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn declarations_are_off_by_default() {
        assert!(SupportSources::parse(None).unwrap().is_empty());
        assert!(SupportSources::parse(Some("{}")).unwrap().is_empty());
    }

    #[test]
    fn cli_and_provider_selectors_share_canonical_keys_without_io() {
        let raw = format!(
            r#"{{"first":"acestream://{SYNTHETIC}","second":"cid:{SYNTHETIC}","third":"{SYNTHETIC}","fourth":"local:synthetic"}}"#
        );
        let sources = SupportSources::parse(Some(&raw)).unwrap();
        assert_eq!(
            sources.iter().collect::<Vec<_>>(),
            vec![
                ("first", format!("cid:{SYNTHETIC}").as_str()),
                ("fourth", "local:synthetic"),
                ("second", format!("cid:{SYNTHETIC}").as_str()),
                ("third", SYNTHETIC),
            ]
        );
    }

    #[test]
    fn duplicate_names_and_invalid_maps_never_publish_partial_declarations() {
        for raw in [
            r#"{"same":"local:first","same":"local:second"}"#,
            r#"{"first":"local:valid","second":false}"#,
            r#"{"":"local:valid"}"#,
            r#"{"bad/name":"local:valid"}"#,
            r#"{"é":"local:valid"}"#,
            r#"{"first":""}"#,
            r#"{"first":"file:///private/selector"}"#,
            r#"{"first":"local:../secret"}"#,
            r#"{"first":"not-a-selector"}"#,
            "[]",
            "null",
            "{} trailing",
            "",
        ] {
            assert_eq!(SupportSources::parse(Some(raw)), Err(SupportConfigError));
        }
    }

    #[test]
    fn declarations_and_name_limits_are_enforced_at_admission() {
        let map = |count: usize| {
            format!(
                "{{{}}}",
                (0..count)
                    .map(|i| format!(r#""source{i}":"local:synthetic""#))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        };
        assert_eq!(
            SupportSources::parse(Some(&map(8))).unwrap().iter().count(),
            8
        );
        assert!(SupportSources::parse(Some(&map(9))).is_err());
        assert!(SupportSources::parse(Some(&format!(
            r#"{{"{}":"local:synthetic"}}"#,
            "n".repeat(64)
        )))
        .is_ok());
        assert!(SupportSources::parse(Some(&format!(
            r#"{{"{}":"local:synthetic"}}"#,
            "n".repeat(65)
        )))
        .is_err());
    }

    #[test]
    fn raw_configuration_and_selector_bytes_are_bounded() {
        let bounded = format!("{}{{}}", " ".repeat(65534));
        assert!(SupportSources::parse(Some(&bounded)).is_ok());
        assert!(SupportSources::parse(Some(&format!(" {bounded}"))).is_err());
        let prefix = "https://example.invalid/";
        let url = format!("{prefix}{}", "x".repeat(4096 - prefix.len()));
        assert!(SupportSources::parse(Some(&format!(r#"{{"source":"{url}"}}"#))).is_ok());
        assert!(SupportSources::parse(Some(&format!(r#"{{"source":"{url}x"}}"#))).is_err());
    }

    #[test]
    fn private_values_do_not_escape_debug_or_errors() {
        let secret = "https://example.invalid/descriptor?secret=private-token";
        let sources =
            SupportSources::parse(Some(&format!(r#"{{"private-name":"{secret}"}}"#))).unwrap();
        let debug = format!("{sources:?}");
        assert!(!debug.contains("private-name"));
        assert!(!debug.contains("private-token"));
        let error = SupportSources::parse(Some(r#"{"private-name":"private-token"}"#)).unwrap_err();
        assert!(!format!("{error} {error:?}").contains("private-token"));
    }

    #[test]
    fn support_requires_public_inbound_seeding_and_the_ace_provider() {
        let sources = SupportSources::parse(Some(r#"{"source":"local:synthetic"}"#)).unwrap();
        let defaults = crate::config::Config::default();
        assert!(sources.validate(&defaults).is_ok());
        let mut conflicts = Vec::new();
        let mut config = defaults.clone();
        config.enable_inbound = false;
        conflicts.push(config);
        let mut config = defaults.clone();
        config.enable_seeding = false;
        conflicts.push(config);
        let mut config = defaults.clone();
        config.networks.clear();
        conflicts.push(config);
        let mut config = defaults;
        config.peer_policy =
            ace_swarm::peer_policy::PeerPolicy::from_private_peers(Some("127.0.0.1:32123"))
                .unwrap();
        conflicts.push(config);
        for config in conflicts {
            assert_eq!(sources.validate(&config), Err(SupportConfigError));
            assert!(SupportSources::default().validate(&config).is_ok());
        }
    }

    #[test]
    fn serde_boundary_redacts_untrusted_non_string_values() {
        let error =
            serde_json::from_str::<SupportSources>(r#"{"private-name":{"private-token":"value"}}"#)
                .unwrap_err();
        assert!(!error.to_string().contains("private-token"));
        assert!(!error.to_string().contains("private-name"));
        for selector in ["http://", "turl-invalid", "cid:invalid"] {
            assert!(SupportSources::parse(Some(&format!(r#"{{"source":"{selector}"}}"#))).is_err());
        }
    }

    #[test]
    fn full_configuration_preserves_support_declarations() {
        let config: crate::config::Config =
            serde_json::from_str(r#"{"support_sources":{"source":"local:synthetic"}}"#).unwrap();
        let serialized = serde_json::to_value(config).unwrap();
        assert_eq!(serialized["support_sources"]["source"], "local:synthetic");
    }

    #[tokio::test]
    async fn conflicting_support_configuration_has_no_runtime_filesystem_effects() {
        let root = std::env::temp_dir().join(format!(
            "outpace-support-config-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let config = crate::config::Config {
            support_sources: SupportSources::parse(Some(r#"{"source":"local:synthetic"}"#))
                .unwrap(),
            networks: Vec::new(),
            enable_inbound: false,
            enable_seeding: false,
            data_dir: root.clone(),
            cache_dir: root.join("cache"),
            ..Default::default()
        };
        let result = crate::runtime::build_runtime(config, Vec::new()).await;
        let refused = match result {
            Err(error) => error.to_string() == "invalid_support_sources",
            Ok(runtime) => {
                let _ = runtime.broadcasts.shutdown().await;
                let _ = runtime.manager.shutdown().await;
                false
            }
        };
        let wrote_files = root.exists();
        if wrote_files {
            std::fs::remove_dir_all(&root).unwrap();
        }
        assert!(
            refused,
            "runtime must reject support configuration before startup"
        );
        assert!(
            !wrote_files,
            "invalid configuration must not create identity/cache files"
        );
    }
}
