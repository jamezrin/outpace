//! Startup configuration for the strict outbound-only peer boundary.

use crate::config::Config;
use std::{fmt, net::SocketAddrV4};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PrivateConfigError(pub &'static str);
impl fmt::Display for PrivateConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for PrivateConfigError {}

/// Environment presence disambiguates implicit public defaults from explicit conflicts.
pub fn normalize_private_env(
    config: &mut Config,
    inbound: Option<bool>,
    mapping: Option<bool>,
    trackers_present: bool,
) -> Result<(), PrivateConfigError> {
    if !config.peer_policy.is_private() {
        return Ok(());
    }
    if inbound == Some(true) {
        return Err(PrivateConfigError("private_mode_conflict_enable_inbound"));
    }
    if mapping == Some(true) {
        return Err(PrivateConfigError("private_mode_conflict_port_mapping"));
    }
    if trackers_present {
        return Err(PrivateConfigError("private_mode_conflict_trackers"));
    }
    config.enable_inbound = false;
    config.enable_port_mapping = false;
    Ok(())
}

/// Final effective configuration, including Rust/Serde/CLI callers, must pass before effects.
pub fn validate_private_config(
    config: &Config,
    bootstrap: &[SocketAddrV4],
) -> Result<(), PrivateConfigError> {
    if config.peer_policy.is_private() {
        if config.enable_inbound {
            return Err(PrivateConfigError("private_mode_conflict_enable_inbound"));
        }
        if config.enable_port_mapping {
            return Err(PrivateConfigError("private_mode_conflict_port_mapping"));
        }
        if bootstrap
            .iter()
            .any(|peer| !config.peer_policy.permits(*peer))
        {
            return Err(PrivateConfigError("private_mode_bootstrap_peer_denied"));
        }
    } else if !config.private_descriptors.is_empty() {
        return Err(PrivateConfigError("local_descriptors_require_private_mode"));
    }
    Ok(())
}

pub fn apply_private_env(
    config: &mut Config,
    peers: Option<&str>,
    descriptors: Option<&str>,
    inbound: Option<bool>,
    mapping: Option<bool>,
    trackers: bool,
) -> Result<(), PrivateConfigError> {
    let peer_policy = ace_swarm::peer_policy::PeerPolicy::from_private_peers(peers)
        .map_err(|_| PrivateConfigError("invalid_private_peers"))?;
    let private_descriptors = crate::local_descriptors::DescriptorPaths::from_config(descriptors)
        .map_err(|_| PrivateConfigError("invalid_local_descriptor_config"))?;
    let mut candidate = config.clone();
    candidate.peer_policy = peer_policy;
    candidate.private_descriptors = private_descriptors;
    normalize_private_env(&mut candidate, inbound, mapping, trackers)?;
    validate_private_config(&candidate, &[])?;
    *config = candidate;
    Ok(())
}

/// Public status is deliberately narrower than serialized operator configuration.
pub fn policy_status(policy: &ace_swarm::peer_policy::PeerPolicy) -> serde_json::Value {
    if policy.is_private() {
        serde_json::json!({"mode":"private", "allowed_peer_count":policy.endpoint_count(),
            "inbound_peer_connections":false, "public_discovery":false, "public_advertising":false})
    } else {
        serde_json::json!({"mode":"public", "allowed_peer_count":null})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    fn policy() -> ace_swarm::peer_policy::PeerPolicy {
        ace_swarm::peer_policy::PeerPolicy::from_private_peers(Some("127.0.0.1:1234")).unwrap()
    }

    #[test]
    fn private_defaults_and_explicit_conflict_matrix() {
        let mut config = Config {
            peer_policy: policy(),
            ..Config::default()
        };
        normalize_private_env(&mut config, None, None, false).unwrap();
        assert!(!config.enable_inbound && !config.enable_port_mapping);
        validate_private_config(&config, &[]).unwrap();
        validate_private_config(&config, &["127.0.0.1:1234".parse().unwrap()]).unwrap();
        assert!(validate_private_config(&config, &["127.0.0.1:4321".parse().unwrap()]).is_err());
        for (inbound, mapping, trackers) in [
            (Some(true), None, false),
            (None, Some(true), false),
            (None, None, true),
        ] {
            assert!(normalize_private_env(&mut config, inbound, mapping, trackers).is_err());
        }
        normalize_private_env(&mut config, Some(false), Some(false), false).unwrap();
        config.enable_inbound = true;
        assert!(validate_private_config(&config, &[]).is_err());
        let public = Config::default();
        assert!(public.enable_inbound);
        validate_private_config(&public, &[]).unwrap();
    }

    #[test]
    fn private_config_roundtrip_retains_boundary_but_debug_redacts_endpoints() {
        let mut config = Config {
            peer_policy: policy(),
            ..Config::default()
        };
        normalize_private_env(&mut config, None, None, false).unwrap();
        let json = serde_json::to_string(&config).unwrap();
        let parsed: Config = serde_json::from_str(&json).unwrap();
        assert!(parsed.peer_policy.is_private());
        assert!(parsed
            .peer_policy
            .permits("127.0.0.1:1234".parse().unwrap()));
        assert!(!parsed
            .peer_policy
            .permits("127.0.0.1:4321".parse().unwrap()));
        assert!(!format!("{:?}", parsed.peer_policy).contains("127.0.0.1"));
        assert!(serde_json::from_str::<Config>(r#"{"private_peers":""}"#).is_err());
        let declared: Config = serde_json::from_str(
            r#"{"private_descriptors":{"fixture":"/nonexistent/operator/descriptor"}}"#,
        )
        .unwrap();
        assert!(validate_private_config(&declared, &[]).is_err());
        // Deserialize admits declarations, not files; loading is a distinct startup step.
        assert!(!declared.private_descriptors.is_empty());
    }
    #[tokio::test]
    async fn private_runtime_rejects_conflicting_direct_config_before_identity_files() {
        let path =
            std::env::temp_dir().join(format!("outpace-private-invalid-{}", std::process::id()));
        assert!(!path.exists());
        let config = Config {
            peer_policy: policy(),
            data_dir: path.clone(),
            cache_dir: std::path::PathBuf::new(),
            ..Config::default()
        };
        let result = crate::runtime::build_runtime(config, vec![]).await;
        let error = match result {
            Err(error) => error.to_string(),
            Ok(runtime) => {
                runtime.broadcasts.close_admissions();
                let _ = runtime.broadcasts.shutdown().await;
                runtime.manager.shutdown().await;
                String::new()
            }
        };
        let created = path.exists();
        if created {
            let _ = std::fs::remove_dir_all(&path);
        }
        assert!(!created, "invalid private config created identity files");
        assert_eq!(error, "private_mode_conflict_enable_inbound");
    }
    #[test]
    fn private_env_parser_admits_declarations_without_opening_files() {
        let mut config = Config::default();
        apply_private_env(
            &mut config,
            Some("127.0.0.1:1234"),
            Some(r#"{"fixture":"/nonexistent/operator/descriptor"}"#),
            None,
            None,
            false,
        )
        .unwrap();
        assert!(config.peer_policy.is_private());
        assert!(!config.enable_inbound && !config.enable_port_mapping);
        assert!(!config.private_descriptors.is_empty());
        assert!(apply_private_env(&mut config, None, Some("not json"), None, None, false).is_err());
        assert!(
            config.peer_policy.is_private(),
            "failed parse downgraded an existing private policy"
        );
        assert!(apply_private_env(&mut config, Some(""), None, None, None, false).is_err());
        assert!(apply_private_env(
            &mut config,
            None,
            Some(r#"{"fixture":"/x"}"#),
            None,
            None,
            false
        )
        .is_err());
    }
}
