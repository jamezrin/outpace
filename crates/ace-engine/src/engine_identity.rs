//! Installs the optional AceStream identity once per process.
//!
//! `OUTPACE_ACESTREAM_IDENTITY` defaults off. When it is on, search, catalog, peer
//! handshakes, UPnP and broadcast descriptors share one profile. Overrides are ignored
//! unless the switch is on, and a bad override fails startup with the variable's name.

use crate::config::{
    read_acestream_device_id, write_acestream_device_id, AcestreamIdentitySettings, Config,
};
use ace_swarm::engine_profile::{
    self, IdentityInputs, ENV_ASN, ENV_COUNTRY, ENV_DEVICE_ID, ENV_HARDWARE_KEY, ENV_IDENTITY,
    ENV_PLATFORM, ENV_USER_AGENT, ENV_VERSION,
};

pub(crate) fn settings_from_env() -> Result<AcestreamIdentitySettings, Box<dyn std::error::Error>> {
    let enabled = bool_from_env(ENV_IDENTITY)?.unwrap_or_default();
    if !enabled {
        return Ok(AcestreamIdentitySettings::default());
    }
    let settings = AcestreamIdentitySettings {
        enabled: true,
        version: optional_var(ENV_VERSION)?,
        platform: optional_var(ENV_PLATFORM)?,
        device_id: optional_var(ENV_DEVICE_ID)?,
        hardware_key: optional_var(ENV_HARDWARE_KEY)?,
        asn: optional_var(ENV_ASN)?,
        country: optional_var(ENV_COUNTRY)?,
        user_agent: optional_var(ENV_USER_AGENT)?,
    };
    validate(&settings)?;
    Ok(settings)
}

pub(crate) fn validate(
    settings: &AcestreamIdentitySettings,
) -> Result<(), Box<dyn std::error::Error>> {
    if !settings.enabled {
        return Ok(());
    }
    engine_profile::validate_identity_inputs(&inputs(settings, None))?;
    Ok(())
}

/// Resolve, persist a new device id, and install the identity. A second call in the
/// same process keeps the identity already installed.
pub(crate) fn apply(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    if !config.acestream_identity.enabled {
        return Ok(());
    }
    validate(&config.acestream_identity)?;
    if engine_profile::identity().is_some() {
        return Ok(());
    }
    let persisted = if config.acestream_identity.device_id.is_none() {
        read_acestream_device_id(&config.data_dir).map_err(device_id_io)?
    } else {
        None
    };
    let resolved = engine_profile::resolve_identity(inputs(&config.acestream_identity, persisted))?;
    if let Some(id) = resolved.persist_device_id {
        write_acestream_device_id(&config.data_dir, &id).map_err(device_id_io)?;
    }
    let version = resolved.identity.version_name.clone();
    let platform = resolved.identity.platform.name();
    let _ = engine_profile::install(resolved.identity);
    if engine_profile::identity().is_some_and(|identity| !identity.has_geo()) {
        ace_swarm::engine_location::enable_lookup();
    }
    eprintln!("outpace: AceStream identity enabled ({platform} {version})");
    Ok(())
}

fn inputs(
    settings: &AcestreamIdentitySettings,
    persisted_device_id: Option<[u8; 20]>,
) -> IdentityInputs<'_> {
    IdentityInputs {
        version: settings.version.as_deref(),
        platform: settings.platform.as_deref(),
        device_id: settings.device_id.as_deref(),
        hardware_key: settings.hardware_key.as_deref(),
        asn: settings.asn.as_deref(),
        country: settings.country.as_deref(),
        user_agent: settings.user_agent.as_deref(),
        persisted_device_id,
        host_hardware_key: if settings.hardware_key.is_some() {
            None
        } else {
            engine_profile::host_hardware_key()
        },
    }
}

fn device_id_io(error: std::io::Error) -> Box<dyn std::error::Error> {
    if error.kind() == std::io::ErrorKind::InvalidData {
        return error.into();
    }
    format!("cannot use acestream device id in OUTPACE_DATA_DIR: {error}").into()
}

fn optional_var(name: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("invalid {name}").into()),
    }
}

fn bool_from_env(name: &str) -> Result<Option<bool>, Box<dyn std::error::Error>> {
    let value = match std::env::var(name) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!("{name} must be valid UTF-8 and one of: 1, true, 0, false").into());
        }
    };
    match value.as_str() {
        "1" | "true" => Ok(Some(true)),
        "0" | "false" => Ok(Some(false)),
        _ => Err(format!("invalid {name}: {value:?}; expected one of: 1, true, 0, false").into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled(hardware_key: &str) -> AcestreamIdentitySettings {
        AcestreamIdentitySettings {
            enabled: true,
            hardware_key: Some(hardware_key.into()),
            ..AcestreamIdentitySettings::default()
        }
    }

    #[test]
    fn disabled_settings_skip_override_checks() {
        let mut settings = enabled("nope");
        settings.enabled = false;
        assert!(validate(&settings).is_ok());
    }

    #[test]
    fn enabled_settings_name_the_rejected_variable() {
        let settings = enabled("zz");
        let error = validate(&settings).unwrap_err().to_string();
        assert!(error.contains(ENV_HARDWARE_KEY), "{error}");
        let mut settings = enabled("0123456789abcdef0123456789abcdef01234567");
        settings.version = Some("nope".into());
        let error = validate(&settings).unwrap_err().to_string();
        assert!(error.contains(ENV_VERSION), "{error}");
        settings.version = None;
        settings.asn = Some("3352".into());
        let error = validate(&settings).unwrap_err().to_string();
        assert!(
            error.contains(ENV_ASN) && error.contains(ENV_COUNTRY),
            "{error}"
        );
    }

    #[test]
    fn prepare_reuses_a_persisted_device_id_without_installing() {
        let dir =
            std::env::temp_dir().join(format!("outpace-identity-prepare-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let id = [0x22; 20];
        write_acestream_device_id(&dir, &id).unwrap();
        let settings = enabled("0123456789abcdef0123456789abcdef01234567");
        let persisted = read_acestream_device_id(&dir).unwrap();
        let resolved = engine_profile::resolve_identity(inputs(&settings, persisted)).unwrap();
        assert_eq!(resolved.identity.device_id, id);
        assert_eq!(resolved.persist_device_id, None);
        assert!(engine_profile::identity().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_missing_device_id_file_asks_to_be_persisted() {
        let settings = enabled("0123456789abcdef0123456789abcdef01234567");
        let resolved = engine_profile::resolve_identity(inputs(&settings, None)).unwrap();
        assert_eq!(
            resolved.persist_device_id,
            Some(resolved.identity.device_id)
        );
    }
}
