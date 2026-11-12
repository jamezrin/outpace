//! Run outpace as a Windows service (`outpace service run|install|uninstall`).
//!
//! The Service Control Manager starts `outpace.exe service run`. A Stop or Shutdown request
//! drives the same graceful shutdown as Ctrl+C on a console: the HTTP server drains and the
//! runtime cleans up before the service reports Stopped.
//!
//! Services have no console, so `run` points the process's stderr (where every log line goes)
//! at `<data dir>\outpace.log`. Without `OUTPACE_DATA_DIR` the data directory is
//! `%ProgramData%\outpace`, because the service account's own profile is not a useful home.
//! Settings come from the service's `Environment` registry value; see the wiki.
//!
//! The service runs as its own virtual account, `NT SERVICE\outpace`, and its data directory
//! (identity seed, broadcast signing keys) gets a protected DACL: SYSTEM and Administrators
//! full control, the service account modify, nothing inherited from `%ProgramData%` (which
//! would otherwise let every local user read it).

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use windows_service::service::{
    ServiceAccess, ServiceAction, ServiceActionType, ServiceControl, ServiceControlAccept,
    ServiceErrorControl, ServiceExitCode, ServiceFailureActions, ServiceFailureResetPeriod,
    ServiceInfo, ServiceStartType, ServiceState, ServiceStatus, ServiceType,
};
use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};
use windows_service::{define_windows_service, service_dispatcher};

pub const SERVICE_NAME: &str = "outpace";
const DISPLAY_NAME: &str = "Outpace streaming engine";
const DESCRIPTION: &str = "Streams and broadcasts public Acestream content (outpace serve).";
/// Per-service virtual account: unprivileged, and unlike `LocalService` not shared with
/// other services, so the data directory ACL can name it alone.
const SERVICE_ACCOUNT: &str = r"NT SERVICE\outpace";
/// Longest graceful shutdown the SCM is told to wait for (mirrors `TimeoutStopSec=30`).
const STOP_WAIT_HINT: Duration = Duration::from_secs(30);

type BoxError = Box<dyn std::error::Error>;

define_windows_service!(ffi_service_main, service_main);

/// `outpace service run`: hand the process to the Service Control Manager.
pub fn run() -> Result<(), BoxError> {
    service_dispatcher::start(SERVICE_NAME, ffi_service_main).map_err(|error| {
        format!(
            "{error}: `outpace service run` is started by the Service Control Manager; \
             use `outpace service install`, then `sc.exe start {SERVICE_NAME}`"
        )
        .into()
    })
}

/// `outpace service install`: register this executable as an automatic-start service.
pub fn install() -> Result<(), BoxError> {
    let manager = ServiceManager::local_computer(
        None::<&str>,
        ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE,
    )?;
    let info = ServiceInfo {
        name: OsString::from(SERVICE_NAME),
        display_name: OsString::from(DISPLAY_NAME),
        service_type: ServiceType::OWN_PROCESS,
        start_type: ServiceStartType::AutoStart,
        error_control: ServiceErrorControl::Normal,
        executable_path: std::env::current_exe()?,
        launch_arguments: vec![OsString::from("service"), OsString::from("run")],
        dependencies: vec![],
        account_name: Some(OsString::from(SERVICE_ACCOUNT)),
        account_password: None,
    };
    let service = manager.create_service(
        &info,
        ServiceAccess::CHANGE_CONFIG | ServiceAccess::START | ServiceAccess::QUERY_STATUS,
    )?;
    service.set_description(DESCRIPTION)?;
    // The virtual account exists once the service does; lock down the default data dir now so
    // it is never created with %ProgramData%'s inherited, world-readable ACL.
    if let Some(dir) = default_data_dir(std::env::var_os("ProgramData").as_deref()) {
        std::fs::create_dir_all(&dir)?;
        restrict_data_dir(&dir)?;
    }
    // Restart after a crash, like systemd's Restart=on-failure.
    service.update_failure_actions(ServiceFailureActions {
        reset_period: ServiceFailureResetPeriod::After(Duration::from_secs(24 * 60 * 60)),
        reboot_msg: None,
        command: None,
        actions: Some(vec![
            ServiceAction {
                action_type: ServiceActionType::Restart,
                delay: Duration::from_secs(5),
            };
            3
        ]),
    })?;
    eprintln!(
        "outpace service: installed '{SERVICE_NAME}' as {SERVICE_ACCOUNT}; start it with `sc.exe start {SERVICE_NAME}`"
    );
    Ok(())
}

/// `outpace service uninstall`: stop the service if it runs, then delete it.
pub fn uninstall() -> Result<(), BoxError> {
    let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)?;
    let service = manager.open_service(
        SERVICE_NAME,
        ServiceAccess::QUERY_STATUS | ServiceAccess::STOP | ServiceAccess::DELETE,
    )?;
    if service.query_status()?.current_state != ServiceState::Stopped {
        let _ = service.stop();
        let deadline = std::time::Instant::now() + STOP_WAIT_HINT + Duration::from_secs(5);
        while service.query_status()?.current_state != ServiceState::Stopped {
            if std::time::Instant::now() > deadline {
                return Err("service did not stop in time; not deleting it".into());
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    }
    service.delete()?;
    eprintln!("outpace service: removed '{SERVICE_NAME}'");
    Ok(())
}

fn service_main(_arguments: Vec<OsString>) {
    if let Err(error) = run_service() {
        crate::alog!("[service] failed: {error}");
    }
}

fn running_status(state: ServiceState, exit_code: ServiceExitCode) -> ServiceStatus {
    let accepted = if state == ServiceState::Running {
        ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN
    } else {
        ServiceControlAccept::empty()
    };
    ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: state,
        controls_accepted: accepted,
        exit_code,
        checkpoint: 0,
        wait_hint: if state == ServiceState::StopPending {
            STOP_WAIT_HINT
        } else {
            Duration::ZERO
        },
        process_id: None,
    }
}

fn run_service() -> Result<(), BoxError> {
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let stop_tx = Mutex::new(Some(stop_tx));
    let status = service_control_handler::register(SERVICE_NAME, move |control| match control {
        ServiceControl::Stop | ServiceControl::Shutdown => {
            if let Some(tx) = stop_tx.lock().ok().and_then(|mut tx| tx.take()) {
                let _ = tx.send(());
            }
            ServiceControlHandlerResult::NoError
        }
        ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
        _ => ServiceControlHandlerResult::NotImplemented,
    })?;

    let result = serve(status, stop_rx);
    let exit_code = match &result {
        Ok(()) => ServiceExitCode::Win32(0),
        Err(_) => ServiceExitCode::ServiceSpecific(1),
    };
    status.set_service_status(running_status(ServiceState::Stopped, exit_code))?;
    result
}

fn serve(
    status: service_control_handler::ServiceStatusHandle,
    stop: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), BoxError> {
    let mut config = crate::runtime::config_from_env()?;
    apply_service_data_dir(
        &mut config,
        std::env::var_os("OUTPACE_DATA_DIR").as_deref(),
        std::env::var_os("OUTPACE_CACHE_DIR").as_deref(),
        std::env::var_os("ProgramData").as_deref(),
    );
    let existed = config.data_dir.exists();
    std::fs::create_dir_all(&config.data_dir)?;
    // A directory this service creates is owned by it and is locked down before any key or
    // log is written. One that already exists (made by `service install`, or by an admin for
    // OUTPACE_DATA_DIR) is owned by its creator; re-applying is attempted, and a failure is
    // logged rather than fatal because the account has modify, not change-permission, rights.
    let restricted = restrict_data_dir(&config.data_dir);
    if !existed {
        restricted.as_ref().map_err(|e| e.to_string())?;
    }
    redirect_stderr(&config.data_dir.join("outpace.log"))?;
    crate::alog!(
        "[service] starting (data dir {})",
        config.data_dir.display()
    );
    if let Err(error) = restricted {
        crate::alog!("[service] warning: kept existing data dir permissions: {error}");
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let peers = crate::runtime::bootstrap_peers_from_env();
        let engine = crate::runtime::build_runtime(config, peers).await?;
        status.set_service_status(running_status(
            ServiceState::Running,
            ServiceExitCode::Win32(0),
        ))?;
        crate::runtime::serve_http_with_shutdown(engine, async move {
            let _ = stop.await;
            crate::alog!("[service] stop requested; draining HTTP connections");
            let _ = status.set_service_status(running_status(
                ServiceState::StopPending,
                ServiceExitCode::Win32(0),
            ));
        })
        .await
    })
}

/// Without `OUTPACE_DATA_DIR`, a service keeps its data in `%ProgramData%\outpace` (and the
/// disk cache under it unless `OUTPACE_CACHE_DIR` says otherwise).
fn apply_service_data_dir(
    config: &mut crate::config::Config,
    data_dir_env: Option<&OsStr>,
    cache_dir_env: Option<&OsStr>,
    program_data: Option<&OsStr>,
) {
    if data_dir_env.is_some() {
        return;
    }
    let Some(data_dir) = default_data_dir(program_data) else {
        return;
    };
    config.data_dir = data_dir;
    if cache_dir_env.is_none() {
        config.cache_dir = config.data_dir.join("cache");
    }
}

fn default_data_dir(program_data: Option<&OsStr>) -> Option<PathBuf> {
    program_data.map(|dir| PathBuf::from(dir).join("outpace"))
}

/// `icacls` arguments that replace `dir`'s DACL: inheritance removed, then full control for
/// SYSTEM (`S-1-5-18`) and Administrators (`S-1-5-32-544`) and modify for the service account,
/// all inherited by files and subdirectories. SIDs avoid localized group names.
fn restrict_data_dir_args(dir: &Path) -> Vec<OsString> {
    vec![
        dir.as_os_str().to_owned(),
        OsString::from("/inheritance:r"),
        OsString::from("/grant:r"),
        OsString::from("*S-1-5-18:(OI)(CI)F"),
        OsString::from("*S-1-5-32-544:(OI)(CI)F"),
        OsString::from(format!("{SERVICE_ACCOUNT}:(OI)(CI)M")),
        OsString::from("/Q"),
    ]
}

fn restrict_data_dir(dir: &Path) -> Result<(), BoxError> {
    // Absolute path, so a PATH entry cannot substitute another icacls.
    let system_root = std::env::var_os("SystemRoot").ok_or("SystemRoot is not set")?;
    let icacls = PathBuf::from(system_root)
        .join("System32")
        .join("icacls.exe");
    let output = std::process::Command::new(icacls)
        .args(restrict_data_dir_args(dir))
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "could not restrict permissions on {}: {}",
            dir.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(())
}

/// Point this process's standard error at `path` (appending), so `alog!` and `eprintln!`
/// output from a console-less service lands in a file. Rust's stderr looks the handle up on
/// every write, so replacing the process handle takes effect immediately.
fn redirect_stderr(path: &Path) -> Result<(), BoxError> {
    use std::os::windows::io::IntoRawHandle;
    use windows_sys::Win32::System::Console::{SetStdHandle, STD_ERROR_HANDLE};

    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let handle = file.into_raw_handle();
    // SAFETY: `handle` is a valid, owned file handle that is intentionally leaked for the
    // lifetime of the process; SetStdHandle only stores it.
    if unsafe { SetStdHandle(STD_ERROR_HANDLE, handle as _) } == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_data_dir_defaults_to_program_data() {
        let mut config = crate::config::Config::default();
        apply_service_data_dir(&mut config, None, None, Some(OsStr::new(r"C:\ProgramData")));
        assert_eq!(config.data_dir, PathBuf::from(r"C:\ProgramData\outpace"));
        assert_eq!(
            config.cache_dir,
            PathBuf::from(r"C:\ProgramData\outpace\cache")
        );
    }

    #[test]
    fn explicit_data_dir_wins() {
        let mut config = crate::config::Config::default();
        let before = config.data_dir.clone();
        apply_service_data_dir(
            &mut config,
            Some(OsStr::new(r"D:\outpace")),
            None,
            Some(OsStr::new(r"C:\ProgramData")),
        );
        assert_eq!(config.data_dir, before);
    }

    #[test]
    fn explicit_cache_dir_is_kept() {
        let mut config = crate::config::Config {
            cache_dir: PathBuf::from(r"E:\cache"),
            ..Default::default()
        };
        apply_service_data_dir(
            &mut config,
            None,
            Some(OsStr::new(r"E:\cache")),
            Some(OsStr::new(r"C:\ProgramData")),
        );
        assert_eq!(config.cache_dir, PathBuf::from(r"E:\cache"));
    }

    #[test]
    fn data_dir_acl_grants_only_system_admins_and_the_service() {
        let args = restrict_data_dir_args(Path::new(r"C:\ProgramData\outpace"));
        let args: Vec<_> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(args[0], r"C:\ProgramData\outpace");
        assert!(args.contains(&"/inheritance:r".to_owned()));
        assert!(args.contains(&"/grant:r".to_owned()));
        let grants: Vec<_> = args.iter().filter(|a| a.contains(":(OI)(CI)")).collect();
        assert_eq!(
            grants,
            [
                "*S-1-5-18:(OI)(CI)F",
                "*S-1-5-32-544:(OI)(CI)F",
                r"NT SERVICE\outpace:(OI)(CI)M"
            ]
        );
    }

    #[test]
    fn stop_pending_tells_the_scm_how_long_to_wait() {
        let status = running_status(ServiceState::StopPending, ServiceExitCode::Win32(0));
        assert_eq!(status.wait_hint, STOP_WAIT_HINT);
        assert!(status.controls_accepted.is_empty());
        let running = running_status(ServiceState::Running, ServiceExitCode::Win32(0));
        assert!(running
            .controls_accepted
            .contains(ServiceControlAccept::STOP));
    }
}
