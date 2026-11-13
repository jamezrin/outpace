//! Private data directory for the Windows service.
//!
//! The data directory holds the node identity seed and broadcast signing keys. Under
//! `%ProgramData%` a plain `create_dir_all` would inherit an ACL that lets every local user
//! read it, and any local user may pre-create the path (as a directory they own, or as a
//! junction to somewhere else) before the service ever runs. So the service never trusts
//! what it finds there:
//!
//! * a missing directory is created atomically with a protected security descriptor:
//!   SYSTEM and Administrators full control, the service account modify, nothing inherited;
//! * an existing one must be a real directory (not a reparse point), owned by SYSTEM,
//!   Administrators or the service account, with a protected DACL whose allow entries name
//!   only those three. Anything else is refused, never repaired in place.
//!
//! The checks run on one handle, opened without following reparse points and without
//! `FILE_SHARE_DELETE`. The caller keeps that handle ([`PrivateDir`]) for as long as it uses
//! the directory, so the verified entry cannot be renamed, deleted or swapped for a junction
//! between the check and every later open by path.

use std::ffi::OsStr;
use std::os::windows::ffi::OsStrExt;
use std::path::Path;

use windows_sys::Win32::Foundation::{
    CloseHandle, LocalFree, ERROR_ALREADY_EXISTS, HANDLE, INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, GetSecurityInfo,
    SDDL_REVISION_1, SE_FILE_OBJECT,
};
use windows_sys::Win32::Security::{
    CreateWellKnownSid, EqualSid, GetAce, GetSecurityDescriptorControl, LookupAccountNameW,
    WinBuiltinAdministratorsSid, WinLocalSystemSid, ACCESS_ALLOWED_ACE, ACE_HEADER, ACL,
    DACL_SECURITY_INFORMATION, OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID,
    SECURITY_ATTRIBUTES, SECURITY_MAX_SID_SIZE, SE_DACL_PROTECTED, WELL_KNOWN_SID_TYPE,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateDirectoryW, CreateFileW, GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS,
    FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES, FILE_SHARE_READ, FILE_SHARE_WRITE,
    OPEN_EXISTING, READ_CONTROL,
};
use windows_sys::Win32::System::SystemServices::{ACCESS_ALLOWED_ACE_TYPE, ACCESS_DENIED_ACE_TYPE};

/// "Modify" (icacls `M`): read, write, execute and delete, but not change permissions or owner.
const MODIFY: u32 = 0x0013_01bf;

type Error = String;

/// An open, verified handle on the private data directory. While it lives, the directory
/// entry cannot be deleted, renamed or replaced (no `FILE_SHARE_DELETE`).
pub struct PrivateDir(HANDLE);

impl Drop for PrivateDir {
    fn drop(&mut self) {
        // SAFETY: the handle came from CreateFileW and is closed exactly once.
        unsafe { CloseHandle(self.0) };
    }
}

// SAFETY: a kernel handle may be used and closed from any thread.
unsafe impl Send for PrivateDir {}

fn wide(value: &OsStr) -> Vec<u16> {
    value.encode_wide().chain(std::iter::once(0)).collect()
}

struct Sid(Vec<u8>);

impl Sid {
    fn as_psid(&self) -> PSID {
        self.0.as_ptr() as PSID
    }

    fn well_known(kind: WELL_KNOWN_SID_TYPE) -> Result<Self, Error> {
        let mut buf = vec![0u8; SECURITY_MAX_SID_SIZE as usize];
        let mut size = buf.len() as u32;
        // SAFETY: `buf` is writable for `size` bytes; no domain SID is needed for these kinds.
        let ok = unsafe {
            CreateWellKnownSid(
                kind,
                std::ptr::null_mut(),
                buf.as_mut_ptr() as PSID,
                &mut size,
            )
        };
        if ok == 0 {
            return Err(format!(
                "CreateWellKnownSid: {}",
                std::io::Error::last_os_error()
            ));
        }
        buf.truncate(size as usize);
        Ok(Self(buf))
    }

    /// The virtual account SID of a service, e.g. `NT SERVICE\outpace`.
    fn account(name: &str) -> Result<Self, Error> {
        let name = wide(OsStr::new(name));
        let mut sid = vec![0u8; SECURITY_MAX_SID_SIZE as usize];
        let mut sid_size = sid.len() as u32;
        let mut domain = vec![0u16; 256];
        let mut domain_size = domain.len() as u32;
        let mut kind = 0;
        // SAFETY: every buffer is writable for the length passed alongside it.
        let ok = unsafe {
            LookupAccountNameW(
                std::ptr::null(),
                name.as_ptr(),
                sid.as_mut_ptr() as PSID,
                &mut sid_size,
                domain.as_mut_ptr(),
                &mut domain_size,
                &mut kind,
            )
        };
        if ok == 0 {
            return Err(format!(
                "LookupAccountName: {}",
                std::io::Error::last_os_error()
            ));
        }
        sid.truncate(sid_size as usize);
        Ok(Self(sid))
    }

    fn to_sddl(&self) -> Result<String, Error> {
        let mut text = std::ptr::null_mut();
        // SAFETY: `self` holds a valid SID; on success `text` is a LocalAlloc'd wide string.
        if unsafe { ConvertSidToStringSidW(self.as_psid(), &mut text) } == 0 {
            return Err(format!(
                "ConvertSidToStringSid: {}",
                std::io::Error::last_os_error()
            ));
        }
        // SAFETY: `text` is NUL-terminated; it is freed exactly once below.
        let value = unsafe {
            let len = (0..).take_while(|&i| *text.add(i) != 0).count();
            String::from_utf16_lossy(std::slice::from_raw_parts(text, len))
        };
        unsafe { LocalFree(text as _) };
        Ok(value)
    }
}

/// SDDL for the data directory: protected DACL, full control for SYSTEM and Administrators,
/// modify for the service account, all inherited by children. With `owner_admins`, the owner
/// is set to Administrators (possible from an elevated prompt); otherwise it is the creator.
pub fn data_dir_sddl(service_sid: &str, owner_admins: bool) -> String {
    let owner = if owner_admins { "O:BA" } else { "" };
    format!("{owner}D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;{MODIFY:#x};;;{service_sid})")
}

/// Create `dir` with the private security descriptor, or check an existing one, and return a
/// handle that pins it. Fails closed: a directory that is not provably private is an error.
pub fn ensure_private_dir(
    dir: &Path,
    service_account: &str,
    owner_admins: bool,
) -> Result<PrivateDir, Error> {
    let service = Sid::account(service_account)?;
    if let Some(parent) = dir.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    }
    let sddl = wide(OsStr::new(&data_dir_sddl(
        &service.to_sddl()?,
        owner_admins,
    )));
    let mut descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: `sddl` is NUL-terminated; on success `descriptor` is LocalAlloc'd and freed below.
    let ok = unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_ptr(),
            SDDL_REVISION_1,
            &mut descriptor,
            std::ptr::null_mut(),
        )
    };
    if ok == 0 {
        return Err(format!(
            "build security descriptor: {}",
            std::io::Error::last_os_error()
        ));
    }
    let attributes = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: descriptor,
        bInheritHandle: 0,
    };
    let path = wide(dir.as_os_str());
    // SAFETY: `path` is NUL-terminated and `attributes` points at a valid descriptor.
    let created = unsafe { CreateDirectoryW(path.as_ptr(), &attributes) } != 0;
    let error = std::io::Error::last_os_error();
    unsafe { LocalFree(descriptor as _) };
    if !created && error.raw_os_error() != Some(ERROR_ALREADY_EXISTS as i32) {
        return Err(format!("create {}: {error}", dir.display()));
    }
    verify_private_dir(dir, &service).map_err(|reason| {
        format!(
            "refusing data directory {}: {reason}. It holds the node identity and broadcast keys; \
             inspect it, remove it, and let `outpace service install` create it",
            dir.display()
        )
    })
}

fn open_dir(dir: &Path) -> Result<PrivateDir, Error> {
    let path = wide(dir.as_os_str());
    // SAFETY: `path` is NUL-terminated; the returned handle is owned by PrivateDir.
    let handle = unsafe {
        CreateFileW(
            path.as_ptr(),
            READ_CONTROL | FILE_READ_ATTRIBUTES,
            // Deliberately no FILE_SHARE_DELETE: nobody may rename or delete it while held.
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            std::ptr::null(),
            OPEN_EXISTING,
            // Directories need BACKUP_SEMANTICS; OPEN_REPARSE_POINT opens a junction itself
            // instead of its target, so it is detected below rather than followed.
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(format!("open: {}", std::io::Error::last_os_error()));
    }
    Ok(PrivateDir(handle))
}

fn verify_private_dir(dir: &Path, service: &Sid) -> Result<PrivateDir, Error> {
    let pinned = open_dir(dir)?;
    // SAFETY: zeroed is a valid BY_HANDLE_FILE_INFORMATION; the handle is open.
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    if unsafe { GetFileInformationByHandle(pinned.0, &mut info) } == 0 {
        return Err(format!(
            "read attributes: {}",
            std::io::Error::last_os_error()
        ));
    }
    if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err("it is a link or junction".into());
    }
    if info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY == 0 {
        return Err("it is not a directory".into());
    }
    let trusted = [
        Sid::well_known(WinLocalSystemSid)?,
        Sid::well_known(WinBuiltinAdministratorsSid)?,
        Sid(service.0.clone()),
    ];
    // SAFETY: both pointers refer to valid SIDs for the duration of the call.
    let is_trusted = |sid: PSID| {
        trusted
            .iter()
            .any(|t| unsafe { EqualSid(t.as_psid(), sid) } != 0)
    };

    let mut owner: PSID = std::ptr::null_mut();
    let mut dacl: *mut ACL = std::ptr::null_mut();
    let mut descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: out-pointers are valid; owner and dacl point into `descriptor`, freed below.
    let status = unsafe {
        GetSecurityInfo(
            pinned.0,
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &mut owner,
            std::ptr::null_mut(),
            &mut dacl,
            std::ptr::null_mut(),
            &mut descriptor,
        )
    };
    if status != 0 {
        return Err(format!(
            "read permissions: {}",
            std::io::Error::from_raw_os_error(status as i32)
        ));
    }
    let verdict = (|| {
        if owner.is_null() || !is_trusted(owner) {
            return Err("it is owned by an untrusted account".to_string());
        }
        let mut control = 0u16;
        let mut revision = 0u32;
        // SAFETY: `descriptor` is the valid descriptor returned above.
        if unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) } == 0
            || control & SE_DACL_PROTECTED == 0
        {
            return Err("its permissions are inherited from the parent".to_string());
        }
        if dacl.is_null() {
            return Err("it has no access list (everyone has access)".to_string());
        }
        // SAFETY: `dacl` is non-null and part of `descriptor`.
        let count = unsafe { (*dacl).AceCount };
        for index in 0..u32::from(count) {
            let mut ace = std::ptr::null_mut();
            // SAFETY: index < AceCount; `ace` receives a pointer into the ACL.
            if unsafe { GetAce(dacl, index, &mut ace) } == 0 {
                return Err("its access list is unreadable".to_string());
            }
            // SAFETY: every ACE starts with an ACE_HEADER.
            let kind = u32::from(unsafe { (*(ace as *const ACE_HEADER)).AceType });
            if kind == ACCESS_DENIED_ACE_TYPE {
                continue;
            }
            if kind != ACCESS_ALLOWED_ACE_TYPE {
                return Err(format!(
                    "its access list has an unexpected entry type {kind}"
                ));
            }
            // SAFETY: an ACCESS_ALLOWED_ACE stores its SID starting at `SidStart`.
            let sid = unsafe { std::ptr::addr_of!((*(ace as *const ACCESS_ALLOWED_ACE)).SidStart) };
            if !is_trusted(sid as PSID) {
                return Err("it grants access to accounts other than SYSTEM, Administrators and the service".to_string());
            }
        }
        Ok(())
    })();
    unsafe { LocalFree(descriptor as _) };
    verdict.map(|()| pinned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sddl_is_protected_and_names_only_trusted_principals() {
        let sddl = data_dir_sddl("S-1-5-80-1-2-3-4-5", true);
        assert_eq!(
            sddl,
            "O:BAD:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;0x1301bf;;;S-1-5-80-1-2-3-4-5)"
        );
        assert!(data_dir_sddl("S-1-5-80-9", false).starts_with("D:P("));
    }

    #[test]
    fn well_known_sids_resolve() {
        assert_eq!(
            Sid::well_known(WinLocalSystemSid)
                .unwrap()
                .to_sddl()
                .unwrap(),
            "S-1-5-18"
        );
        assert_eq!(
            Sid::well_known(WinBuiltinAdministratorsSid)
                .unwrap()
                .to_sddl()
                .unwrap(),
            "S-1-5-32-544"
        );
    }

    #[test]
    fn a_directory_inheriting_its_parent_acl_is_refused() {
        // A plain temp directory inherits its parent's ACL, like a pre-created ProgramData dir.
        let dir = std::env::temp_dir().join(format!("outpace-acl-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let system = Sid::well_known(WinLocalSystemSid).unwrap();
        let verdict = verify_private_dir(&dir, &system);
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(verdict.is_err(), "inherited ACL must be refused");
    }
}
