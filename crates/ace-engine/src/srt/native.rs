//! Thread-confined reference-library boundary; no automatic listener startup.

use std::{cell::Cell, marker::PhantomData, rc::Rc, sync::Mutex};

static REFERENCES: Mutex<usize> = Mutex::new(0);
#[derive(Debug)]
pub enum NativeError {
    Initialization,
    Policy,
    Close,
    Cleanup,
    Listen,
    Accept,
    Receive,
}
#[repr(C)]
#[derive(Default)]
struct RawPolicy {
    receive_bytes: i32,
    send_bytes: i32,
    flow_packets: i32,
    key_bytes: i32,
    nonblocking_read: i32,
    nonblocking_write: i32,
}
unsafe extern "C" {
    fn op_srt_version() -> u32;
    fn op_srt_start() -> i32;
    fn op_srt_cleanup() -> i32;
    fn op_srt_receiver(latency: i32, key: *const u8, key_len: i32) -> i32;
    fn op_srt_policy(socket: i32, out: *mut RawPolicy) -> i32;
    fn op_srt_close(socket: i32) -> i32;
    fn op_srt_retired(socket: i32) -> i32;
    fn op_srt_listen(
        socket: i32,
        ip: *const std::ffi::c_char,
        port: u16,
        scope: u32,
        flow: u32,
        backlog: i32,
        bound_port: *mut u16,
    ) -> i32;
    fn op_srt_accept(listener: i32, accepted: *mut i32) -> i32;
    fn op_srt_stream_id(socket: i32, buffer: *mut u8, capacity: i32, length: *mut i32) -> i32;
    fn op_srt_encrypted(socket: i32) -> i32;
    fn op_srt_receive(socket: i32, buffer: *mut u8, capacity: i32, length: *mut i32) -> i32;
}
pub struct NativeLibrary {
    live: bool,
    failed: Cell<bool>,
    _thread: PhantomData<Rc<()>>,
}
pub struct NativeSocket<'a> {
    library: &'a NativeLibrary,
    socket: Option<i32>,
}
/// A closed socket still occupies an admission slot until the SDK removes it.
/// Borrowing the library keeps its collector alive while the owner polls this token.
pub struct NativeRetirement<'a> {
    _library: &'a NativeLibrary,
    socket: i32,
}
impl NativeRetirement<'_> {
    pub fn confirmed(&self) -> bool {
        (unsafe { op_srt_retired(self.socket) }) == 1
    }
}
#[derive(Clone, Copy)]
pub struct Policy {
    pub receive_bytes: i32,
    pub send_bytes: i32,
    pub flow_packets: i32,
    pub key_bytes: i32,
    pub nonblocking_read: bool,
    pub nonblocking_write: bool,
    pub encrypted: bool,
}
impl Policy {
    fn valid_receiver(&self) -> bool {
        (1_000_000..=1_048_576).contains(&self.receive_bytes)
            && (1_000_000..=1_048_576).contains(&self.send_bytes)
            && self.flow_packets == 1024
            && self.key_bytes == 0
            && self.nonblocking_read
            && self.nonblocking_write
    }
}
#[derive(Debug, PartialEq, Eq)]
pub enum ReadStatus {
    WouldBlock,
    Data(usize),
    Closed,
}
impl NativeLibrary {
    /// Call only on the owned native thread. Rc marker prevents transfer or sharing.
    pub fn acquire() -> Result<Self, NativeError> {
        let mut refs = REFERENCES.lock().map_err(|_| NativeError::Initialization)?;
        if unsafe { op_srt_version() } < 0x010507 {
            return Err(NativeError::Initialization);
        }
        if *refs == 0 && unsafe { op_srt_start() } != 0 {
            return Err(NativeError::Initialization);
        }
        *refs += 1;
        Ok(Self {
            live: true,
            failed: Cell::new(false),
            _thread: PhantomData,
        })
    }
    pub fn receiver(&self, latency: i32, key: &[u8]) -> Result<NativeSocket<'_>, NativeError> {
        if !(20..=2000).contains(&latency) || !(10..=79).contains(&key.len()) || key.contains(&0) {
            return Err(NativeError::Policy);
        }
        let socket = unsafe { op_srt_receiver(latency, key.as_ptr(), key.len() as i32) };
        if socket < 0 {
            return Err(NativeError::Policy);
        }
        let socket = NativeSocket {
            library: self,
            socket: Some(socket),
        };
        if !socket.policy()?.valid_receiver() {
            return Err(NativeError::Policy);
        }
        Ok(socket)
    }
    fn release(&mut self) -> Result<(), NativeError> {
        if !self.live {
            return Ok(());
        }
        let mut refs = REFERENCES.lock().map_err(|_| NativeError::Cleanup)?;
        self.live = false;
        *refs -= 1;
        if *refs == 0 && unsafe { op_srt_cleanup() } != 0 {
            return Err(NativeError::Cleanup);
        }
        if self.failed.get() {
            return Err(NativeError::Close);
        }
        Ok(())
    }
    pub fn shutdown(mut self) -> Result<(), NativeError> {
        self.release()
    }
}
impl Drop for NativeLibrary {
    fn drop(&mut self) {
        let _ = self.release();
    }
}
impl<'a> NativeSocket<'a> {
    pub fn listen(
        &mut self,
        mut address: std::net::SocketAddr,
        backlog: usize,
    ) -> Result<std::net::SocketAddr, NativeError> {
        if !(1..=16).contains(&backlog) {
            return Err(NativeError::Listen);
        }
        let ip =
            std::ffi::CString::new(address.ip().to_string()).map_err(|_| NativeError::Listen)?;
        let (scope, flow) = match address {
            std::net::SocketAddr::V6(v6) => (v6.scope_id(), v6.flowinfo()),
            _ => (0, 0),
        };
        let mut port = 0;
        if unsafe {
            op_srt_listen(
                self.socket.ok_or(NativeError::Close)?,
                ip.as_ptr(),
                address.port(),
                scope,
                flow,
                backlog as i32,
                &mut port,
            )
        } != 0
        {
            return Err(NativeError::Listen);
        }
        address.set_port(port);
        Ok(address)
    }
    pub fn accept(&self) -> Result<Option<NativeSocket<'a>>, NativeError> {
        let mut socket = -1;
        match unsafe { op_srt_accept(self.socket.ok_or(NativeError::Close)?, &mut socket) } {
            0 => Ok(None),
            1 => {
                let accepted = NativeSocket {
                    library: self.library,
                    socket: Some(socket),
                };
                if !accepted.policy()?.encrypted {
                    return Err(NativeError::Policy);
                }
                Ok(Some(accepted))
            }
            _ => Err(NativeError::Accept),
        }
    }
    pub fn stream_id(&self) -> Result<String, NativeError> {
        let mut buffer = [0u8; 65];
        let mut length = 0;
        if unsafe {
            op_srt_stream_id(
                self.socket.ok_or(NativeError::Close)?,
                buffer.as_mut_ptr(),
                buffer.len() as i32,
                &mut length,
            )
        } != 0
        {
            return Err(NativeError::Policy);
        }
        let mut bytes = buffer
            .get(..usize::try_from(length).map_err(|_| NativeError::Policy)?)
            .ok_or(NativeError::Policy)?;
        if bytes.last() == Some(&0) {
            bytes = &bytes[..bytes.len() - 1];
        }
        let id = std::str::from_utf8(bytes).map_err(|_| NativeError::Policy)?;
        if !crate::broadcast::valid_broadcast_name(id) {
            return Err(NativeError::Policy);
        }
        Ok(id.to_owned())
    }
    pub fn receive(&self, buffer: &mut [u8]) -> Result<ReadStatus, NativeError> {
        if buffer.is_empty() || buffer.len() > 65536 {
            return Err(NativeError::Receive);
        }
        let mut length = 0;
        match unsafe {
            op_srt_receive(
                self.socket.ok_or(NativeError::Close)?,
                buffer.as_mut_ptr(),
                buffer.len() as i32,
                &mut length,
            )
        } {
            0 => Ok(ReadStatus::WouldBlock),
            1 if length > 0 && length as usize <= buffer.len() => {
                Ok(ReadStatus::Data(length as usize))
            }
            2 => Ok(ReadStatus::Closed),
            _ => Err(NativeError::Receive),
        }
    }
    pub fn policy(&self) -> Result<Policy, NativeError> {
        let mut raw = RawPolicy::default();
        if unsafe { op_srt_policy(self.socket.ok_or(NativeError::Close)?, &mut raw) } != 0 {
            return Err(NativeError::Policy);
        }
        Ok(Policy {
            receive_bytes: raw.receive_bytes,
            send_bytes: raw.send_bytes,
            flow_packets: raw.flow_packets,
            key_bytes: raw.key_bytes,
            nonblocking_read: raw.nonblocking_read != 0,
            nonblocking_write: raw.nonblocking_write != 0,
            encrypted: match unsafe { op_srt_encrypted(self.socket.ok_or(NativeError::Close)?) } {
                0 => false,
                1 => true,
                _ => return Err(NativeError::Policy),
            },
        })
    }
    pub fn begin_close(&mut self) -> Result<NativeRetirement<'a>, NativeError> {
        let socket = self.socket.take().ok_or(NativeError::Close)?;
        if unsafe { op_srt_close(socket) } != 0 {
            // The SDK's failure leaves ownership uncertain. Never reuse this id
            // or acknowledge retirement; final owned library cleanup reports failure.
            self.library.failed.set(true);
            return Err(NativeError::Close);
        }
        Ok(NativeRetirement {
            _library: self.library,
            socket,
        })
    }
    pub fn close(&mut self) -> Result<(), NativeError> {
        if self.socket.is_none() {
            return Ok(());
        }
        // Drop fallback requests close only. Normal native owner paths retain and
        // observe begin_close's token before releasing slots/producer completion.
        self.begin_close().map(|_| ())
    }
}
impl Drop for NativeSocket<'_> {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receiver_readback_rejects_oversized_buffers_and_blocking_or_wrong_flow_policy() {
        let valid = Policy {
            receive_bytes: 1_047_552,
            send_bytes: 1_047_552,
            flow_packets: 1024,
            key_bytes: 0,
            nonblocking_read: true,
            nonblocking_write: true,
            encrypted: false,
        };
        assert!(valid.valid_receiver());
        for invalid in [
            Policy {
                receive_bytes: 1_048_577,
                ..valid
            },
            Policy {
                send_bytes: 1_048_577,
                ..valid
            },
            Policy {
                receive_bytes: 0,
                ..valid
            },
            Policy {
                send_bytes: 0,
                ..valid
            },
            Policy {
                flow_packets: 2048,
                ..valid
            },
            Policy {
                key_bytes: 32,
                ..valid
            },
            Policy {
                nonblocking_read: false,
                ..valid
            },
            Policy {
                nonblocking_write: false,
                ..valid
            },
        ] {
            assert!(
                !invalid.valid_receiver(),
                "an incompatible native readback must fail listener construction"
            );
        }
    }

    #[test]
    fn fixed_native_receiver_policy_is_nonblocking_and_bounded() {
        std::thread::spawn(|| {
            let library = NativeLibrary::acquire().unwrap();
            let socket = library.receiver(120, b"synthetic-test-passphrase").unwrap();
            let policy = socket.policy().unwrap();
            assert!(policy.nonblocking_read && policy.nonblocking_write);
            assert_eq!(policy.flow_packets, 1024);
            // Native byte buffers are quantized to whole payload slots.
            assert!((1_000_000..=1_048_576).contains(&policy.receive_bytes));
            assert!((1_000_000..=1_048_576).contains(&policy.send_bytes));
            // Reference unidirectional API: sender selects the key size.
            assert_eq!(policy.key_bytes, 0);
            drop(socket);
            library.shutdown().unwrap();
        })
        .join()
        .unwrap();
    }

    #[test]
    #[ignore = "requires FFmpeg with SRT support; generated loopback media only"]
    fn ffmpeg_loopback_requires_encryption_and_preserves_complete_messages() {
        use std::{
            process::{Command, Stdio},
            time::{Duration, Instant},
        };
        assert_eq!(std::env::var("OUTPACE_TEST_SRT_FFMPEG").as_deref(), Ok("1"));
        struct OwnedChild(std::process::Child);
        impl Drop for OwnedChild {
            fn drop(&mut self) {
                if self.0.try_wait().ok().flatten().is_none() {
                    let _ = self.0.kill();
                }
                let _ = self.0.wait();
            }
        }
        std::thread::spawn(|| {
            let library = NativeLibrary::acquire().unwrap();
            let mut listener = library.receiver(120, b"synthetic-test-passphrase").unwrap();
            let address = listener.listen("127.0.0.1:0".parse().unwrap(), 4).unwrap();
            for (secret, key_bytes, should_connect) in [
                (None, 16, false),
                (Some("synthetic-wrong-passphrase"), 16, false),
                (Some("synthetic-test-passphrase"), 16, true),
                (Some("synthetic-test-passphrase"), 24, true),
                (Some("synthetic-test-passphrase"), 32, true),
            ] {
                let mut url = format!("srt://{address}?mode=caller&streamid=synthetic&pkt_size=1316&connect_timeout=500&pbkeylen={key_bytes}");
                if let Some(secret) = secret { url.push_str(&format!("&passphrase={secret}")); }
                let mut child = OwnedChild(Command::new("ffmpeg").args(["-hide_banner","-loglevel","error","-re","-f","lavfi","-i","testsrc=size=64x64:rate=10","-an","-t","2","-c:v","mpeg2video","-f","mpegts",&url])
                    .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap());
                let deadline = Instant::now() + Duration::from_secs(4);
                let mut accepted = None;
                while Instant::now() < deadline {
                    match listener.accept() {
                        Ok(Some(socket)) => { accepted = Some(socket); break; },
                        Ok(None) => {},
                        Err(error) => panic!("listener failed: {error:?}"),
                    }
                    if child.0.try_wait().unwrap().is_some() { break; }
                    std::thread::sleep(Duration::from_millis(5));
                }
                let observed = if let Some(socket) = accepted.as_mut() {
                    let identity = socket.stream_id().unwrap();
                    let encrypted = socket.policy().unwrap().encrypted;
                    let mut tiny = [0u8; 8];
                    let mut rejected_small_buffer = false;
                    while Instant::now() < deadline {
                        match socket.receive(&mut tiny) {
                            Err(NativeError::Receive) => { rejected_small_buffer = true; break; },
                            Ok(ReadStatus::WouldBlock) => std::thread::sleep(Duration::from_millis(5)),
                            _ => break,
                        }
                    }
                    assert!(rejected_small_buffer, "an undersized buffer must error, never return a successful truncated message");
                    let mut bytes = [0u8; 65536];
                    let mut size = 0;
                    while Instant::now() < deadline {
                        match socket.receive(&mut bytes).unwrap() {
                            ReadStatus::Data(n) => { size=n;break; },
                            ReadStatus::WouldBlock => std::thread::sleep(Duration::from_millis(5)),
                            ReadStatus::Closed => break,
                        }
                    }
                    let sync = bytes[..size].as_chunks::<188>().0.iter().all(|packet| packet[0] == 0x47);
                    Some((identity, encrypted, size, sync))
                } else { None };
                if child.0.try_wait().unwrap().is_none() { child.0.kill().unwrap(); }
                child.0.wait().unwrap();
                let retirement = accepted.as_mut().map(|socket| socket.begin_close().unwrap());
                drop(accepted);
                if let Some(retirement) = retirement {
                    let until = Instant::now() + Duration::from_secs(4);
                    while !retirement.confirmed() && Instant::now() < until { std::thread::sleep(Duration::from_millis(5)); }
                    assert!(retirement.confirmed(), "accepted socket must leave SDK closed table before slot reuse");
                }
                if should_connect {
                    let (identity, encrypted, size, sync) = observed.expect("correct-key publisher accepted");
                    assert_eq!(identity, "synthetic");
                    assert!(encrypted);
                    assert!(size > 0 && size % 188 == 0);
                    assert!(sync, "decrypted messages must preserve every TS packet boundary");
                } else { assert!(observed.is_none(), "plaintext and wrong keys must never be accepted"); }
            }
            listener.close().unwrap();
            drop(listener);
            library.shutdown().unwrap();
        }).join().unwrap();
    }

    #[test]
    fn idle_nonblocking_native_polls_do_not_emit_raw_stderr() {
        use std::io::Read;
        if std::env::var_os("OUTPACE_TEST_SRT_QUIET_CHILD").is_some() {
            let library = NativeLibrary::acquire().unwrap();
            let mut listener = library.receiver(120, b"synthetic-test-passphrase").unwrap();
            listener.listen("127.0.0.1:0".parse().unwrap(), 4).unwrap();
            for _ in 0..32 {
                assert!(listener.accept().unwrap().is_none());
            }
            listener.close().unwrap();
            drop(listener);
            library.shutdown().unwrap();
            return;
        }
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "srt::native::tests::idle_nonblocking_native_polls_do_not_emit_raw_stderr",
                "--exact",
                "--nocapture",
            ])
            .env("OUTPACE_TEST_SRT_QUIET_CHILD", "1")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
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
        let mut stderr = Vec::new();
        child
            .stderr
            .take()
            .unwrap()
            .take(16384)
            .read_to_end(&mut stderr)
            .unwrap();
        assert!(
            status.is_some_and(|s| s.success()),
            "owned native fixture must terminate and reap"
        );
        assert!(
            stderr.is_empty(),
            "idle polls emitted {} raw stderr bytes",
            stderr.len()
        );
    }

    #[test]
    fn native_retirement_waits_for_sdk_socket_removal() {
        std::thread::spawn(|| {
            let library = NativeLibrary::acquire().unwrap();
            let mut socket = library.receiver(120, b"synthetic-test-passphrase").unwrap();
            let retirement = socket.begin_close().unwrap();
            drop(socket);
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(4);
            while !retirement.confirmed() && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            let confirmed = retirement.confirmed();
            library.shutdown().unwrap();
            assert!(
                confirmed,
                "SDK socket removal must be observed before retirement confirmation"
            );
        })
        .join()
        .unwrap();
    }

    #[test]
    fn native_rejects_invalid_secret_and_latency_before_creating_socket() {
        std::thread::spawn(|| {
            let library = NativeLibrary::acquire().unwrap();
            for secret in [&b"short"[..], &[b'x'; 80][..], &b"invalid\0passphrase"[..]] {
                assert!(library.receiver(120, secret).is_err());
            }
            for latency in [19, 2001] {
                assert!(library
                    .receiver(latency, b"synthetic-test-passphrase")
                    .is_err());
            }
        })
        .join()
        .unwrap();
    }
}
