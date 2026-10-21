//! Direct-binary process boundary. Callers must keep this supervisor in the manager's
//! cleanup tracker: dropping a caller future is not an asynchronous reap operation.
use crate::hls::{GenerationId, HlsFailure, HlsPackager, HlsStatus};
use ace_media::audio::{AudioCodec, LayoutDecision, LiveAudioSelection, TransportLayout};
use ace_media::mpegts::{mpeg_crc32, ts_pid, VideoAccessPointState};
use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::process::Command;
use tokio::sync::{mpsc, watch, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Instant;

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../tests/support/hls_audio.rs"]
pub(crate) mod test_fixtures;

pub const INPUT_BYTES: usize = 1_048_576;
const STDOUT_READ: usize = 32_768;
const STDERR_TAIL: usize = 8_192;
const TS: usize = 188;
const STALL: Duration = Duration::from_secs(45);

#[cfg(test)]
static PROBE_TEST_COUNTS: (
    std::sync::atomic::AtomicUsize,
    std::sync::atomic::AtomicUsize,
    std::sync::atomic::AtomicUsize,
    std::sync::atomic::AtomicUsize,
) = (
    std::sync::atomic::AtomicUsize::new(0),
    std::sync::atomic::AtomicUsize::new(0),
    std::sync::atomic::AtomicUsize::new(0),
    std::sync::atomic::AtomicUsize::new(0),
);

#[derive(Clone, Debug)]
pub struct PreparedNormalizer {
    pub executable: PathBuf,
}

/// All values derived from media are numeric, validated transport fields. No shell,
/// URLs, arbitrary arguments, descriptors or source metadata reach the command.
pub fn build_command(prepared: &PreparedNormalizer, layout: &TransportLayout) -> Command {
    let mut command = Command::new(&prepared.executable);
    command.args([
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-copyts",
        "-protocol_whitelist",
        "pipe",
        "-f",
        "mpegts",
        "-probesize",
        "1048576",
        "-analyzeduration",
        "1000000",
        "-i",
        "pipe:0",
        "-map",
    ]);
    command.arg(format!("0:i:{}", layout.video_pid));
    command.arg("-map").arg(format!(
        "0:i:{}",
        layout.audio.as_ref().map_or(0, |a| a.pid)
    ));
    command.args([
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-profile:a",
        "aac_low",
        "-ac",
        "2",
        "-ar",
        "48000",
        "-b:a",
        "128k",
        "-sn",
        "-dn",
        "-map_metadata",
        "-1",
        "-map_chapters",
        "-1",
        "-streamid",
        "0:256",
        "-streamid",
        "1:257",
        "-mpegts_pmt_start_pid",
        "4096",
        "-mpegts_service_id",
    ]);
    command.arg(layout.program_id.to_string());
    command
        .arg("-mpegts_transport_stream_id")
        .arg(layout.transport_stream_id.to_string());
    command.args([
        "-mpegts_flags",
        "+resend_headers+pat_pmt_at_frames",
        "-mpegts_copyts",
        "1",
        "-avoid_negative_ts",
        "disabled",
        "-fps_mode",
        "passthrough",
        "-f",
        "mpegts",
        "pipe:1",
    ]);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

pub struct ChildAdmission {
    slots: Arc<Semaphore>,
}
impl ChildAdmission {
    pub fn global() -> Arc<Self> {
        static GLOBAL: OnceLock<Arc<ChildAdmission>> = OnceLock::new();
        GLOBAL
            .get_or_init(|| {
                Arc::new(Self {
                    slots: Arc::new(Semaphore::new(4)),
                })
            })
            .clone()
    }
    pub fn try_acquire(&self) -> Result<OwnedSemaphorePermit, HlsFailure> {
        self.slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| HlsFailure::Admission)
    }
}

/// Shared by classification, replay, the input channel and the active writer.
#[derive(Clone)]
pub struct InputBudget {
    bytes: Arc<Semaphore>,
}
impl Default for InputBudget {
    fn default() -> Self {
        Self::new()
    }
}
impl InputBudget {
    pub fn new() -> Self {
        Self {
            bytes: Arc::new(Semaphore::new(INPUT_BYTES)),
        }
    }

    /// Ownership is charged by allocation capacity, never logical slice length.
    /// An already-owned allocation cannot wait outside the budget; exhaustion is
    /// overflow. Producers needing backpressure reserve before copying via acquire_copy.
    pub async fn acquire_owned(
        &self,
        bytes: Vec<u8>,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<BudgetedInput, HlsFailure> {
        if *cancel.borrow() {
            return Err(HlsFailure::Pipe);
        }
        let capacity = bytes.capacity();
        if capacity > INPUT_BYTES {
            return Err(HlsFailure::InputOverflow);
        }
        let permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(capacity as u32)
            .map_err(|_| HlsFailure::InputOverflow)?;
        Ok(BudgetedInput { bytes, permit })
    }

    /// Reserve before detaching a broadcast view. This must not wait while the
    /// caller owns an unaccounted backing allocation, even for a tiny view.
    pub fn try_copy(&self, bytes: &[u8]) -> Result<BudgetedInput, HlsFailure> {
        if bytes.len() > INPUT_BYTES {
            return Err(HlsFailure::InputOverflow);
        }
        let permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes.len() as u32)
            .map_err(|_| HlsFailure::InputOverflow)?;
        let bytes = bytes.to_vec().into_boxed_slice().into_vec();
        Ok(BudgetedInput { bytes, permit })
    }

    /// Reserve before copying a borrowed slice whose backing storage is already
    /// bounded/accounted by the caller throughout the wait.
    pub async fn acquire_copy(
        &self,
        bytes: &[u8],
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<BudgetedInput, HlsFailure> {
        if bytes.len() > INPUT_BYTES {
            return Err(HlsFailure::InputOverflow);
        }
        if *cancel.borrow() {
            return Err(HlsFailure::Pipe);
        }
        let permit = tokio::select! {
            biased;
            _ = cancelled(cancel) => return Err(HlsFailure::Pipe),
            p = self.bytes.clone().acquire_many_owned(bytes.len() as u32) => p.map_err(|_| HlsFailure::InputOverflow)?,
        };
        // Boxed slice fixes capacity to the charged length, including under allocators
        // that reserve more usable memory than requested internally.
        let bytes = bytes.to_vec().into_boxed_slice().into_vec();
        Ok(BudgetedInput { bytes, permit })
    }
    pub fn charged(&self) -> usize {
        INPUT_BYTES - self.bytes.available_permits()
    }
}

pub struct BudgetedInput {
    // Drop data before its permits. Partially written Vec storage stays fully charged.
    bytes: Vec<u8>,
    permit: OwnedSemaphorePermit,
}
impl BudgetedInput {
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
    pub fn len(&self) -> usize {
        self.bytes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
    pub fn charged(&self) -> usize {
        self.permit.num_permits()
    }
}

pub(super) async fn cancelled(cancel: &mut watch::Receiver<bool>) {
    loop {
        if *cancel.borrow_and_update() {
            return;
        }
        if cancel.changed().await.is_err() {
            return;
        }
    }
}

/// Only valid *complete* output packets retire accepted input. More input never
/// refreshes the oldest pending deadline. Empty, fully drained sources stay idle.
#[derive(Default)]
struct StallClock {
    pending: bool,
    accepted: bool,
    since: Option<Instant>,
}
impl StallClock {
    fn pending(&mut self, now: Instant) {
        self.pending = true;
        self.since.get_or_insert(now);
    }
    fn accepted(&mut self) {
        self.accepted = true;
    }
    fn drained(&mut self) {
        self.pending = false;
        if !self.accepted {
            self.since = None;
        }
    }
    fn output(&mut self, now: Instant) {
        self.accepted = false;
        self.since = self.pending.then_some(now);
    }
    fn deadline(&self) -> Option<Instant> {
        self.since.map(|t| t + STALL)
    }
}

#[derive(Default)]
struct Progress {
    clock: Mutex<StallClock>,
    changed: Notify,
    // Publication and teardown share this lock: no feed can follow fencing.
    publishing: Mutex<bool>,
}

// These hooks are compiled only when this source is included by the integration
// harness. The shipped library exposes no executable override or fault options.
#[derive(Clone, Default)]
struct Observer {
    #[cfg(test)]
    counters: Arc<Mutex<TestCounters>>,
    #[cfg(test)]
    fault: TestFault,
    #[cfg(test)]
    harness_prefix: bool,
}
#[cfg(test)]
#[derive(Default, Debug, Clone)]
struct TestCounters {
    children_started: usize,
    children_reaped: usize,
    pumps_started: usize,
    pumps_joined: usize,
    active_tasks: usize,
    max_task_count: usize,
    max_stdout_read: usize,
    max_framing: usize,
    max_stderr_tail: usize,
    stderr_bytes_read: usize,
    stdout_bytes_read: usize,
    harness_prefix_seen: usize,
    max_input_bytes: usize,
    writes_started: usize,
    writes_completed: usize,
    stdin_write_waits: usize,
    input_recv_waits: usize,
    stdout_read_waits: usize,
    stderr_read_waits: usize,
    valid_packets: usize,
    accounting_window_output_events: usize,
    output_packets_received: usize,
    cancel_observed: usize,
    kill_requested: usize,
}
#[cfg(test)]
#[derive(Clone, Copy, Default, PartialEq)]
enum TestFault {
    #[default]
    None,
    StdinError,
    StdoutError,
    StderrError,
    StdinPanic,
    StdoutPanic,
    StderrPanic,
    OutputFeedPanic,
    OutputBetweenWriteAndAccounting,
}
macro_rules! observe {
    ($observer:expr, $c:ident, $body:block) => {{
        #[cfg(test)]
        {
            let mut $c = $observer.counters.lock().unwrap();
            $body
        }
        #[cfg(not(test))]
        {
            let _ = &$observer;
        }
    }};
}

/// On any live termination (even exit 0), fence media before closing pipes and
/// await child reap before releasing admission. Must run under CleanupTracker.
#[allow(clippy::too_many_arguments)]
pub async fn run_child(
    prepared: PreparedNormalizer,
    layout: TransportLayout,
    generation: GenerationId,
    media: Arc<HlsPackager>,
    input: mpsc::Receiver<BudgetedInput>,
    cancel: watch::Receiver<bool>,
    deadline: Instant,
) -> Result<(), HlsFailure> {
    if !valid_source(&layout) {
        media.fail_generation(generation, HlsFailure::Unsupported);
        return Err(HlsFailure::Unsupported);
    }
    #[cfg(test)]
    let (command, observer) = test_support::command_for(&prepared)
        .unwrap_or_else(|| (build_command(&prepared, &layout), Observer::default()));
    #[cfg(not(test))]
    let command = build_command(&prepared, &layout);
    #[cfg(not(test))]
    let observer = Observer::default();
    run_command(
        command, layout, generation, media, input, cancel, deadline, observer,
    )
    .await
}

fn valid_source(layout: &TransportLayout) -> bool {
    let elementary = |pid| (32..8191).contains(&pid);
    layout.program_id != 0
        && elementary(layout.video_pid)
        && elementary(layout.pmt_pid)
        && elementary(layout.pcr_pid)
        && layout.video_pid != layout.pmt_pid
        && matches!(layout.video_type, 0x02 | 0x1b | 0x24)
        && layout.audio.as_ref().is_some_and(|a| {
            elementary(a.pid)
                && a.pid != layout.video_pid
                && a.pid != layout.pmt_pid
                && matches!(a.codec, AudioCodec::MpegAudio | AudioCodec::Ac3)
        })
}

#[allow(clippy::too_many_arguments)]
async fn run_command(
    mut command: Command,
    layout: TransportLayout,
    generation: GenerationId,
    media: Arc<HlsPackager>,
    input: mpsc::Receiver<BudgetedInput>,
    mut cancel: watch::Receiver<bool>,
    deadline: Instant,
    observer: Observer,
) -> Result<(), HlsFailure> {
    if *cancel.borrow() {
        return Ok(());
    }
    let permit = match ChildAdmission::global().try_acquire() {
        Ok(p) => p,
        Err(e) => {
            media.fail_generation(generation, e);
            return Err(e);
        }
    };
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => {
            media.fail_generation(generation, HlsFailure::Pipe);
            return Err(HlsFailure::Pipe);
        }
    };
    observe!(observer, c, {
        c.children_started += 1;
        c.active_tasks += 1;
        c.max_task_count = c.max_task_count.max(c.active_tasks);
    });
    let progress = Arc::new(Progress::default());
    *progress.publishing.lock().unwrap() = true;
    let (pump_cancel, pump_rx) = watch::channel(false);
    let mut pumps = JoinSet::new();
    // Piped handles are guaranteed by our command setup. Still route any missing
    // handle through the same cleanup path, never an early post-spawn return.
    let handles = (child.stdin.take(), child.stdout.take(), child.stderr.take());
    // A pump panic is a JoinError; a supervisor panic must reach the same reap
    // path as well. In particular, publication can be poisoned by a failed feed.
    let outcome = AssertUnwindSafe(async {
    let mut result = Err(HlsFailure::Pipe);
    if let (Some(stdin), Some(stdout), Some(stderr)) = handles {
        pumps.spawn(stdin_pump(
            stdin,
            input,
            pump_rx.clone(),
            progress.clone(),
            observer.clone(),
        ));
        pumps.spawn(stdout_pump(
            stdout,
            layout,
            generation,
            media.clone(),
            pump_rx.clone(),
            progress.clone(),
            observer.clone(),
        ));
        pumps.spawn(stderr_pump(stderr, pump_rx, observer.clone()));
        observe!(observer, c, {
            c.pumps_started += 3;
            c.active_tasks += 3;
            c.max_task_count = c.max_task_count.max(c.active_tasks);
        });
        loop {
            // Register first so activity between computing the deadline and select
            // cannot leave the supervisor asleep on an obsolete timer.
            let changed = progress.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let status = media.status();
            let ready = status == HlsStatus::Ready;
            if let HlsStatus::Failed(e) = status {
                result = Err(e);
                break;
            }
            if status == HlsStatus::Stopped {
                result = Ok(());
                break;
            }
            let next = if ready {
                progress.clock.lock().unwrap().deadline()
            } else {
                Some(deadline)
            };
            if next.is_some_and(|d| Instant::now() >= d) {
                result = Err(if ready {
                    HlsFailure::OutputStall
                } else {
                    HlsFailure::Deadline
                });
                break;
            }
            let timer = async {
                match next {
                    Some(d) => tokio::time::sleep_until(d).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                biased;
                _ = cancelled(&mut cancel) => {
                    observe!(observer, c, { c.cancel_observed += 1; });
                    result = Ok(()); break;
                }
                _ = timer => {},
                _ = &mut changed => {},
                status = child.wait() => { let _ = status; result = Err(HlsFailure::ChildExit); break; }
                pump = pumps.join_next() => {
                    observe!(observer, c, { c.pumps_joined += 1; c.active_tasks -= 1; });
                    result = match pump { Some(Ok(Err(e))) => Err(e), _ => Err(HlsFailure::Pump) };
                    break;
                }
            }
        }
    }
    result
    }).catch_unwind().await;
    let mut result = outcome.unwrap_or(Err(HlsFailure::Pump));
    // Fence even if a pump panicked with this guard held. Poison must not abort
    // teardown or release admission before the child has been reaped.
    *progress
        .publishing
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = false;
    if let Err(e) = result {
        if std::panic::catch_unwind(AssertUnwindSafe(|| media.fail_generation(generation, e)))
            .is_err()
        {
            result = Err(HlsFailure::Pump);
        }
    }
    pump_cancel.send_replace(true);
    // Abort writer immediately to close stdin even if a write is blocked. The
    // JoinSet remains owned; all aborted pumps are joined below after child wait.
    pumps.abort_all();
    let waited = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
    if !matches!(waited, Ok(Ok(_))) {
        observe!(observer, c, {
            c.kill_requested += 1;
        });
        let _ = child.start_kill();
        // Only a successful wait confirms reap. Neither a failed kill request nor
        // a wait error permits releasing admission. OS cleanup has no portable
        // completion bound; retry transient errors without spinning.
        while child.wait().await.is_err() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    observe!(observer, c, {
        c.children_reaped += 1;
    });
    while pumps.join_next().await.is_some() {
        observe!(observer, c, {
            c.pumps_joined += 1;
            c.active_tasks -= 1;
        });
    }
    observe!(observer, c, {
        c.active_tasks -= 1;
    });
    drop(permit);
    result
}

async fn stdin_pump<W: AsyncWrite + Unpin>(
    mut stdin: W,
    mut input: mpsc::Receiver<BudgetedInput>,
    mut cancel: watch::Receiver<bool>,
    progress: Arc<Progress>,
    observer: Observer,
) -> Result<(), HlsFailure> {
    #[cfg(test)]
    match observer.fault {
        TestFault::StdinError => return Err(HlsFailure::Pipe),
        TestFault::StdinPanic => panic!("injected stdin pump panic"),
        _ => {}
    }
    loop {
        observe!(observer, c, {
            c.input_recv_waits += 1;
        });
        let item = tokio::select! {
            biased;
            _ = cancelled(&mut cancel) => return Ok(()),
            item = input.recv() => item.ok_or(HlsFailure::Pipe)?,
        };
        if item.is_empty() {
            continue;
        }
        observe!(observer, c, {
            c.max_input_bytes = c
                .max_input_bytes
                .max(INPUT_BYTES - item.permit.semaphore().available_permits());
            c.writes_started += 1;
        });
        progress.clock.lock().unwrap().pending(Instant::now());
        progress.changed.notify_one();
        let mut offset = 0;
        while offset < item.len() {
            observe!(observer, c, {
                c.stdin_write_waits += 1;
            });
            // Serialize the nonblocking write and its acceptance record against
            // stdout observations. No lock crosses an await: a Pending poll drops
            // the guard before yielding. Output can never retire a successful
            // write and then be overwritten by late acceptance accounting.
            let write = std::future::poll_fn(|cx| {
                let mut clock = progress.clock.lock().unwrap();
                let result =
                    std::pin::Pin::new(&mut stdin).poll_write(cx, &item.as_slice()[offset..]);
                if matches!(result, std::task::Poll::Ready(Ok(n)) if n > 0) {
                    clock.accepted();
                }
                result
            });
            let n = tokio::select! {
                biased;
                _ = cancelled(&mut cancel) => return Ok(()),
                n = write => n.map_err(|_| HlsFailure::Pipe)?,
            };
            if n == 0 {
                return Err(HlsFailure::Pipe);
            }
            offset += n;
            #[cfg(test)]
            if observer.fault == TestFault::OutputBetweenWriteAndAccounting {
                // Inject a logical valid-output event in the physical-write/accounting
                // window. This does not run a codec or claim real stdout activity.
                progress.clock.lock().unwrap().output(Instant::now());
                observe!(observer, c, {
                    c.accounting_window_output_events += 1;
                });
            }
        }
        // Free the allocation and charge together; partial writes never release
        // permits for storage the Vec still owns.
        drop(item);
        progress.clock.lock().unwrap().drained();
        progress.changed.notify_one();
        observe!(observer, c, {
            c.writes_completed += 1;
        });
    }
}

async fn stderr_pump<R: AsyncRead + Unpin>(
    mut stderr: R,
    mut cancel: watch::Receiver<bool>,
    observer: Observer,
) -> Result<(), HlsFailure> {
    #[cfg(test)]
    match observer.fault {
        TestFault::StderrError => return Err(HlsFailure::Pipe),
        TestFault::StderrPanic => panic!("injected stderr pump panic"),
        _ => {}
    }
    let mut read = [0; 4096];
    let mut tail = [0; STDERR_TAIL];
    let mut len = 0;
    loop {
        observe!(observer, c, {
            c.stderr_read_waits += 1;
        });
        let n = tokio::select! {
            biased;
            _ = cancelled(&mut cancel) => return Ok(()),
            n = stderr.read(&mut read) => n.map_err(|_| HlsFailure::Pipe)?,
        };
        if n == 0 {
            return Err(HlsFailure::Pipe);
        }
        let keep = len.min(STDERR_TAIL - n);
        tail.copy_within(len - keep..len, 0);
        tail[keep..keep + n].copy_from_slice(&read[..n]);
        len = keep + n;
        observe!(observer, c, {
            c.max_stderr_tail = c.max_stderr_tail.max(len);
            c.stderr_bytes_read += n;
        });
    }
}

/// Raw output must obey the strict selector and the exact remux contract. No
/// malformed PSI is allowed to hide behind the selector's preserve-last-good rule.
struct OutputValidator {
    source: TransportLayout,
    selector: LiveAudioSelection,
    access: VideoAccessPointState,
    pat: Option<[u8; TS]>,
    pmt: Option<[u8; TS]>,
    opened: bool,
}
impl OutputValidator {
    fn new(source: TransportLayout) -> Self {
        let mut access = VideoAccessPointState::new();
        access.enable_mpeg2();
        Self {
            source,
            selector: LiveAudioSelection::default(),
            access,
            pat: None,
            pmt: None,
            opened: false,
        }
    }
    fn packet(&mut self, packet: &[u8; TS], mut feed: impl FnMut(&[u8])) -> Result<(), HlsFailure> {
        let invalid = HlsFailure::OutputInvalid;
        if packet[0] != 0x47
            || packet[1] & 0x80 != 0
            || packet[3] & 0xc0 != 0
            || packet[3] & 0x30 == 0
        {
            return Err(invalid);
        }
        if packet[3] & 0x20 != 0 && (packet[4] > 183 || (packet[3] & 0x10 != 0 && packet[4] > 182))
        {
            return Err(invalid);
        }
        let pid = ts_pid(packet);
        if packet[3] & 0x20 != 0
            && packet[4] > 0
            && packet[5] & 0x10 != 0
            && (packet[4] < 7 || pid != 256)
        {
            return Err(invalid);
        }
        if !matches!(pid, 0 | 17 | 256 | 257 | 4096 | 8191) {
            return Err(invalid);
        }
        if pid == 0 {
            let s = output_section(packet, 0)?;
            if s.len() != 16
                || u16::from_be_bytes([s[3], s[4]]) != self.source.transport_stream_id
                || u16::from_be_bytes([s[8], s[9]]) != self.source.program_id
                || (((s[10] & 31) as u16) << 8 | s[11] as u16) != 4096
            {
                return Err(invalid);
            }
            self.pat = Some(*packet);
        } else if pid == 4096 {
            if self.pat.is_none() {
                return Err(invalid);
            }
            let s = output_section(packet, 2)?;
            if s.len() < 26
                || u16::from_be_bytes([s[3], s[4]]) != self.source.program_id
                || (((s[8] & 31) as u16) << 8 | s[9] as u16) != 256
            {
                return Err(invalid);
            }
            let info = (((s[10] & 15) as usize) << 8) | s[11] as usize;
            let mut offset = 12 + info;
            let end = s.len() - 4;
            let mut video = false;
            let mut audio = false;
            while offset < end {
                let h = s
                    .get(offset..offset + 5)
                    .filter(|_| offset + 5 <= end)
                    .ok_or(invalid)?;
                let pid = (((h[1] & 31) as u16) << 8) | h[2] as u16;
                let len = (((h[3] & 15) as usize) << 8) | h[4] as usize;
                match (h[0], pid) {
                    (ty, 256) if ty == self.source.video_type && !video => video = true,
                    (0x0f, 257) if len == 0 && !audio => audio = true,
                    _ => return Err(invalid),
                }
                offset += 5 + len;
                if offset > end {
                    return Err(invalid);
                }
            }
            if offset != end || !video || !audio {
                return Err(invalid);
            }
            self.pmt = Some(*packet);
        }
        self.selector.observe(*packet);
        if pid == 4096 {
            match self.selector.layout() {
                LayoutDecision::Supported(l)
                    if l.program_id == self.source.program_id
                        && l.transport_stream_id == self.source.transport_stream_id
                        && l.pmt_pid == 4096
                        && l.video_pid == 256
                        && l.pcr_pid == 256
                        && l.video_type == self.source.video_type
                        && l.audio
                            .as_ref()
                            .is_some_and(|a| a.pid == 257 && a.codec == AudioCodec::AdtsAac) => {}
                _ => return Err(invalid),
            }
        }
        let opening = self.access.observe(packet);
        if !self.opened {
            if !opening || self.pat.is_none() || self.pmt.is_none() {
                return Ok(());
            }
            feed(self.pat.as_ref().unwrap());
            feed(self.pmt.as_ref().unwrap());
            self.opened = true;
        }
        // Suppress mux SDT; the packager synthesizes the session service metadata.
        if pid != 17 {
            feed(packet);
        }
        Ok(())
    }
}

fn output_section(packet: &[u8; TS], id: u8) -> Result<&[u8], HlsFailure> {
    let invalid = HlsFailure::OutputInvalid;
    if packet[1] & 0x40 == 0 || packet[3] & 0x10 == 0 {
        return Err(invalid);
    }
    let payload = if packet[3] & 0x20 != 0 {
        5 + packet[4] as usize
    } else {
        4
    };
    let pointer = *packet.get(payload).ok_or(invalid)? as usize;
    let offset = payload + 1 + pointer;
    let h = packet.get(offset..offset + 8).ok_or(invalid)?;
    if h[0] != id || h[1] & 0xf0 != 0xb0 || h[5] & 1 == 0 || h[6] != 0 || h[7] != 0 {
        return Err(invalid);
    }
    let len = 3 + (((h[1] & 15) as usize) << 8 | h[2] as usize);
    let s = packet
        .get(offset..offset + len)
        .filter(|s| s.len() >= 12)
        .ok_or(invalid)?;
    if mpeg_crc32(s) != 0 {
        return Err(invalid);
    }
    Ok(s)
}

async fn stdout_pump<R: AsyncRead + Unpin>(
    mut stdout: R,
    layout: TransportLayout,
    generation: GenerationId,
    media: Arc<HlsPackager>,
    mut cancel: watch::Receiver<bool>,
    progress: Arc<Progress>,
    observer: Observer,
) -> Result<(), HlsFailure> {
    #[cfg(test)]
    match observer.fault {
        TestFault::StdoutError => return Err(HlsFailure::Pipe),
        TestFault::StdoutPanic => panic!("injected stdout pump panic"),
        _ => {}
    }
    #[cfg(test)]
    if observer.harness_prefix {
        let mut prefix = [0; b"\nrunning 1 test\n".len()];
        tokio::select! {
            biased;
            _ = cancelled(&mut cancel) => return Ok(()),
            r = stdout.read_exact(&mut prefix) => r.map_err(|_| HlsFailure::Pipe)?,
        };
        if prefix != *b"\nrunning 1 test\n" {
            return Err(HlsFailure::OutputInvalid);
        }
        observe!(observer, c, {
            c.harness_prefix_seen += 1;
        });
    }
    let mut read = [0; STDOUT_READ];
    let mut packet = [0; TS];
    let mut staged = 0;
    let mut validator = OutputValidator::new(layout);
    loop {
        observe!(observer, c, {
            c.stdout_read_waits += 1;
        });
        let n = tokio::select! {
            biased;
            _ = cancelled(&mut cancel) => return Ok(()),
            n = stdout.read(&mut read) => n.map_err(|_| HlsFailure::Pipe)?,
        };
        observe!(observer, c, {
            c.max_stdout_read = c.max_stdout_read.max(n);
            c.stdout_bytes_read += n;
        });
        if n == 0 {
            return Err(if staged != 0 {
                HlsFailure::OutputInvalid
            } else {
                HlsFailure::Pipe
            });
        }
        let mut offset = 0;
        while offset < n {
            let take = (TS - staged).min(n - offset);
            packet[staged..staged + take].copy_from_slice(&read[offset..offset + take]);
            offset += take;
            staged += take;
            if staged == TS {
                observe!(observer, c, {
                    c.output_packets_received += 1;
                });
                let publishing = progress.publishing.lock().unwrap();
                if !*publishing {
                    return Ok(());
                }
                #[cfg(test)]
                if observer.fault == TestFault::OutputFeedPanic {
                    panic!("injected output panic while publication lock is held");
                }
                validator.packet(&packet, |bytes| media.feed_generation(generation, bytes))?;
                progress.clock.lock().unwrap().output(Instant::now());
                progress.changed.notify_one();
                observe!(observer, c, {
                    c.valid_packets += 1;
                });
                staged = 0;
            }
            observe!(observer, c, {
                c.max_framing = c.max_framing.max(staged);
            });
        }
    }
}

/// Integration-only adapter. It calls the same supervisor and pumps used by production;
/// only the executable, counters, fault injection, and fixture prefix are supplied by tests.
#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod test_support {
    use super::*;
    use std::collections::HashMap;
    use std::process::{Command as StdCommand, Stdio};
    use std::sync::atomic::{AtomicU64, Ordering};

    type FakeFactory = Mutex<HashMap<PathBuf, (ChildMode, Arc<Mutex<TestCounters>>)>>;
    static FACTORIES: OnceLock<FakeFactory> = OnceLock::new();
    static FACTORY_ID: AtomicU64 = AtomicU64::new(1);

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum ChildMode {
        ExitZero,
        ExitFailure,
        RefuseStdin,
        BlockStdin,
        NoStdout,
        PartialStdout,
        MalformedStdout,
        StderrFlood,
        EchoValidTs,
        ReadyThenStall,
        EchoReadyIdle,
        StdoutFlood,
        CapabilityListing,
    }
    #[derive(Clone, Copy, Debug)]
    pub enum PumpFault {
        StdinError,
        StdoutError,
        StderrError,
        StdinPanic,
        StdoutPanic,
        StderrPanic,
    }
    #[derive(Clone, Copy, Debug)]
    pub enum PendingIo {
        StdinWrite,
        StdoutRead,
        StderrRead,
        InputReceive,
    }

    #[derive(Clone)]
    pub struct Counters(Arc<Mutex<TestCounters>>);
    static PROCESS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    pub async fn process_test_lock() -> tokio::sync::MutexGuard<'static, ()> {
        PROCESS_TEST_LOCK.lock().await
    }
    impl Default for Counters {
        fn default() -> Self {
            Self(Arc::new(Mutex::new(TestCounters::default())))
        }
    }
    #[derive(Debug, Clone, Default)]
    pub struct Snapshot {
        pub children_started: usize,
        pub children_reaped: usize,
        pub active_tasks: usize,
        pub pumps_started: usize,
        pub pumps_joined: usize,
        pub max_task_count: usize,
        pub max_stdout_read: usize,
        pub max_stderr_tail: usize,
        pub max_ts_framing_stage: usize,
        pub stderr_bytes_read: usize,
        pub stdout_bytes_read: usize,
        pub harness_prefix_seen: usize,
        pub cancel_observed: usize,
        pub max_input_bytes: usize,
        pub writes_started: usize,
        pub writes_completed: usize,
        pub valid_packets: usize,
        pub accounting_window_output_events: usize,
        pub output_packets_received: usize,
        pub kill_requested: usize,
        pub stdin_write_waits: usize,
        pub input_recv_waits: usize,
        pub stdout_read_waits: usize,
        pub stderr_read_waits: usize,
    }
    impl Counters {
        pub fn observe_budget(&self, budget: &InputBudget) {
            let mut c = self.0.lock().unwrap();
            c.max_input_bytes = c.max_input_bytes.max(budget.charged());
        }
        pub fn snapshot(&self) -> Snapshot {
            let c = self.0.lock().unwrap();
            Snapshot {
                children_started: c.children_started,
                children_reaped: c.children_reaped,
                active_tasks: c.active_tasks,
                pumps_started: c.pumps_started,
                pumps_joined: c.pumps_joined,
                max_task_count: c.max_task_count,
                max_stdout_read: c.max_stdout_read,
                max_stderr_tail: c.max_stderr_tail,
                max_ts_framing_stage: c.max_framing,
                stderr_bytes_read: c.stderr_bytes_read,
                stdout_bytes_read: c.stdout_bytes_read,
                harness_prefix_seen: c.harness_prefix_seen,
                cancel_observed: c.cancel_observed,
                max_input_bytes: c.max_input_bytes,
                writes_started: c.writes_started,
                writes_completed: c.writes_completed,
                valid_packets: c.valid_packets,
                accounting_window_output_events: c.accounting_window_output_events,
                output_packets_received: c.output_packets_received,
                kill_requested: c.kill_requested,
                stdin_write_waits: c.stdin_write_waits,
                input_recv_waits: c.input_recv_waits,
                stdout_read_waits: c.stdout_read_waits,
                stderr_read_waits: c.stderr_read_waits,
            }
        }
    }

    /// Install a per-test executable sentinel. Only cfg(test) run_child consults it;
    /// production PreparedNormalizer values and command construction are untouched.
    pub fn fake_prepared_child(mode: ChildMode) -> (PreparedNormalizer, Counters) {
        let id = FACTORY_ID.fetch_add(1, Ordering::Relaxed);
        let executable = PathBuf::from(format!("__outpace_test_child_{id}"));
        let counters = Counters::default();
        FACTORIES
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .insert(executable.clone(), (mode, counters.0.clone()));
        (PreparedNormalizer { executable }, counters)
    }

    pub(super) fn command_for(prepared: &PreparedNormalizer) -> Option<(Command, Observer)> {
        let entry = FACTORIES
            .get()?
            .lock()
            .unwrap()
            .get(&prepared.executable)
            .cloned()?;
        let (mode, counters) = entry;
        Some((
            Command::from(fake_child_unit(mode, &[])),
            Observer {
                counters,
                harness_prefix: true,
                ..Observer::default()
            },
        ))
    }

    fn fake_child_unit(mode: ChildMode, extra_args: &[&str]) -> StdCommand {
        let mode_name = match mode {
            ChildMode::ExitZero => "exit-zero",
            ChildMode::ExitFailure => "exit-failure",
            ChildMode::RefuseStdin => "refuse-stdin",
            ChildMode::BlockStdin => "block-stdin",
            ChildMode::NoStdout => "no-stdout",
            ChildMode::PartialStdout => "partial-stdout",
            ChildMode::MalformedStdout => "malformed-stdout",
            ChildMode::StderrFlood => "stderr-flood",
            ChildMode::EchoValidTs => "echo-valid-ts",
            ChildMode::ReadyThenStall => "ready-then-stall",
            ChildMode::EchoReadyIdle => "echo-ready-idle",
            ChildMode::StdoutFlood => "stdout-flood",
            ChildMode::CapabilityListing => "capability-listing",
        };
        let executable = std::env::current_exe().expect("test executable");
        // This source is also compiled into the fault integration target, whose
        // portable fake-child entry is at crate root rather than this unit path.
        let child_test = if executable
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("hls_audio_faults-"))
        {
            "fake_child_entry"
        } else {
            "hls_audio::process::test_support::fake_child_entry"
        };
        let mut command = StdCommand::new(executable);
        command
            .args(["--format", "terse", "--exact", child_test, "--nocapture"])
            .arg("--")
            .args(extra_args)
            .env("OUTPACE_TEST_HLS_AUDIO_CHILD", mode_name)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    pub(super) fn probe_command_for(
        prepared: &PreparedNormalizer,
        args: &[&str],
    ) -> Option<Command> {
        let (mode, _) = FACTORIES
            .get()?
            .lock()
            .unwrap()
            .get(&prepared.executable)
            .cloned()?;
        Some(Command::from(fake_child_unit(mode, args)))
    }

    #[test]
    fn fake_child_entry() {
        let Some(mode) = std::env::var_os("OUTPACE_TEST_HLS_AUDIO_CHILD") else {
            return;
        };
        if let Some(mode) = mode
            .to_str()
            .and_then(test_fixtures::child::ChildMode::parse)
        {
            test_fixtures::child::run(mode);
        }
    }

    /// Runs the production stdin pump with a successful writer and injects a
    /// logical valid-output event in the physical-write/accounting window.
    pub async fn output_between_write_and_accounting() -> (Option<Instant>, usize, usize, usize) {
        let budget = InputBudget::new();
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (input_tx, input_rx) = mpsc::channel(1);
        let mut acquire_cancel = cancel_tx.subscribe();
        let item = budget
            .acquire_copy(&[0x47; 188], &mut acquire_cancel)
            .await
            .unwrap();
        input_tx.send(item).await.unwrap();
        let counters = Arc::new(Mutex::new(TestCounters::default()));
        let progress = Arc::new(Progress::default());
        let pump = tokio::spawn(stdin_pump(
            tokio::io::sink(),
            input_rx,
            cancel_rx,
            progress.clone(),
            Observer {
                counters: counters.clone(),
                fault: TestFault::OutputBetweenWriteAndAccounting,
                ..Observer::default()
            },
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while counters.lock().unwrap().writes_completed == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("stdin pump completes the successful write");
        let deadline = progress.clock.lock().unwrap().deadline();
        let (writes, events) = {
            let c = counters.lock().unwrap();
            (c.writes_completed, c.accounting_window_output_events)
        };
        cancel_tx.send_replace(true);
        pump.await
            .expect("stdin pump joins")
            .expect("pump succeeds");
        drop(input_tx);
        (deadline, writes, events, budget.charged())
    }

    fn fixture() -> (TransportLayout, Arc<HlsPackager>) {
        fixture_with_config(crate::config::HlsConfig {
            segment_packets: 6,
            startup_segments: 1,
            segment_duration_ms: 1000,
            startup_timeout_ms: 5000,
            ..Default::default()
        })
    }

    fn fixture_with_config(
        config: crate::config::HlsConfig,
    ) -> (TransportLayout, Arc<HlsPackager>) {
        use crate::session::StreamSession;
        struct Empty;
        #[async_trait::async_trait]
        impl crate::provider::TsSource for Empty {
            async fn next(&mut self) -> Option<bytes::Bytes> {
                None
            }
            fn stats(&self) -> crate::provider::SourceStats {
                Default::default()
            }
        }
        let session = StreamSession::start(Box::new(Empty), 1);
        let media = HlsPackager::create(&session, config, true);
        let layout = TransportLayout {
            transport_stream_id: 7,
            program_id: 9,
            pmt_pid: 100,
            pcr_pid: 513,
            video_pid: 512,
            video_type: 0x1b,
            audio: Some(ace_media::audio::SelectedAudio {
                pid: 513,
                codec: AudioCodec::Ac3,
            }),
            semantic_identity: vec![],
        };
        (layout, media)
    }

    async fn run_controlled(
        command: std::process::Command,
        observer: Observer,
        input: Vec<u8>,
        timeout: Duration,
        cancel_after: Option<Duration>,
        keep_sender: bool,
    ) -> Result<(), HlsFailure> {
        let (layout, media) = fixture();
        let generation = GenerationId(1);
        media.begin_generation(generation, Instant::now() + timeout, false);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (tx, rx) = mpsc::channel(1);
        let budget = InputBudget::new();
        let mut budget_cancel = cancel_rx.clone();
        if !input.is_empty() {
            let item = budget.acquire_owned(input, &mut budget_cancel).await?;
            tx.send(item).await.map_err(|_| HlsFailure::Pipe)?;
        }
        if !keep_sender {
            drop(tx);
        }
        let cancel_task = cancel_after.map(|after| {
            let signal = cancel_tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(after).await;
                signal.send_replace(true);
            })
        });
        let status_media = media.clone();
        let result = run_command(
            Command::from(command),
            layout,
            generation,
            media,
            rx,
            cancel_rx,
            Instant::now() + timeout,
            observer,
        )
        .await;
        if result.is_err() {
            assert!(
                matches!(status_media.status(), HlsStatus::Failed(_)),
                "failure must become terminal and wake status waiters"
            );
        }
        if let Some(task) = cancel_task {
            let _ = task.await;
        }
        drop(cancel_tx);
        result
    }

    pub async fn supervisor_ready_stall_evidence() -> Snapshot {
        let _serial = process_test_lock().await;
        let (layout, media) = fixture();
        let generation = GenerationId(1);
        media.begin_generation(generation, Instant::now() + Duration::from_secs(5), false);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (tx, rx) = mpsc::channel(1);
        let budget = InputBudget::new();
        let mut budget_cancel = cancel_rx.clone();
        tx.send(
            budget
                .acquire_owned(vec![0x47; INPUT_BYTES / 2], &mut budget_cancel)
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        let counters = Arc::new(Counters::default());
        let observer = Observer {
            counters: counters.0.clone(),
            harness_prefix: true,
            ..Observer::default()
        };
        let task = tokio::spawn(run_command(
            Command::from(fake_child(ChildMode::ReadyThenStall)),
            layout,
            generation,
            media.clone(),
            rx,
            cancel_rx,
            Instant::now() + Duration::from_secs(5),
            observer,
        ));
        let required_stdout = test_fixtures::ready_output_fixture(12).len() + 187;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let s = counters.snapshot();
                if media.status() == HlsStatus::Ready
                    && s.stdout_bytes_read >= required_stdout
                    && s.writes_started > 0
                    && s.writes_completed == 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("child handshake, partial output, and blocked write observed before pausing");
        let before = counters.snapshot();
        assert_eq!(before.max_ts_framing_stage, 187);
        let packets_before = before.output_packets_received;
        counters.observe_budget(&budget);
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(44)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            !task.is_finished(),
            "Ready supervisor is pending at 44 seconds"
        );
        assert_eq!(media.status(), HlsStatus::Ready);
        assert_eq!(
            counters.snapshot().output_packets_received,
            packets_before,
            "partial stdout did not count as output"
        );
        let second = budget
            .acquire_owned(vec![0x47; INPUT_BYTES / 2], &mut budget_cancel)
            .await
            .unwrap();
        tx.send(second).await.unwrap();
        counters.observe_budget(&budget);
        assert_eq!(budget.charged(), INPUT_BYTES);
        assert_eq!(
            counters.snapshot().max_input_bytes,
            INPUT_BYTES,
            "queue plus active write is charged in aggregate"
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            media.status(),
            HlsStatus::Failed(HlsFailure::OutputStall)
        ));
        // Advance after the supervisor enters its one-second reap grace; kill/wait then uses the OS child handle.
        for _ in 0..4 {
            tokio::time::advance(Duration::from_secs(1)).await;
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            if counters.snapshot().children_reaped == 1 {
                break;
            }
        }
        let result = task.await.expect("supervisor task joined");
        assert_eq!(result, Err(HlsFailure::OutputStall));
        drop(tx);
        drop(cancel_tx);
        assert_eq!(budget.charged(), 0);
        counters.snapshot()
    }

    pub async fn supervisor_drained_idle_evidence() -> Snapshot {
        let _serial = process_test_lock().await;
        let (layout, media) = fixture();
        let generation = GenerationId(1);
        media.begin_generation(generation, Instant::now() + Duration::from_secs(5), false);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (tx, rx) = mpsc::channel(1);
        let budget = InputBudget::new();
        let payload = test_fixtures::ready_output_fixture(12);
        let mut budget_cancel = cancel_rx.clone();
        tx.send(
            budget
                .acquire_owned(payload, &mut budget_cancel)
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        let counters = Arc::new(Counters::default());
        let observer = Observer {
            counters: counters.0.clone(),
            harness_prefix: true,
            ..Observer::default()
        };
        let task = tokio::spawn(run_command(
            Command::from(fake_child(ChildMode::EchoReadyIdle)),
            layout,
            generation,
            media.clone(),
            rx,
            cancel_rx,
            Instant::now() + Duration::from_secs(5),
            observer,
        ));
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let s = counters.snapshot();
                if media.status() == HlsStatus::Ready
                    && s.writes_completed == 1
                    && s.stdout_bytes_read >= test_fixtures::ready_output_fixture(12).len()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("idle child emitted valid output and drained accepted input");
        assert_eq!(budget.charged(), 0);
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(90)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            !task.is_finished(),
            "fully drained idle supervisor remains healthy"
        );
        assert_eq!(media.status(), HlsStatus::Ready);
        cancel_tx.send_replace(true);
        tokio::time::resume();
        let result = tokio::time::timeout(Duration::from_secs(4), task)
            .await
            .expect("cancelled idle child reaped")
            .expect("supervisor task joined");
        assert_eq!(result, Ok(()));
        drop(tx);
        counters.snapshot()
    }

    pub async fn many_chunk_echo_evidence() -> (Snapshot, usize) {
        const CHUNKS: usize = 1_024;
        let _serial = process_test_lock().await;
        let (layout, media) = fixture();
        let generation = GenerationId(1);
        let deadline = Instant::now() + Duration::from_secs(5);
        media.begin_generation(generation, deadline, false);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (input_tx, input_rx) = mpsc::channel(8);
        let budget = InputBudget::new();
        let initial = test_fixtures::ready_output_fixture(12);
        let fixture_packets = initial.len() / TS;
        let mut budget_cancel = cancel_rx.clone();
        input_tx
            .send(
                budget
                    .acquire_copy(&initial, &mut budget_cancel)
                    .await
                    .expect("ready fixture fits input budget"),
            )
            .await
            .expect("supervisor receiver is open");

        let counters = Arc::new(Counters::default());
        let observer = Observer {
            counters: counters.0.clone(),
            harness_prefix: true,
            ..Observer::default()
        };
        let supervisor = tokio::spawn(run_command(
            Command::from(fake_child(ChildMode::EchoValidTs)),
            layout,
            generation,
            media,
            input_rx,
            cancel_rx.clone(),
            deadline,
            observer,
        ));

        let producer_budget = budget.clone();
        let mut producer_cancel = cancel_rx.clone();
        let producer = tokio::spawn(async move {
            for _ in 0..CHUNKS {
                let mut packet = [0xff; TS];
                packet[..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0x10]);
                let item = match producer_budget
                    .acquire_copy(&packet, &mut producer_cancel)
                    .await
                {
                    Ok(item) => item,
                    Err(_) => return,
                };
                tokio::select! {
                    biased;
                    _ = cancelled(&mut producer_cancel) => return,
                    sent = input_tx.send(item) => if sent.is_err() { return; },
                }
            }
            // Keep the channel alive through the observation window so EOF cannot
            // become the reason the child supervisor exits.
            let _ = cancelled(&mut producer_cancel).await;
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = counters.snapshot();
                if snapshot.writes_completed > CHUNKS
                    && snapshot.valid_packets >= fixture_packets + CHUNKS
                {
                    break;
                }
                assert!(
                    !supervisor.is_finished(),
                    "echo supervisor exited before all chunks were observed: {snapshot:?}"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("all individually queued chunks were written and validated");

        cancel_tx.send_replace(true);
        tokio::time::timeout(Duration::from_secs(3), async {
            assert_eq!(supervisor.await.expect("supervisor task joined"), Ok(()));
            producer.await.expect("producer task joined");
        })
        .await
        .expect("supervisor cleanup and producer join are bounded");
        let charged = budget.charged();
        let snapshot = counters.snapshot();
        (snapshot, charged)
    }

    pub async fn delayed_startup_deadline_evidence(
    ) -> (HlsStatus, bool, Result<(), HlsFailure>, Snapshot) {
        let _serial = process_test_lock().await;
        let (layout, media) = fixture();
        let generation = GenerationId(1);
        let deadline = Instant::now() + Duration::from_millis(500);
        media.begin_generation(generation, deadline, false);
        tokio::time::sleep(Duration::from_millis(400)).await;

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (input_tx, input_rx) = mpsc::channel(1);
        let counters = Arc::new(Counters::default());
        let observer = Observer {
            counters: counters.0.clone(),
            harness_prefix: true,
            ..Observer::default()
        };
        let task = tokio::spawn(run_command(
            Command::from(fake_child(ChildMode::NoStdout)),
            layout,
            generation,
            media.clone(),
            input_rx,
            cancel_rx,
            deadline,
            observer,
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            while counters.snapshot().children_started != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("no-stdout child starts before the remaining startup window");

        let remaining = deadline.saturating_duration_since(Instant::now());
        let status = media
            .wait_available(remaining + Duration::from_millis(150))
            .await;
        let unreaped_at_failure = counters.snapshot().children_reaped == 0;
        let result = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("deadline child cleanup and reap are bounded")
            .expect("supervisor task joined");
        drop(input_tx);
        drop(cancel_tx);
        (status, unreaped_at_failure, result, counters.snapshot())
    }

    async fn run(
        command: std::process::Command,
        observer: Observer,
        input: Vec<u8>,
        timeout: Duration,
    ) -> Result<(), HlsFailure> {
        run_controlled(command, observer, input, timeout, None, false).await
    }

    pub async fn run_fault_case(
        mode: ChildMode,
        counters: Arc<Counters>,
    ) -> Result<(), HlsFailure> {
        let _serial = process_test_lock().await;
        let observer = Observer {
            counters: counters.0.clone(),
            fault: TestFault::None,
            harness_prefix: true,
        };
        let input = if mode == ChildMode::BlockStdin {
            vec![0x47; INPUT_BYTES]
        } else {
            vec![0x47; 188]
        };
        run_controlled(
            fake_child(mode),
            observer,
            input,
            Duration::from_millis(800),
            None,
            true,
        )
        .await
    }

    pub async fn supervisors_hold_four_global_slots_until_reap() -> (usize, usize, usize, usize) {
        let _serial = process_test_lock().await;
        let counters = Arc::new(Counters::default());
        let mut tasks = Vec::new();
        let mut signals = Vec::new();
        for _ in 0..4 {
            let (tx, rx) = watch::channel(false);
            signals.push(tx);
            let counters = counters.clone();
            tasks.push(tokio::spawn(async move {
                let (layout, media) = fixture();
                let generation = GenerationId(1);
                media.begin_generation(generation, Instant::now() + Duration::from_secs(5), false);
                let (input_tx, input_rx) = mpsc::channel(1);
                let observer = Observer {
                    counters: counters.0.clone(),
                    harness_prefix: true,
                    ..Observer::default()
                };
                let result = run_command(
                    Command::from(fake_child(ChildMode::BlockStdin)),
                    layout,
                    generation,
                    media,
                    input_rx,
                    rx,
                    Instant::now() + Duration::from_secs(5),
                    observer,
                )
                .await;
                drop(input_tx);
                result
            }));
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while counters.snapshot().children_started < 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("four supervisors started");
        let (layout, media) = fixture();
        let generation = GenerationId(1);
        media.begin_generation(generation, Instant::now() + Duration::from_secs(2), false);
        let (_, rx) = watch::channel(false);
        let (input_tx, input_rx) = mpsc::channel(1);
        let fifth = run_command(
            Command::from(fake_child(ChildMode::BlockStdin)),
            layout,
            generation,
            media,
            input_rx,
            rx,
            Instant::now() + Duration::from_secs(2),
            Observer::default(),
        )
        .await;
        drop(input_tx);
        let immediate_rejected = usize::from(fifth == Err(HlsFailure::Admission));
        tokio::time::sleep(Duration::from_millis(100)).await;
        for signal in &signals {
            signal.send_replace(true);
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
        let during_grace_rejected = usize::from(ChildAdmission::global().try_acquire().is_err());
        tokio::time::timeout(Duration::from_secs(3), async {
            for task in tasks {
                assert_eq!(task.await.unwrap(), Ok(()));
            }
        })
        .await
        .expect("all four children killed and reaped");
        let after_reap_available = usize::from(ChildAdmission::global().try_acquire().is_ok());
        let snapshot = counters.snapshot();
        (
            immediate_rejected,
            during_grace_rejected,
            after_reap_available,
            snapshot.children_reaped,
        )
    }

    pub async fn run_with_pump_fault(
        fault: PumpFault,
        counters: Arc<Counters>,
    ) -> Result<(), HlsFailure> {
        let _serial = process_test_lock().await;
        let mode = ChildMode::BlockStdin;
        let command = fake_child(mode);
        let fault = match fault {
            PumpFault::StdinError => TestFault::StdinError,
            PumpFault::StdoutError => TestFault::StdoutError,
            PumpFault::StderrError => TestFault::StderrError,
            PumpFault::StdinPanic => TestFault::StdinPanic,
            PumpFault::StdoutPanic => TestFault::StdoutPanic,
            PumpFault::StderrPanic => TestFault::StderrPanic,
        };
        run_controlled(
            command,
            Observer {
                counters: counters.0.clone(),
                fault,
                harness_prefix: true,
            },
            vec![0x47; 188],
            Duration::from_millis(800),
            None,
            true,
        )
        .await
    }

    pub async fn output_guard_panic_cleanup(counters: Arc<Counters>) -> Result<(), HlsFailure> {
        let _serial = process_test_lock().await;
        let mut packet = vec![0xff; TS];
        packet[0] = 0x47;
        packet[1] = 0x40;
        packet[2] = 0;
        packet[3] = 0x10;
        let mut input = packet.clone();
        input.extend_from_slice(&packet);
        input.extend_from_slice(&packet);
        let command = fake_child(ChildMode::EchoValidTs);
        let observer = Observer {
            counters: counters.0.clone(),
            fault: TestFault::OutputFeedPanic,
            harness_prefix: true,
        };
        let task = tokio::spawn(run_controlled(
            command,
            observer,
            input,
            Duration::from_millis(800),
            None,
            true,
        ));
        match tokio::time::timeout(Duration::from_secs(2), task).await {
            Ok(Ok(result)) => result,
            Ok(Err(_join)) => Err(HlsFailure::Pump),
            Err(_) => panic!("guard panic supervisor exceeded bounded observation window"),
        }
    }

    pub async fn cancel_while_pending(_state: PendingIo, counters: Arc<Counters>) {
        let _serial = process_test_lock().await;
        // Cancellation coverage is exercised by the same bounded child cleanup path.
        let mode = ChildMode::BlockStdin;
        let command = fake_child(mode);
        let (input, cancel_after, keep_sender) = match _state {
            PendingIo::StdinWrite => (vec![0x47; INPUT_BYTES], Duration::from_millis(100), true),
            PendingIo::StdoutRead | PendingIo::StderrRead => {
                (vec![], Duration::from_millis(100), true)
            }
            PendingIo::InputReceive => (vec![], Duration::from_millis(100), true),
        };
        let result = run_controlled(
            command,
            Observer {
                counters: counters.0.clone(),
                ..Observer::default()
            },
            input,
            Duration::from_secs(5),
            Some(cancel_after),
            keep_sender,
        )
        .await;
        assert_eq!(
            result,
            Ok(()),
            "cancellation must be the observed terminal event"
        );
        assert_eq!(counters.snapshot().children_started, 1);
        assert_eq!(counters.snapshot().children_reaped, 1);
        assert_eq!(counters.snapshot().pumps_joined, 3);
        let snapshot = counters.snapshot();
        match _state {
            PendingIo::StdinWrite => {
                assert_eq!(snapshot.writes_started, 1);
                assert_eq!(snapshot.writes_completed, 0);
                assert!(snapshot.stdin_write_waits > 0);
            }
            PendingIo::StdoutRead => assert!(snapshot.stdout_read_waits > 0),
            PendingIo::StderrRead => assert!(snapshot.stderr_read_waits > 0),
            PendingIo::InputReceive => assert!(snapshot.input_recv_waits > 0),
        }
    }

    pub fn validate_output(
        bytes: &[u8],
        chunk_sizes: &[usize],
    ) -> Result<(usize, usize), HlsFailure> {
        let mut validator = OutputValidator::new(TransportLayout {
            transport_stream_id: 7,
            program_id: 9,
            pmt_pid: 100,
            pcr_pid: 513,
            video_pid: 512,
            video_type: 0x1b,
            audio: Some(ace_media::audio::SelectedAudio {
                pid: 513,
                codec: AudioCodec::Ac3,
            }),
            semantic_identity: vec![],
        });
        let mut staged = Vec::new();
        let mut fed = 0;
        let mut packets = 0;
        let mut offset = 0;
        #[cfg(test)]
        if bytes.len() >= TS {
            let first: &[u8; TS] = bytes[..TS].try_into().unwrap();
            if ts_pid(first) == 0 {
                let _ = output_section(first, 0)?;
            }
        }
        for size in chunk_sizes
            .iter()
            .copied()
            .chain(std::iter::once(bytes.len()))
        {
            if offset == bytes.len() {
                break;
            }
            let end = (offset + size).min(bytes.len());
            staged.extend_from_slice(&bytes[offset..end]);
            offset = end;
            while staged.len() >= TS {
                let packet: [u8; TS] = staged.drain(..TS).collect::<Vec<_>>().try_into().unwrap();
                validator.packet(&packet, |_| fed += 1)?;
                packets += 1;
            }
        }
        Ok((packets, fed))
    }

    pub fn command_args() -> Vec<String> {
        let command = build_command(
            &PreparedNormalizer {
                executable: PathBuf::from("ffmpeg-test"),
            },
            &TransportLayout {
                transport_stream_id: 7,
                program_id: 9,
                pmt_pid: 100,
                pcr_pid: 513,
                video_pid: 512,
                video_type: 0x1b,
                audio: Some(ace_media::audio::SelectedAudio {
                    pid: 513,
                    codec: AudioCodec::Ac3,
                }),
                semantic_identity: vec![],
            },
        );
        command
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect()
    }

    pub async fn invalid_source_preflight() -> (Result<(), HlsFailure>, HlsStatus) {
        let _serial = process_test_lock().await;
        let (mut layout, media) = fixture();
        layout.video_pid = layout.pmt_pid;
        let generation = GenerationId(1);
        media.begin_generation(generation, Instant::now() + Duration::from_secs(5), false);
        let (_cancel_tx, cancel) = watch::channel(false);
        let (_tx, rx) = mpsc::channel(1);
        let result = run_child(
            PreparedNormalizer {
                executable: PathBuf::from("path-that-must-not-be-looked-up"),
            },
            layout,
            generation,
            media.clone(),
            rx,
            cancel,
            Instant::now() + Duration::from_secs(5),
        )
        .await;
        (result, media.status())
    }

    pub async fn bounded_channel_send_cancels_and_joins() {
        let (tx, _rx) = mpsc::channel::<usize>(1);
        tx.send(1).await.unwrap();
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let sender = tokio::spawn(async move {
            let _ = entered_tx.send(());
            tokio::select! {
                biased;
                _ = cancelled(&mut cancel_rx) => Ok::<(), ()>(()),
                result = tx.send(2) => result.map_err(|_| ()),
            }
        });
        entered_rx.await.expect("sender entered bounded send");
        tokio::task::yield_now().await;
        cancel_tx.send_replace(true);
        tokio::time::timeout(Duration::from_secs(1), sender)
            .await
            .expect("blocked sender must be joined")
            .expect("sender task must not panic")
            .expect("cancellation is a clean send termination");
    }

    pub fn fake_child(mode: ChildMode) -> StdCommand {
        let mode_name = match mode {
            ChildMode::ExitZero => "exit-zero",
            ChildMode::ExitFailure => "exit-failure",
            ChildMode::RefuseStdin => "refuse-stdin",
            ChildMode::BlockStdin => "block-stdin",
            ChildMode::NoStdout => "no-stdout",
            ChildMode::PartialStdout => "partial-stdout",
            ChildMode::MalformedStdout => "malformed-stdout",
            ChildMode::StderrFlood => "stderr-flood",
            ChildMode::EchoValidTs => "echo-valid-ts",
            ChildMode::ReadyThenStall => "ready-then-stall",
            ChildMode::EchoReadyIdle => "echo-ready-idle",
            ChildMode::StdoutFlood => "stdout-flood",
            ChildMode::CapabilityListing => "capability-listing",
        };
        let mut command = StdCommand::new(std::env::current_exe().expect("test executable"));
        command
            .args([
                "--format",
                "terse",
                "--exact",
                "fake_child_entry",
                "--nocapture",
            ])
            .env("OUTPACE_TEST_HLS_AUDIO_CHILD", mode_name)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    pub async fn stall_after_first_pending_input(duration: Duration) -> Result<(), HlsFailure> {
        let mut clock = StallClock::default();
        clock.pending(Instant::now());
        tokio::time::advance(Duration::from_secs(20)).await;
        clock.accepted();
        clock.pending(Instant::now());
        tokio::time::advance(duration).await;
        if clock.deadline().is_some_and(|d| Instant::now() >= d) {
            Err(HlsFailure::OutputStall)
        } else {
            Ok(())
        }
    }
    pub async fn stall_boundary_is_healthy_at(duration: Duration) {
        let mut clock = StallClock::default();
        clock.pending(Instant::now());
        tokio::time::advance(duration).await;
        assert!(clock.deadline().is_some_and(|d| Instant::now() < d));
    }
    pub async fn drained_idle_source_remains_healthy(duration: Duration) {
        let mut clock = StallClock::default();
        clock.pending(Instant::now());
        clock.drained();
        tokio::time::advance(duration).await;
        assert!(clock.deadline().is_none());
    }

    pub async fn stall_clock_transitions() {
        let start = Instant::now();
        let mut clock = StallClock::default();
        clock.pending(start);
        clock.accepted();
        clock.drained();
        assert_eq!(clock.deadline(), Some(start + STALL));
        clock.output(start + Duration::from_secs(10));
        assert!(clock.deadline().is_none(), "output retires drained input");

        clock.pending(start + Duration::from_secs(20));
        clock.accepted();
        clock.output(start + Duration::from_secs(30));
        let refreshed = start + Duration::from_secs(30);
        assert_eq!(clock.deadline(), Some(refreshed + STALL));
        clock.accepted();
        clock.pending(start + Duration::from_secs(35));
        assert_eq!(clock.deadline(), Some(refreshed + STALL));

        let mut drained_first = StallClock::default();
        drained_first.pending(start);
        drained_first.accepted();
        drained_first.drained();
        assert_eq!(drained_first.deadline(), Some(start + STALL));
    }

    pub async fn partial_stdout_does_not_count_as_output() {
        let (mut writer, reader) = tokio::io::duplex(256);
        let (layout, media) = fixture();
        let generation = GenerationId(1);
        let progress = Arc::new(Progress::default());
        let observer = Observer::default();
        let pump = tokio::spawn(stdout_pump(
            reader,
            layout,
            generation,
            media,
            watch::channel(false).1,
            progress,
            observer.clone(),
        ));
        writer.write_all(&[0x47; 187]).await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(observer.counters.lock().unwrap().output_packets_received, 0);
        assert_eq!(observer.counters.lock().unwrap().valid_packets, 0);
        pump.abort();
        let _ = pump.await;
    }
}

/// Public failures contain only fixed categories, never executable paths or output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HlsPreflightError {
    Configuration,
    Executable,
    Capability(&'static str),
    Timeout,
    Capture,
    Process,
    Smoke,
    Cancelled,
}
impl std::fmt::Display for HlsPreflightError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HLS normalization preflight: {self:?}")
    }
}
impl std::error::Error for HlsPreflightError {}

/// A dropped startup caller signals the runtime-owned preflight supervisor; it
/// never drops a live Child future. Normal success/error awaits its full cleanup.
pub async fn preflight(
    config: &crate::config::HlsAudioConfig,
) -> Result<Option<PreparedNormalizer>, HlsPreflightError> {
    if !config.normalize {
        return Ok(None);
    }
    config
        .validate()
        .map_err(|_| HlsPreflightError::Configuration)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let path = config.ffmpeg_path.clone();
    let (cancel, rx) = watch::channel(false);
    struct CancelOnDrop(watch::Sender<bool>);
    impl Drop for CancelOnDrop {
        fn drop(&mut self) {
            self.0.send_replace(true);
        }
    }
    let guard = CancelOnDrop(cancel);
    let task = tokio::spawn(async move {
        let prepared = PreparedNormalizer {
            executable: resolve_executable(&path)?,
        };
        preflight_prepared(&prepared, rx, deadline).await?;
        Ok(Some(prepared))
    });
    let result = task.await.map_err(|_| HlsPreflightError::Process)?;
    drop(guard);
    result
}
fn resolve_executable(path: &std::path::Path) -> Result<PathBuf, HlsPreflightError> {
    resolve_executable_with(path, std::env::var_os("PATH").as_deref(), cfg!(windows))
        .and_then(|p| p.canonicalize().ok())
        .ok_or(HlsPreflightError::Executable)
}

// Keep filename resolution separate from PATH acquisition so platform naming
// rules do not introduce implicit application, system, or current-dir searches.
fn resolve_executable_with(
    path: &std::path::Path,
    search_path: Option<&std::ffi::OsStr>,
    windows: bool,
) -> Option<PathBuf> {
    fn executable(path: &std::path::Path) -> bool {
        let Ok(metadata) = path.metadata() else {
            return false;
        };
        if !metadata.is_file() {
            return false;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            metadata.permissions().mode() & 0o111 != 0
        }
        #[cfg(not(unix))]
        {
            true
        }
    }
    let bare = path.components().count() == 1 && !path.is_absolute();
    if bare {
        let mut name = path.as_os_str().to_os_string();
        // Like Command on Windows, bare names containing a dot are exact;
        // otherwise only the native .exe suffix is searched (never PATHEXT).
        if windows && !name.as_encoded_bytes().contains(&b'.') {
            name.push(".exe");
        }
        search_path.and_then(|paths| {
            std::env::split_paths(paths)
                .filter(|p| !windows || !p.as_os_str().is_empty())
                .map(|p| p.join(&name))
                .find(|p| executable(p))
        })
    } else {
        let has_exe_suffix = path
            .as_os_str()
            .as_encoded_bytes()
            .get(path.as_os_str().len().saturating_sub(4)..)
            .is_some_and(|suffix| suffix.eq_ignore_ascii_case(b".exe"));
        if windows && !has_exe_suffix {
            // Explicit paths try an appended .exe first, even when another
            // extension is present, then fall back to the supplied path.
            let mut suffixed = path.as_os_str().to_os_string();
            suffixed.push(".exe");
            let candidate = PathBuf::from(suffixed);
            if executable(&candidate) {
                return Some(candidate);
            }
        }
        executable(path).then(|| path.to_path_buf())
    }
}

async fn preflight_prepared(
    prepared: &PreparedNormalizer,
    mut cancel: watch::Receiver<bool>,
    deadline: Instant,
) -> Result<(), HlsPreflightError> {
    let probes: &[(&str, &[&str])] = &[
        ("-demuxers", &["mpegts"]),
        ("-muxers", &["mpegts"]),
        ("-encoders", &["aac"]),
        ("-decoders", &["mp2", "mp3", "ac3"]),
        ("-protocols", &["pipe"]),
    ];
    for (argument, required) in probes {
        let mut command = Command::new(&prepared.executable);
        command.args(["-hide_banner", argument]);
        #[cfg(test)]
        if let Some(fake) = test_support::probe_command_for(prepared, &["-hide_banner", argument]) {
            command = fake;
        }
        let capture = probe(command, Vec::new(), &mut cancel, deadline).await?;
        let output =
            std::str::from_utf8(&capture).map_err(|_| HlsPreflightError::Capability("listing"))?;
        for capability in *required {
            let present = if *argument == "-protocols" {
                output.lines().any(|l| l.trim() == *capability)
            } else {
                output.lines().any(|l| {
                    l.split_whitespace()
                        .nth(1)
                        .is_some_and(|names| names.split(',').any(|n| n == *capability))
                })
            };
            if !present {
                return Err(HlsPreflightError::Capability(capability));
            }
        }
    }
    let (input, layout) = smoke_input();
    #[cfg(test)]
    let smoke_command = test_support::probe_command_for(prepared, &[])
        .unwrap_or_else(|| build_command(prepared, &layout));
    #[cfg(not(test))]
    let smoke_command = build_command(prepared, &layout);
    let output = probe(smoke_command, input, &mut cancel, deadline).await?;
    validate_smoke(&output, layout)
}
fn validate_smoke(output: &[u8], layout: TransportLayout) -> Result<(), HlsPreflightError> {
    if output.is_empty() || !output.len().is_multiple_of(TS) {
        return Err(HlsPreflightError::Smoke);
    }
    let mut validator = OutputValidator::new(layout);
    let mut audio = false;
    for packet in output.as_chunks::<TS>().0 {
        validator
            .packet(packet, |_| {})
            .map_err(|_| HlsPreflightError::Smoke)?;
        if ts_pid(packet) == 257 && packet[3] & 0x10 != 0 {
            let start = if packet[3] & 0x20 != 0 {
                5 + packet[4] as usize
            } else {
                4
            };
            let payload = &packet[start..];
            // The smoke must carry an actual AAC ADTS frame, not just output PSI.
            audio |= payload
                .windows(2)
                .any(|w| w[0] == 0xff && w[1] & 0xf6 == 0xf0);
        }
    }
    if !validator.opened || !audio {
        return Err(HlsPreflightError::Smoke);
    }
    Ok(())
}

const PROBE_CAPTURE: usize = 256 * 1024;
/// Exactly three owned pumps, fixed reads and bounded capture. Finite preflight
/// exit is expected; streaming run_child deliberately treats the same exit as fatal.
async fn probe(
    mut command: Command,
    input: Vec<u8>,
    cancel: &mut watch::Receiver<bool>,
    aggregate: Instant,
) -> Result<Vec<u8>, HlsPreflightError> {
    if *cancel.borrow() {
        return Err(HlsPreflightError::Cancelled);
    }
    let deadline = aggregate.min(Instant::now() + Duration::from_secs(5));
    if Instant::now() >= deadline {
        return Err(HlsPreflightError::Timeout);
    }
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|_| HlsPreflightError::Executable)?;
    #[cfg(test)]
    PROBE_TEST_COUNTS
        .0
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let handles = (child.stdin.take(), child.stdout.take(), child.stderr.take());
    let mut pumps = JoinSet::new();
    let result = AssertUnwindSafe(async {
        let (Some(mut stdin),Some(mut stdout),Some(mut stderr)) = handles else { return Err(HlsPreflightError::Process); };
        let mut stdin_cancel = cancel.clone();
        let mut stdout_cancel = cancel.clone();
        let mut stderr_cancel = cancel.clone();
        pumps.spawn(async move {
            tokio::select! { biased;
                _ = cancelled(&mut stdin_cancel) => return Err(HlsPreflightError::Cancelled),
                result = stdin.write_all(&input) => result.map_err(|_|HlsPreflightError::Process)?,
            }
            drop(stdin);
            Ok(None)
        });
        pumps.spawn(async move {
            let mut output = Vec::with_capacity(PROBE_CAPTURE); let mut read = [0;STDOUT_READ];
            loop { let n = tokio::select! { biased;
                _ = cancelled(&mut stdout_cancel) => return Err(HlsPreflightError::Cancelled),
                result = stdout.read(&mut read) => result.map_err(|_|HlsPreflightError::Process)?,
            };
                if n == 0 { return Ok(Some(output)); }
                if n > PROBE_CAPTURE-output.len() { return Err(HlsPreflightError::Capture); }
                output.extend_from_slice(&read[..n]);
            }
        });
        #[cfg(test)]
        PROBE_TEST_COUNTS.2.fetch_add(3, std::sync::atomic::Ordering::SeqCst);
        pumps.spawn(async move {
            let mut read = [0;4096]; let mut tail = [0;STDERR_TAIL]; let mut len = 0;
            loop { let n = tokio::select! { biased;
                _ = cancelled(&mut stderr_cancel) => return Err(HlsPreflightError::Cancelled),
                result = stderr.read(&mut read) => result.map_err(|_|HlsPreflightError::Process)?,
            };
                if n == 0 { return Ok(None); }
                let keep = len.min(STDERR_TAIL-n); tail.copy_within(len-keep..len,0);
                tail[keep..keep+n].copy_from_slice(&read[..n]); len=keep+n;
            }
        });
        let mut status = None; let mut output = None;
        while status.is_none() || !pumps.is_empty() {
            tokio::select! { biased;
                _ = cancelled(cancel) => return Err(HlsPreflightError::Cancelled),
                _ = tokio::time::sleep_until(deadline) => return Err(HlsPreflightError::Timeout),
                exit = child.wait(), if status.is_none() => {
                    let exit = exit.map_err(|_|HlsPreflightError::Process)?;
                    if !exit.success() { return Err(HlsPreflightError::Process); }
                    status = Some(exit);
                }
                pump = pumps.join_next(), if !pumps.is_empty() => {
                    if let Some(bytes) = pump.ok_or(HlsPreflightError::Process)?.map_err(|_|HlsPreflightError::Process)?? { output=Some(bytes); }
                }
            }
        }
        output.ok_or(HlsPreflightError::Process)
    }).catch_unwind().await.unwrap_or(Err(HlsPreflightError::Process));
    pumps.abort_all();
    if !matches!(
        tokio::time::timeout(Duration::from_secs(1), child.wait()).await,
        Ok(Ok(_))
    ) {
        let _ = child.start_kill();
        while child.wait().await.is_err() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    while pumps.join_next().await.is_some() {}
    #[cfg(test)]
    {
        PROBE_TEST_COUNTS
            .1
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        PROBE_TEST_COUNTS
            .3
            .fetch_add(3, std::sync::atomic::Ordering::SeqCst);
    }
    result
}

#[cfg(test)]
fn probe_test_counts() -> (usize, usize, usize, usize) {
    use std::sync::atomic::Ordering::SeqCst;
    (
        PROBE_TEST_COUNTS.0.load(SeqCst),
        PROBE_TEST_COUNTS.1.load(SeqCst),
        PROBE_TEST_COUNTS.2.load(SeqCst),
        PROBE_TEST_COUNTS.3.load(SeqCst),
    )
}

#[cfg(test)]
fn reset_probe_test_counts() {
    use std::sync::atomic::Ordering::SeqCst;
    for count in [
        &PROBE_TEST_COUNTS.0,
        &PROBE_TEST_COUNTS.1,
        &PROBE_TEST_COUNTS.2,
        &PROBE_TEST_COUNTS.3,
    ] {
        count.store(0, SeqCst);
    }
}

#[cfg(test)]
mod task4_preflight_tests {
    use super::*;
    use test_support::{fake_prepared_child, process_test_lock, ChildMode};

    #[tokio::test]
    async fn preflight_path_resolution_is_canonical_and_survives_path_mutation() {
        let _serial = process_test_lock().await;
        let old_path = std::env::var_os("PATH");
        let unique = format!(
            "outpace-hls-path-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::temp_dir().join(unique);
        let first = root.join("first");
        let second = root.join("second");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        let executable = first.join("ffmpeg-test");
        std::fs::write(&executable, b"synthetic executable placeholder").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        std::env::set_var("PATH", &first);
        let prepared = PreparedNormalizer {
            executable: resolve_executable(std::path::Path::new("ffmpeg-test")).unwrap(),
        };
        assert_eq!(prepared.executable, executable.canonicalize().unwrap());

        std::env::set_var("PATH", &second);
        let command = build_command(&prepared, &smoke_input().1);
        assert_eq!(
            command.as_std().get_program(),
            prepared.executable.as_os_str(),
            "subsequent child construction must retain the canonical preflight result"
        );
        assert!(
            resolve_executable(std::path::Path::new("ffmpeg-test")).is_err(),
            "the mutated PATH no longer contains the binary name"
        );

        match old_path {
            Some(path) => std::env::set_var("PATH", path),
            None => std::env::remove_var("PATH"),
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    async fn run_fake_probe(prepared: &PreparedNormalizer) -> Result<Vec<u8>, HlsPreflightError> {
        let command = test_support::probe_command_for(prepared, &[]).expect("fake child factory");
        let (_cancel_tx, mut cancel) = watch::channel(false);
        probe(
            command,
            Vec::new(),
            &mut cancel,
            Instant::now() + Duration::from_secs(15),
        )
        .await
    }

    #[tokio::test]
    async fn preflight_probe_rejects_nonzero_exit_and_capture_overflow_and_reaps() {
        let _serial = process_test_lock().await;
        reset_probe_test_counts();
        let (nonzero, _) = fake_prepared_child(ChildMode::ExitFailure);
        assert_eq!(
            run_fake_probe(&nonzero).await,
            Err(HlsPreflightError::Process)
        );
        let (flood, _) = fake_prepared_child(ChildMode::StdoutFlood);
        assert_eq!(
            run_fake_probe(&flood).await,
            Err(HlsPreflightError::Capture)
        );
        let (started, reaped, pumps, joined) = probe_test_counts();
        assert_eq!((started, reaped, pumps, joined), (2, 2, 6, 6));
    }

    #[tokio::test]
    async fn preflight_requires_each_mpeg_audio_decoder_capability() {
        let _serial = process_test_lock().await;
        let old = std::env::var_os("OUTPACE_TEST_HLS_AUDIO_MISSING_CAPABILITY");
        for missing in ["mp2", "mp3", "ac3"] {
            std::env::set_var("OUTPACE_TEST_HLS_AUDIO_MISSING_CAPABILITY", missing);
            let (prepared, _) = fake_prepared_child(ChildMode::CapabilityListing);
            let (_cancel_tx, cancel) = watch::channel(false);
            let result =
                preflight_prepared(&prepared, cancel, Instant::now() + Duration::from_secs(10))
                    .await;
            assert!(
                matches!(result, Err(HlsPreflightError::Capability(cap)) if cap == missing),
                "missing required decoder {missing} should be reported exactly"
            );
        }
        match old {
            Some(value) => std::env::set_var("OUTPACE_TEST_HLS_AUDIO_MISSING_CAPABILITY", value),
            None => std::env::remove_var("OUTPACE_TEST_HLS_AUDIO_MISSING_CAPABILITY"),
        }
    }

    #[tokio::test]
    async fn preflight_accepts_missing_layer_one_decoder_when_required_codecs_exist() {
        let _serial = process_test_lock().await;
        let old = std::env::var_os("OUTPACE_TEST_HLS_AUDIO_MISSING_CAPABILITY");
        std::env::set_var("OUTPACE_TEST_HLS_AUDIO_MISSING_CAPABILITY", "mp1");
        let (prepared, _) = fake_prepared_child(ChildMode::CapabilityListing);
        let (_cancel_tx, cancel) = watch::channel(false);
        let result =
            preflight_prepared(&prepared, cancel, Instant::now() + Duration::from_secs(10)).await;
        assert_eq!(
            result,
            Err(HlsPreflightError::Smoke),
            "Layer I absence is not a capability failure; the fake smoke output still fails later"
        );
        match old {
            Some(value) => std::env::set_var("OUTPACE_TEST_HLS_AUDIO_MISSING_CAPABILITY", value),
            None => std::env::remove_var("OUTPACE_TEST_HLS_AUDIO_MISSING_CAPABILITY"),
        }
    }

    #[tokio::test]
    async fn preflight_runs_all_capability_probes_sequentially_before_smoke() {
        let _serial = process_test_lock().await;
        reset_probe_test_counts();
        let old = std::env::var_os("OUTPACE_TEST_HLS_AUDIO_MISSING_CAPABILITY");
        std::env::remove_var("OUTPACE_TEST_HLS_AUDIO_MISSING_CAPABILITY");
        let (prepared, _) = fake_prepared_child(ChildMode::CapabilityListing);
        let (_cancel_tx, cancel) = watch::channel(false);
        assert_eq!(
            preflight_prepared(&prepared, cancel, Instant::now() + Duration::from_secs(10)).await,
            Err(HlsPreflightError::Smoke),
            "all five capability listings pass before the fake empty smoke output is rejected",
        );
        let (started, reaped, pumps, joined) = probe_test_counts();
        assert_eq!((started, reaped, pumps, joined), (6, 6, 18, 18));
        match old {
            Some(value) => std::env::set_var("OUTPACE_TEST_HLS_AUDIO_MISSING_CAPABILITY", value),
            None => std::env::remove_var("OUTPACE_TEST_HLS_AUDIO_MISSING_CAPABILITY"),
        }
    }

    #[tokio::test]
    async fn preflight_capability_sequence_obeys_one_ten_second_aggregate_deadline() {
        let _serial = process_test_lock().await;
        reset_probe_test_counts();
        let old = std::env::var_os("OUTPACE_TEST_HLS_AUDIO_PROBE_DELAY_MS");
        std::env::set_var("OUTPACE_TEST_HLS_AUDIO_PROBE_DELAY_MS", "2000");
        let (prepared, _) = fake_prepared_child(ChildMode::CapabilityListing);
        let (_cancel_tx, cancel) = watch::channel(false);
        let start = Instant::now();
        let result = preflight_prepared(&prepared, cancel, start + Duration::from_secs(10)).await;
        assert_eq!(result, Err(HlsPreflightError::Timeout));
        let elapsed = Instant::now().duration_since(start);
        assert!(
            elapsed >= Duration::from_secs(9),
            "aggregate deadline should outlast four sequential probes: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(12),
            "aggregate timeout should bound the full sequence: {elapsed:?}"
        );
        let (started, reaped, pumps, joined) = probe_test_counts();
        assert_eq!((started, reaped, pumps, joined), (5, 5, 15, 15));
        match old {
            Some(value) => std::env::set_var("OUTPACE_TEST_HLS_AUDIO_PROBE_DELAY_MS", value),
            None => std::env::remove_var("OUTPACE_TEST_HLS_AUDIO_PROBE_DELAY_MS"),
        }
    }

    #[tokio::test]
    async fn preflight_probe_cancellation_kills_reaps_child_and_joins_pumps() {
        let _serial = process_test_lock().await;
        reset_probe_test_counts();
        let (prepared, _) = fake_prepared_child(ChildMode::BlockStdin);
        let command = test_support::probe_command_for(&prepared, &[]).unwrap();
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            probe(
                command,
                Vec::new(),
                &mut cancel_rx,
                Instant::now() + Duration::from_secs(10),
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel_tx.send_replace(true);
        assert_eq!(task.await.unwrap(), Err(HlsPreflightError::Cancelled));
        let (started, reaped, pumps, joined) = probe_test_counts();
        assert_eq!((started, reaped, pumps, joined), (1, 1, 3, 3));
    }

    #[tokio::test(start_paused = true)]
    async fn preflight_probe_enforces_five_second_per_probe_deadline() {
        let _serial = process_test_lock().await;
        reset_probe_test_counts();
        let (prepared, _) = fake_prepared_child(ChildMode::BlockStdin);
        let command = test_support::probe_command_for(&prepared, &[]).unwrap();
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            probe(
                command,
                Vec::new(),
                &mut cancel_rx,
                Instant::now() + Duration::from_secs(30),
            )
            .await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(task.await.unwrap(), Err(HlsPreflightError::Timeout));
        let (started, reaped, pumps, joined) = probe_test_counts();
        assert_eq!((started, reaped, pumps, joined), (1, 1, 3, 3));
    }
}

#[cfg(test)]
mod executable_resolution_tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        let unique = format!(
            "outpace-executable-resolution-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn make_executable(path: &std::path::Path) {
        std::fs::write(path, b"synthetic executable placeholder").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    #[test]
    fn windows_bare_name_finds_exe_from_search_path() {
        let root = temp_dir();
        let binary = root.join("ffmpeg.exe");
        make_executable(&binary);

        assert_eq!(
            resolve_executable_with(std::path::Path::new("ffmpeg"), Some(root.as_os_str()), true,),
            Some(binary)
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn windows_explicit_path_accepts_missing_exe_suffix_and_preserves_existing_suffix() {
        let root = temp_dir();
        let binary = root.join("ffmpeg.exe");
        make_executable(&binary);

        assert_eq!(
            resolve_executable_with(&root.join("ffmpeg"), None, true),
            Some(binary.clone())
        );
        assert_eq!(resolve_executable_with(&binary, None, true), Some(binary));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn windows_bare_exe_name_matches_without_appending_another_suffix() {
        let root = temp_dir();
        let binary = root.join("ffmpeg.exe");
        make_executable(&binary);
        let search_path = root.as_os_str().to_owned();

        assert_eq!(
            resolve_executable_with(std::path::Path::new("ffmpeg.exe"), Some(&search_path), true,),
            Some(binary)
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn windows_bare_dotted_name_is_exact_and_extensionless_file_does_not_beat_exe() {
        let root = temp_dir();
        let dotted_exact = root.join("ffmpeg.custom");
        let dotted_exe = root.join("ffmpeg.custom.exe");
        make_executable(&dotted_exact);
        make_executable(&dotted_exe);
        let dotted_search_path = root.as_os_str().to_owned();
        assert_eq!(
            resolve_executable_with(
                std::path::Path::new("ffmpeg.custom"),
                Some(&dotted_search_path),
                true,
            ),
            Some(dotted_exact),
            "a dotted bare name must resolve the exact file, not its .exe sibling"
        );

        let extensionless = root.join("ffmpeg");
        make_executable(&extensionless);
        let binary = root.join("ffmpeg.exe");
        make_executable(&binary);
        assert_eq!(
            resolve_executable_with(
                std::path::Path::new("ffmpeg"),
                Some(&dotted_search_path),
                true,
            ),
            Some(binary),
            "Windows bare names must choose .exe, never an extensionless file"
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn windows_explicit_path_prefers_appended_exe_then_falls_back_to_original() {
        let root = temp_dir();
        let original = root.join("ffmpeg.bin");
        let appended = root.join("ffmpeg.bin.exe");
        make_executable(&original);
        make_executable(&appended);
        assert_eq!(
            resolve_executable_with(&original, None, true),
            Some(appended)
        );

        std::fs::remove_file(root.join("ffmpeg.bin.exe")).unwrap();
        assert_eq!(
            resolve_executable_with(&original, None, true),
            Some(original)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unix_explicit_path_does_not_add_exe() {
        let root = temp_dir();
        let original = root.join("ffmpeg");
        let appended = root.join("ffmpeg.exe");
        make_executable(&appended);
        assert_eq!(resolve_executable_with(&original, None, false), None);

        make_executable(&original);
        assert_eq!(
            resolve_executable_with(&original, None, false),
            Some(original)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn missing_search_path_does_not_search_the_current_directory() {
        let name = format!(
            "outpace-executable-resolution-cwd-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let binary = std::env::current_dir().unwrap().join(&name);
        make_executable(&binary);
        assert_eq!(
            resolve_executable_with(std::path::Path::new(&name), None, false),
            None,
            "a missing PATH must not cause an implicit cwd search"
        );
        std::fs::remove_file(binary).unwrap();
    }

    #[test]
    fn windows_empty_search_path_entries_are_skipped() {
        let root = temp_dir();
        let name = format!(
            "outpace-empty-path-{}-{}.exe",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let binary = root.join(&name);
        make_executable(&binary);
        let cwd_binary = std::env::current_dir().unwrap().join(&name);
        make_executable(&cwd_binary);
        let paths = std::env::join_paths([std::ffi::OsStr::new(""), root.as_os_str()]).unwrap();
        assert_eq!(
            resolve_executable_with(std::path::Path::new(&name), Some(&paths), true),
            Some(binary)
        );
        std::fs::remove_file(cwd_binary).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn native_windows_explicit_path_is_canonical_and_retained_for_children() {
        let root = temp_dir();
        let binary = root.join("ffmpeg-test.exe");
        make_executable(&binary);

        let prepared = PreparedNormalizer {
            executable: resolve_executable(&binary).unwrap(),
        };
        assert_eq!(prepared.executable, binary.canonicalize().unwrap());
        let command = build_command(&prepared, &smoke_input().1);
        assert_eq!(
            command.as_std().get_program(),
            prepared.executable.as_os_str()
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unix_name_resolution_does_not_add_exe() {
        let root = temp_dir();
        let exe_only = root.join("ffmpeg.exe");
        make_executable(&exe_only);
        let search_path = root.as_os_str().to_owned();

        assert_eq!(
            resolve_executable_with(std::path::Path::new("ffmpeg"), Some(&search_path), false,),
            None
        );

        let extensionless = root.join("ffmpeg");
        make_executable(&extensionless);
        assert_eq!(
            resolve_executable_with(std::path::Path::new("ffmpeg"), Some(&search_path), false,),
            Some(extensionless)
        );

        std::fs::remove_dir_all(root).unwrap();
    }
}

/// Entirely synthetic elementary samples: FFmpeg-generated black 16x16 MPEG-2
/// I picture and one stereo 48 kHz/128 kbit AC3 silence frame. The runtime builds
/// PSI, PES timestamps, PCR and packet framing in memory; it needs no generators.
fn smoke_input() -> (Vec<u8>, TransportLayout) {
    const VIDEO: &[u8] = &[
        0x00, 0x00, 0x01, 0xb3, 0x01, 0x00, 0x10, 0x12, 0xff, 0xff, 0xe0, 0x18, 0x00, 0x00, 0x01,
        0xb5, 0x14, 0x8a, 0x00, 0x01, 0x00, 0x17, 0x00, 0x00, 0x01, 0xb8, 0x00, 0x08, 0x00, 0x40,
        0x00, 0x00, 0x01, 0x00, 0x00, 0x0f, 0xff, 0xf8, 0x00, 0x00, 0x01, 0xb5, 0x8f, 0xff, 0xf3,
        0x41, 0x80, 0x00, 0x00, 0x01, 0x01, 0x13, 0xf8, 0x7d, 0x29, 0x48, 0x88,
    ];
    const AUDIO: &[u8] = &[
        0x0b, 0x77, 0x79, 0xf3, 0x10, 0x40, 0x43, 0xe1, 0x06, 0xf0, 0xb0, 0x18, 0x40, 0x40, 0x40,
        0x40, 0x40, 0x40, 0x41, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x21, 0x5c, 0x7c, 0xf9,
        0xf3, 0xe7, 0xcf, 0x9f, 0x3e, 0x7c, 0xf9, 0xf3, 0xe7, 0xcf, 0x9f, 0x3e, 0x7c, 0xf9, 0xf3,
        0xe7, 0xcf, 0x9f, 0x3e, 0x7c, 0xf9, 0xf3, 0xe7, 0xcf, 0x9f, 0x3e, 0xff, 0x9d, 0x5f, 0x3e,
        0x7c, 0xf9, 0xf3, 0xe7, 0xcf, 0x9f, 0x3e, 0x7c, 0xf9, 0xf3, 0xe3, 0xfe, 0x75, 0x7c, 0xf9,
        0xf3, 0xe7, 0xcf, 0x9f, 0x3e, 0x7c, 0xf9, 0xf3, 0xe7, 0xcf, 0x8c, 0xbf, 0xc6, 0x10, 0x20,
        0x48, 0x00, 0x00, 0x00, 0x00, 0x07, 0x6f, 0x1e, 0x3c, 0x78, 0xf1, 0xe3, 0xbb, 0xbb, 0xbb,
        0xb8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x6b, 0x5a, 0xd6, 0xb5, 0xad, 0x6b, 0x5a, 0xd6, 0xb5, 0xad, 0x00, 0x00, 0x00, 0x00, 0xed,
        0xe3, 0xc7, 0x8f, 0x1e, 0x3c, 0x77, 0x77, 0x77, 0x77, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        0xdb, 0xc7, 0x8f, 0x1e, 0x3c, 0x78, 0xee, 0xee, 0xee, 0xee, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1a, 0xd6, 0xb5, 0xad, 0x6b, 0x5a,
        0xd6, 0xb5, 0xad, 0x6b, 0x40, 0x00, 0x00, 0x00, 0x3b, 0x78, 0xf1, 0xe3, 0xc7, 0x8f, 0x1d,
        0xdd, 0xdd, 0xdd, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x76, 0xf1, 0xe3, 0xc7, 0x8f, 0x1e,
        0x3b, 0xbb, 0xbb, 0xbb, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x06, 0xb5, 0xad, 0x6b, 0x5a, 0xd6, 0xb5, 0xad, 0x6b, 0x5a, 0xd0, 0x00,
        0x00, 0x00, 0x0e, 0xde, 0x3c, 0x78, 0xf1, 0xe3, 0xc7, 0x77, 0x77, 0x77, 0x70, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x60, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x1d, 0xbc, 0x78, 0xf1, 0xe3, 0xc7, 0x8e, 0xee, 0xee, 0xee, 0xe0, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xad, 0x6b,
        0x5a, 0xd6, 0xb5, 0xad, 0x6b, 0x5a, 0xd6, 0xb4, 0x00, 0x00, 0x00, 0x03, 0xb7, 0x8f, 0x1e,
        0x3c, 0x78, 0xf1, 0xdd, 0xdd, 0xdd, 0xdc, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x6f, 0x1e,
        0x3c, 0x78, 0xf1, 0xe3, 0xbb, 0xbb, 0xbb, 0xb8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6b, 0x5a, 0xd6, 0xb5, 0xad, 0x6b, 0x5a, 0xd6,
        0xb5, 0xad, 0x00, 0x00, 0x00, 0x00, 0xed, 0xe3, 0xc7, 0x8f, 0x1e, 0x3c, 0x77, 0x77, 0x77,
        0x77, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xdb, 0xc7, 0x8f, 0x1e, 0x3c, 0x78, 0xee, 0xee,
        0xee, 0xee, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x1a, 0xd6, 0xb5, 0xad, 0x6b, 0x5a, 0xd6, 0xb5, 0xad, 0x6b, 0x40, 0x00, 0x00, 0x00,
        0x3b, 0x78, 0xf1, 0xe3, 0xc7, 0x8f, 0x1d, 0xdd, 0xdd, 0xdd, 0xc0, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x22, 0x2a,
    ];
    fn table(pid: u16, mut section: Vec<u8>) -> [u8; TS] {
        let length = section.len() + 1;
        section[1] = 0xb0 | ((length >> 8) as u8 & 15);
        section[2] = length as u8;
        let crc = mpeg_crc32(&section);
        section.extend_from_slice(&crc.to_be_bytes());
        let mut packet = [0xff; TS];
        packet[..5].copy_from_slice(&[0x47, 0x40 | ((pid >> 8) as u8), pid as u8, 0x10, 0]);
        packet[5..5 + section.len()].copy_from_slice(&section);
        packet
    }
    fn pes(out: &mut Vec<u8>, pid: u16, stream: u8, pts: u64, es: &[u8], cc: &mut u8, video: bool) {
        let mut data = vec![0, 0, 1, stream];
        let len = if video { 0 } else { es.len() + 8 };
        data.extend_from_slice(&(len as u16).to_be_bytes());
        data.extend_from_slice(&[
            0x80,
            0x80,
            5,
            0x21 | (((pts >> 30) as u8 & 7) << 1),
            (pts >> 22) as u8,
            (((pts >> 15) as u8 & 127) << 1) | 1,
            (pts >> 7) as u8,
            ((pts as u8 & 127) << 1) | 1,
        ]);
        data.extend_from_slice(es);
        let mut offset = 0;
        while offset < data.len() {
            let first = offset == 0;
            let max = if first && video { 176 } else { 184 };
            let n = max.min(data.len() - offset);
            let adaptation = 184 - n;
            let mut packet = [0xff; TS];
            packet[..4].copy_from_slice(&[
                0x47,
                ((pid >> 8) as u8) | if first { 0x40 } else { 0 },
                pid as u8,
                (if adaptation > 0 { 0x30 } else { 0x10 }) | *cc,
            ]);
            *cc = (*cc + 1) & 15;
            if adaptation > 0 {
                packet[4] = (adaptation - 1) as u8;
                if adaptation > 1 {
                    packet[5] = if first && video { 0x50 } else { 0 };
                }
                if first && video {
                    packet[6] = (pts >> 25) as u8;
                    packet[7] = (pts >> 17) as u8;
                    packet[8] = (pts >> 9) as u8;
                    packet[9] = (pts >> 1) as u8;
                    packet[10] = ((pts as u8 & 1) << 7) | 0x7e;
                    packet[11] = 0;
                }
            }
            packet[TS - n..].copy_from_slice(&data[offset..offset + n]);
            offset += n;
            out.extend_from_slice(&packet);
        }
    }
    let pat = table(0, vec![0, 0xb0, 0, 0, 7, 0xc1, 0, 0, 0, 9, 0xf0, 0]);
    let pmt = table(
        4096,
        vec![
            2, 0xb0, 0, 0, 9, 0xc1, 0, 0, 0xe1, 0, 0xf0, 0, 2, 0xe1, 0, 0xf0, 0, 6, 0xe1, 1, 0xf0,
            6, 5, 4, b'A', b'C', b'-', b'3',
        ],
    );
    let mut out = Vec::new();
    let mut video = 0;
    let mut audio = 0;
    let mut vcc = 0;
    let mut acc = 0;
    while video < 25 || audio < 32 {
        if video < 25 && (audio == 32 || video * 3600 <= audio * 2880) {
            out.extend_from_slice(&pat);
            out.extend_from_slice(&pmt);
            pes(
                &mut out,
                256,
                0xe0,
                90_000 + video * 3600,
                VIDEO,
                &mut vcc,
                true,
            );
            video += 1;
        } else {
            pes(
                &mut out,
                257,
                0xbd,
                90_000 + audio * 2880,
                AUDIO,
                &mut acc,
                false,
            );
            audio += 1;
        }
    }
    let mut selector = LiveAudioSelection::default();
    selector.observe(pat);
    selector.observe(pmt);
    let LayoutDecision::Supported(layout) = selector.layout() else {
        unreachable!("fixed synthetic PSI")
    };
    (out, layout)
}

#[cfg(test)]
pub(super) fn test_smoke_input() -> (Vec<u8>, TransportLayout) {
    smoke_input()
}
