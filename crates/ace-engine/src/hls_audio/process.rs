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
#[cfg(test)]
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::process::Command;
use tokio::sync::{mpsc, watch, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Instant;

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../tests/support/hls_audio.rs"]
mod test_fixtures;

pub const INPUT_BYTES: usize = 1_048_576;
const STDOUT_READ: usize = 32_768;
const STDERR_TAIL: usize = 8_192;
const TS: usize = 188;
const STALL: Duration = Duration::from_secs(45);

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

    /// The borrowed broadcast allocation remains session-owned until permits are
    /// available. Copy only the bounded view, so no retained tiny slice pins it.
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

async fn cancelled(cancel: &mut watch::Receiver<bool>) {
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
    run_command(
        build_command(&prepared, &layout),
        layout,
        generation,
        media,
        input,
        cancel,
        deadline,
        Observer::default(),
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
        Self {
            source,
            selector: LiveAudioSelection::default(),
            access: VideoAccessPointState::new(),
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
    use std::process::{Command as StdCommand, Stdio};

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
