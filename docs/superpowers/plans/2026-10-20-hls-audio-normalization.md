# Live HLS Audio Normalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add default-off, bounded live-HLS audio normalization while preserving native AAC/video-only, raw TS and VOD behavior.

**Architecture:** Extend the existing strict media selector with layout metadata; a manager-owned worker supervises native packaging or one FFmpeg child per transformed generation. Separate readable retained media from cancellation/join ownership, and publish generation-aware readiness atomically with playlists. Fixed pipe output returns to the existing segmenter.

**Tech Stack:** Rust, existing Tokio process/sync/time facilities, MPEG-TS, operator-supplied FFmpeg; test-only ffprobe and synthetic generators.

**Spec:** `docs/superpowers/specs/2026-10-20-hls-audio-normalization-design.md` at `d834bc2` (authoritative over `/tmp/outpace-200/design-audit.md`).

## Global Constraints

- `OUTPACE_HLS_AUDIO_NORMALIZE`: boolean, default `false`; accept `true`/`false` and `1`/`0`, reject other values at configuration load.
- `OUTPACE_FFMPEG_PATH`: executable path or bare executable name, default `ffmpeg`; reject an empty value when enabled.
- Keep a separate cloned normalization configuration under `Config`/manager so `HlsConfig` remains Copy.
- Each probe has a five-second deadline, all preflight work shares a ten-second absolute deadline, and captured probe stdout is capped at 256 KiB per invocation.
- Accept unencrypted 188-byte TS, one current program, single-section PAT/PMT contained in single packets under the existing strict parser constraints.
- FFmpeg copies video and encodes selected audio to stereo AAC-LC, 48 kHz, 128 kbit/s.
- Assign deterministic output PIDs: PMT 4096, video 256, audio 257; output PCR must be on video 256.
- Bound demux probing to 1 MiB and one second of media analysis; classification shares the existing absolute HLS startup deadline and a 1 MiB aggregate input budget.
- One 1 MiB input byte budget per worker includes classification/replay buffers, queued bytes and the active stdin write slice.
- One supervisor plus at most three owned pumps (stdin, stdout, stderr) per worker, independent of chunk count.
- Fixed stdout reads of at most 32 KiB feed the bounded packager directly; no extra stdout queue. TS framing stages at most one incomplete packet.
- Continuously drain stderr with fixed reads and retain only an 8 KiB tail, without allocating unbounded lines.
- Four normalization children globally per daemon, shared across managers if multiple managers exist.
- Permit three recovery invalidations per rolling 60 seconds per packager; initial start is not counted, coalesced markers count once, the fourth invalidation fails terminally.
- After readiness, fail when there is no valid complete output TS packet for 45 seconds while nonempty input remains pending or accepted input has not produced output.
- Cleanup allows up to one second graceful exit, then requests kill and always waits; admission remains held until reap. Killed-process wait has no universal OS completion bound.
- Existing segment ceilings, window retention, PCR/access-point segmentation, URLs, authorization and compatibility pins remain authoritative. Raw TS and VOD remain unchanged.
- Disabled mode performs no executable lookup, probing or child admission and preserves existing media-selection and HTTP behavior.
- No video encoding, Safari guarantee, ABR, UI, silence insertion, language selection, fMP4, arbitrary FFmpeg arguments or general media pipeline is included.
- No network/bind changes, registry access, real identifiers/media, Docker/compose package installation, new Rust codec dependency or wrapper-script/process-group support.
- Libraries stay silent; engine operational logs use `alog!` categories/generation/status only, never stderr, titles or command/media text.

## Review Focus

1. Stop or request cancellation races with initialization/publication: no resurrected entry or abandoned cleanup (Task 2).
2. A discontinuity on an excluded PID and a delayed paired marker: exactly one recovery, token expires at opening access point (Task 4).
3. A tiny pending slice backed by a huge broadcast allocation: charged owned allocation never escapes the input budget (Task 3).
4. Partial stdout and blocked stdin after Ready: input does not refresh the stall clock; drained idle input does not fail (Task 3).
5. Expiry/removal races with terminal failure or readiness: authorization 404 wins, and old Ready cannot render across a boundary (Task 5).

---

## Execution and file ownership

Preserved method: fresh subagent implementation and fresh review per coherent task, then whole-branch review. GPT-6.1 Sol owns product code and reviews; GPT-6 Luna owns **all test writing and every command verification**, including expected-red runs, builds, formatting, lint and hygiene. Do not substitute Luna product implementation from the older audit. Group each task's test/product/review work using the interfaces below; execute tasks sequentially because they share files. No setup-only task or simultaneous ownership of the same file. Each task's commit follows its green test/review gate; only named task files are staged. Implementers are not alone: preserve unrelated changes and the root checkout.

File map: `ace-media/src/audio.rs` owns parsing/selection metadata; `ace-engine/src/hls.rs` owns media/generation state and packaging; new `hls_audio.rs` owns classification/supervision/preflight; new `hls_audio/process.rs` owns fixed process configuration and cleanup; `manager.rs` owns entry/control/cleanup lifecycle. `config.rs`, `runtime.rs`, `lib.rs` wire startup. HTTP modules consume snapshots. Tests live beside these modules plus new `tests/hls_audio_faults.rs`, `tests/hls_audio_ffmpeg.rs`, `tests/support/hls_audio.rs` and a test-only child-mode module `tests/support/hls_audio_child.rs`. README and `.github/workflows/ci.yml` own deployment/CI prerequisites. Paths below are relative to `crates/` unless explicitly rooted.

Use the current `StreamManager::with_config(registry, buffer, HlsConfig)` as a disabled-compatible constructor; add a separate enabled constructor rather than changing every caller. `HlsPackager::start` currently returns only an Arc and detaches a task; replace internal manager usage with an owned worker pair. Preserve public compatibility entrypoints if needed, but none may detach an untracked worker. Keep `VodHlsLayout` untouched.

### Task 1: Strict layout metadata with unchanged disabled selection

**Files:** Modify/test `ace-media/src/audio.rs`; add synthetic PSI cases alongside existing tests (no external media).

**Interfaces:**
- Consumes existing `AudioCodec`, `LiveAudioSelection::{observe,reset,tables,pcr_pid}`, `AudioPacket { packet, changed }` and strict `section`/descriptor parsing.
- Produces `LiveAudioSelection::layout(&self) -> LayoutDecision`; public cloned/equatable `LayoutDecision::{Pending, Supported(TransportLayout), Unsupported}`.
- `TransportLayout` fields: `transport_stream_id`, `program_id`, `pmt_pid`, `pcr_pid`, `video_pid: u16`, `video_type: u8`, `audio: Option<SelectedAudio>`, `semantic_identity: Vec<u8>`; `SelectedAudio { pid: u16, codec: AudioCodec }`. Identity excludes PSI version/CRC/continuity but includes relevant descriptors/selection. `Unsupported` is current selected-program evidence; invalid/noncurrent/wrong-program tables leave the previous decision intact. Native selector packet behavior remains independent of the strict decision.

- [ ] **Step 1 (Luna): Write failing `normalization_policy_*` tests.** Table-driven assertions: AAC after earlier MPEG/AC3 wins; otherwise first supported entry wins; `0x03/0x04 => MpegAudio` without claiming layer; validated AC3 descriptors accepted; no-audio plus known video succeeds; LATM/EAC3/DTS-only, unknown private, scrambled payload, audio-only, multi-video/program, fragmented PSI and invalid PCR reject. Invalid/noncurrent/wrong-program tables preserve known layout; current unsupported tables return Unsupported. `assert_eq!(version_only.layout(), baseline.layout())`; descriptor/selected-PID/PCR/video changes differ. Assert disabled `observe` bytes match existing fixtures, including unsupported legacy passthrough.
- [ ] **Step 2 (Luna):** Run `cargo test -p ace-media normalization_policy_ -- --nocapture`; require compilation failure for missing metadata API or assertion failure before implementation.
- [ ] **Step 3 (Sol):** Implement the interface in the existing parser, retaining a single strict PSI parser and the current native rewrite rules. Reject unidentified private entries even beside supported audio; confidently known competing audio may be filtered. Validate every packet for encryption in enabled consumption, not only PSI.
- [ ] **Step 4 (Luna):** Run the focused command and `cargo test -p ace-media`; require PASS with existing selector tests unchanged.
- [ ] **Step 5 (Sol):** Review and commit named files as `feat(ace-media): expose strict live transport layout`.

### Task 2: Owned workers and generation-aware retained media

**Files:** Modify/test `ace-engine/src/{hls.rs,manager.rs,runtime.rs}`; add lifecycle tests beside existing manager/HLS tests.

**Interfaces:**
- Consumes Task 1 metadata, `StreamSession::raw_receiver()`, current per-key `HlsTransition`, compatibility subscription pins and retained segment methods.
- Produces `GenerationId(u64)`; `HlsStatus::{Starting,Ready,Recovering,Failed(HlsFailure),Stopped}`; category-only cloned `HlsFailure` enum (Unsupported, Deadline, InputOverflow, Admission, ChildExit, Pipe, Pump, OutputInvalid, OutputStall, RecoveryLimit).
- `HlsPackager::create(session: &StreamSession, config: HlsConfig, strict: bool) -> Arc<HlsPackager>` creates media only. `begin_generation(&self, generation: GenerationId, deadline: tokio::time::Instant, recovering: bool)`, `feed_generation(&self, generation: GenerationId, data: &[u8])`, `fail_generation(&self, generation: GenerationId, reason: HlsFailure)`, `stop(&self)` fence under the media mutex.
- `status(&self) -> HlsStatus`; `playlist_snapshot(&self, prefix: &str, native_access: bool) -> Result<String,HlsStatus>` checks and renders under one lock; `wait_available(&self, timeout: Duration) -> HlsStatus` uses registered notifications plus recheck and absolute generation deadline. Keep existing boolean/read/render wrappers for disabled callers until Task 5 migrates HTTP.
- Manager `HlsEntry { media: Arc<HlsPackager>, control: WorkerControl }`; `WorkerControl` owns a `watch::Sender<bool>` cancellation and `JoinHandle<()>`. `CleanupTracker` registers join ownership synchronously before spawn/publication and can drain without caller-future ownership. `StreamManager::shutdown(&self)` cancels reaper/entries, drains tracked cleanup, awaits workers. Worker tasks receive media + receiver, never manager/entry/control Arc. Reaper uses Weak manager plus separately owned cancellation/join control.

- [ ] **Step 1 (Luna): Write failing `hls_generation_*` and `hls_owned_*` tests.** Assert retained old segments cannot meet strict new-generation startup count, stale generation feed leaves segments unchanged, all waiters wake on Failed/Stopped within scheduler one second, boundary renders atomically or returns unavailable. Assert stale HTTP Arc does not keep worker alive after stop/reap; concurrent native/compat starts share one entry; stop during delayed initialization prevents publication; aborting initializer transfers cleanup; shutdown joins reaper and workers; unrelated-key start progresses while cleanup waits. Disabled readiness after gaps retains its historical behavior.
- [ ] **Step 2 (Luna):** Run `cargo test -p ace-engine hls_generation_` and `cargo test -p ace-engine hls_owned_`; require meaningful red assertions/missing API.
- [ ] **Step 3 (Sol):** Implement ownership and generation state. Strict generation reset clears partial segment, tables, input framing, selected clock and access-point state but preserves completed segments and monotonic sequences; exactly first new completed segment is discontinuous. Stop/reap detach and signal under lifecycle locks; await outside **all** manager/media locks. Register cleanup before returning a cancellable initializer. Manager Drop only signals synchronously; explicit shutdown awaits. Wire awaited shutdown after HTTP draining and on runtime startup failure; no self-owning reaper. Preserve old disabled media/HTTP semantics while improving ownership.
- [ ] **Step 4 (Luna):** Run focused commands plus `cargo test -p ace-engine manager::tests` and `cargo test -p ace-engine hls::tests`; require PASS, including existing pins/start-cancellation tests.
- [ ] **Step 5 (Sol):** Review and commit as `refactor(ace-engine): own HLS workers and generation state`.

### Task 3: Bounded subprocess boundary and fault harness

**Files:** Create `ace-engine/src/hls_audio.rs`, `ace-engine/src/hls_audio/process.rs`; register module in `ace-engine/src/lib.rs`; create/test `ace-engine/tests/hls_audio_faults.rs`, `ace-engine/tests/support/{hls_audio.rs,hls_audio_child.rs}`; no Cargo target changes are required.

**Interfaces:**
- Consumes Task 1 layout and Task 2 media/generation token/cancellation.
- Produces cloned `PreparedNormalizer { executable: PathBuf }`; `build_command(prepared: &PreparedNormalizer, layout: &TransportLayout) -> tokio::process::Command` (direct binary only).
- `ChildAdmission::global() -> Arc<ChildAdmission>` uses one process-wide semaphore of four; `try_acquire(&self) -> Result<OwnedSemaphorePermit,HlsFailure>` never queues.
- `InputBudget::new() -> Self`, async `acquire_owned(&self, bytes: Vec<u8>, cancel: &mut watch::Receiver<bool>) -> Result<BudgetedInput,HlsFailure>` charges owned capacity, splits/copies incoming broadcast data before retaining; at most 1 MiB including replay/queue/active writes. `BudgetedInput` releases permits as consumed owned slices are freed.
- `run_child(prepared: PreparedNormalizer, layout: TransportLayout, generation: GenerationId, media: Arc<HlsPackager>, input: mpsc::Receiver<BudgetedInput>, cancel: watch::Receiver<bool>, deadline: tokio::time::Instant) -> Result<(),HlsFailure>` owns Child/pumps/permit and returns only after cleanup. Test-only injected process factory and counters exercise the same production pumps without adding public configuration.

- [ ] **Step 1 (Luna): Write failing `hls_audio_faults` integration tests and portable Rust fake child target.** The fault integration executable includes the support module and a `fake_child_entry` test; launch `current_exe()` with `--exact fake_child_entry --nocapture` and explicit test-only mode environment. The entry test returns immediately without that mode; ordinary workspace execution cannot block/flood. Frame fixture stdout after a fixed test-harness prefix in the test process factory only; production still validates raw TS. no shell, Unix signals or `/proc` assumptions. Cover exit zero/nonzero while live, stdin refusal, blocked writes, absent/partial/malformed stdout, stderr flood, pump panic/error and cancellation in every pending I/O state. Assertions: peak charged bytes `<= 1_048_576`, read `<= 32_768`, framing `<188`, stderr `<=8192`, supervisor/pumps `<=4`, no task growth by chunk count; all children reaped/pumps joined after stop/cancel. Across two managers fifth transform fails immediately, native consumes zero slots, slot not reusable before wait/reap. Large backing allocation plus tiny slice remains fully charged or copied. With controlled Tokio time: pending input arms stall once; more input/blocked write does not reset it; 44 seconds is healthy, 45 fails; partial packets do not count, drained idle remains healthy. Startup uses earlier absolute deadline.
- [ ] **Step 2 (Luna):** Run `cargo test -p ace-engine --test hls_audio_faults`; require missing API or behavior assertion failure, not missing fixture/path.
- [ ] **Step 3 (Sol):** Implement fixed process boundary and fault seams. Arguments include `-nostdin`, `-protocol_whitelist pipe`, `-f mpegts -probesize 1048576 -analyzeduration 1000000 -i pipe:0`, explicit `-map 0:i:<validated video PID>`/audio PID, `-c:v copy -c:a aac -profile:a aac_low -ac 2 -ar 48000 -b:a 128k -sn -dn`, one program/service/transport identity, `-streamid 0:256 -streamid 1:257 -mpegts_pmt_start_pid 4096`, repeated headers and `pipe:1` MPEG-TS. No async stretch/frame duplication; use source timestamp preservation (`-copyts`, no start-at-zero rebasing within generation) and validate effective offsets in Task 6. Source PCR cannot configure output packager. Output validates fresh single PAT/PMT, source program/transport IDs, video type/PID256/PCR256, AAC PID257, no extras/descriptors copied onto encoded audio; require fresh access point by deadline. Reads feed directly, no stdout queue. Stall tracks valid complete TS packets and outstanding pending/accepted input. Hold global permit through child wait; fence publication, cancel writer/close stdin, grace one second, kill/wait, cancel/join or abort-and-join pumps on **every** return/panic/cancel. Only cleanup tracker owns cleanup if caller disappears; kill_on_drop is last resort.
- [ ] **Step 4 (Luna):** Run fault command; require PASS offline on supported platforms and counters returning to zero. A platform-specific assertion must be replaced with portable fixture instrumentation, not silently skipped.
- [ ] **Step 5 (Sol):** Review and commit as `feat(ace-engine): supervise bounded HLS normalization children`.

### Task 4: Enabled classifier, recovery and startup preflight

**Files:** Modify/test `ace-engine/src/{hls_audio.rs,hls_audio/process.rs,config.rs,runtime.rs,manager.rs}`; test policy/preflight beside modules and extend fault suite.

**Interfaces:**
- Consumes Tasks 1–3; add cloned `HlsAudioConfig { normalize: bool, ffmpeg_path: PathBuf }` to `Config` as `hls_audio` without changing Copy `HlsConfig`.
- Produces `async fn preflight(config: &HlsAudioConfig) -> Result<Option<PreparedNormalizer>,HlsPreflightError>`; disabled returns None before any lookup. Error is concise category/capability only.
- `StreamManager::with_normalizer(registry: ProviderRegistry, buffer: usize, hls: HlsConfig, normalizer: Option<PreparedNormalizer>) -> Arc<StreamManager>`; existing constructors delegate with None.
- `spawn_worker(session: &StreamSession, media: Arc<HlsPackager>, config: HlsConfig, prepared: Option<PreparedNormalizer>, tracker: &CleanupTracker) -> WorkerControl`. Classifier consumes original broadcast packets; native filtering and transformed output remain separate; both use the same media entry.
- `RecoveryReason::{Source, Lag, Layout, Transport}` and `RecoveryWindow::invalidate(&mut self, now: tokio::time::Instant, reason: RecoveryReason) -> Result<GenerationId,HlsFailure>` define the three/60-second gate. Paired-marker token belongs to Source only, expires/consumes at accepted opening access point even on filtered PID.

- [ ] **Step 1 (Luna): Write failing `hls_audio_classification_*`, `hls_audio_preflight_*`, `hls_audio_recovery_*` tests.** Assert default false, exact boolean acceptance/rejection, enabled empty path rejection, invalid disabled path causes zero lookup/probe/admission. Classifier retains at most aggregate budget, requires fresh matching tables/access point, discards preceding payload, prepends fresh tables; budget hit before opening fails rather than slides. ADTS/video-only starts zero children. Malformed tables preserve valid layout; current unsupported fails terminally. Recovery tests cover lag, source/paired marker (including filtered PID), marker after first opening becoming independent, semantic layout change versus version repeat, old stdout token rejection, no cached source/output clock/table reuse; three invalidations accepted, fourth in 60 seconds terminal, outside window admitted. Child opening continuity reset causes no extra recovery. One startup clock covers all stages/new startup segments. Preflight missing executable/capability, stdout 256 KiB overflow, five-second probe/ten-second aggregate boundaries, failed/malformed smoke and cancellation all reap before return; probes sequential, resolved PATH binary retained despite later PATH change.
- [ ] **Step 2 (Luna):** Run `cargo test -p ace-engine hls_audio_` and fault suite; require expected red tests.
- [ ] **Step 3 (Sol):** Implement classifier/recovery/config wiring and preflight before any listener/session acceptance in `build_runtime`. Inspect demuxer/muxer/native AAC/MPEG Layer II/III/AC3/pipe availability, then pipe a small generated-in-memory synthetic AC3 TS vector through production command/output validation (no lavfi/ffprobe/network in production). Bound capture/deadlines and use identical awaited cleanup. Sequential probes share one absolute deadline. In enabled worker classify, then native or child; recovery fences old publication and fully reaps before reclassification/admission, clearing all old queued state. Layout-change packet may seed only after fencing. Preserve source-relative timing and service-name SDT against validated output service identity; suppress competing SDT. Any child/pump/output/classification/admission failure is terminal; HTTP retries never restart it. No slow operations under manager/media locks.
- [ ] **Step 4 (Luna):** Run focused command, fault suite and `cargo test -p ace-engine runtime::tests`; require PASS with no executable dependency for ordinary offline cases.
- [ ] **Step 5 (Sol):** Review and commit as `feat(ace-engine): classify and recover normalized HLS generations`.

### Task 5: Fail-closed HTTP using atomic playlist snapshots

**Files:** Modify `ace-engine/src/http.rs`, `ace-engine/src/http/{streams.rs,compat.rs}`; test `ace-engine/src/http/tests/compat_hls.rs` and existing native route tests; extend manager fixtures only as needed.

**Interfaces:**
- Consumes Task 2 `wait_available`/`playlist_snapshot`, Task 4 shared worker and existing native demand/compat pin lifetimes.
- Replace readiness-then-render helper with `async fn await_hls_playlist(pkg: &HlsPackager, timeout: Duration, prefix: &str, native_access: bool) -> Result<String,Box<Response>>`; snapshot unavailable after a wake resumes waiting only within original request/generation deadline. Compatibility caller continues selecting cancellation/expiry and revalidates authorization/removal before returning any media/unavailability response.

- [ ] **Step 1 (Luna): Write failing `normalization_http_*` route tests.** Starting/Recovering/Failed/Stopped => `503`, `Retry-After: 1`, no private body; terminal waiters wake within one scheduler second. Race Ready/recovery cannot return old Ready playlist. Expired/invalid token, cancellation, removed entry => existing 404 even when failure arrives simultaneously. Retained segment sequence remains retrievable during Recovering/Failed until eviction/teardown; missing segments preserve existing status. Native/compat same key share segment bytes and worker; retries after failure do not increment starts. Disabled route responses/readiness and VOD/raw bytes equal controls.
- [ ] **Step 2 (Luna):** Run `cargo test -p ace-engine normalization_http_`; require behavior/API failure.
- [ ] **Step 3 (Sol):** Migrate live playlist consumers to atomic snapshots with original bounded request deadline; preserve content type/cache headers, access recording, compatibility cancellation/expiry precedence and segment authorization. Do not modify VOD packaging/routes or raw session broadcast.
- [ ] **Step 4 (Luna):** Run focused command and `cargo test -p ace-engine http::tests`; require PASS including existing compatibility lifetime/authorization regressions.
- [ ] **Step 5 (Sol):** Review and commit as `fix(ace-engine): gate live playlists on current HLS generation`.

### Task 6: Synthetic codec acceptance, mandatory CI prerequisites and operator guidance

**Files:** Create `ace-engine/tests/hls_audio_ffmpeg.rs`, extend `ace-engine/tests/support/hls_audio.rs`; modify `.github/workflows/ci.yml`, `README.md`; product fixes, if evidence requires them, remain in owning task files and receive Sol review rather than test weakening.

**Interfaces:**
- Consumes full production manager/routes/preflight plus test counters and synthetic source provider. Test opt-in `OUTPACE_TEST_HLS_AUDIO_FFMPEG=1`; missing opt-in prints explicit local skip; enabled suite fails on any missing prerequisite.
- `tests/support/hls_audio.rs`: `require_synthetic_tools() -> Result<SyntheticTools,String>`, `generate_case(tools: &SyntheticTools, case: AudioCase) -> Result<SyntheticCase,String>` with test-only `AudioCase::{Ac3,MpegLayer2,MpegLayer3,AacAndAc3,VideoOnly}`; synthetic buffers use no registry/external media. Tools explicitly verify each generator plus ffprobe before claims are run.

- [ ] **Step 1 (Luna): Write failing real-media assertions and prerequisite checks.** Ten-second generated video+sine for AC3 and MPEG Layers II/III: exactly one copied video and AAC-LC stereo/48000 Hz audio, source program/transport ID, PMT4096/video256/audio257/PCR256, fresh PSI/access points, advancing PTS/PCR, decoded audio nonzero; demuxed video elementary payload equals source excluding mux framing. Duration error `<=1024/48000 seconds + measured source frame duration`, A/V end drift `<=0.100 seconds`. Assert AAC+AC3 selects AAC and video-only launches no child. Feed synthetic sources through manager/native/compat routes: configured new-generation startup count, advancing playlist, retained byte identity, raw byte identity versus disabled, unchanged VOD control. Recover/audio-switch => fresh opening, one boundary, old retained retrieval, no stale output. Verify metadata SDT service identity and no competing SDT without publishing titles in logs.
- [ ] **Step 2 (Luna):** Run `OUTPACE_TEST_HLS_AUDIO_FFMPEG=1 cargo test -p ace-engine --test hls_audio_ffmpeg -- --nocapture`; record a meaningful red assertion or hard prerequisite failure. Do not claim layer coverage from stream-type-only tests.
- [ ] **Step 3 (Sol):** Add designated `HLS audio synthetic` job on existing Ubuntu CI alongside offline job; preserve required existing job names. Install and verify FFmpeg/ffprobe and its existing AC3, MP2, MP3 and AAC encoders plus synthetic video/sine generators. The user-approved 2026-10-21 scope amendment excludes Layer I conversion. Remove the MP1 preflight requirement and reject selected Layer I using bounded actual elementary-stream frame-header evidence before child admission, with continued validation after opening. Luna covers split/incomplete/invalid headers and Layer I rejection with synthetic header fixtures; no Layer I encoder is required. Keep runtime implementation independent of test generators. README documents exactly two options/defaults, strict enabled restrictions, stereo loss/copy-video compatibility limits, terminal failure/retry meaning, four-process bound without RSS/CPU promise, operator-built container binary and awaited shutdown expectations; no Docker/compose package changes.
- [ ] **Step 4 (Luna):** Run enabled synthetic suite with verified tool inventory, then `cargo test --workspace --locked`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo fmt --all --check`, `python3 tools/hygiene/check_identifiers.py`. Require PASS and codec-by-codec evidence; no real media/live/browser matrix. Local disabled suite may skip only with explicit notice; designated CI must not skip. Report unsupported generator prerequisites as failure, not passing acceptance.
- [ ] **Step 5 (Sol):** Fresh whole-branch review against spec and recorded evidence, then commit named acceptance/docs/CI files as `test(ace-engine): verify synthetic HLS audio normalization`. Root handles publishing separately; do not push or open a PR merely from this plan.

## Self-review and handoff

Spec coverage: Task 1 owns strict policy/semantic identity; Task 2 owns lifecycle/cancellation/atomic generation state; Task 3 owns bounds/admission/stalls/process cleanup/output validation; Task 4 owns preflight/default-off/recovery/dedup/metadata; Task 5 owns HTTP precedence and retained segments; Task 6 owns measurable codec/timing acceptance, raw/VOD controls and deployment/CI documentation. Review Focus cases have explicit tests in their owning tasks. Interface names/types are shared consistently; test/product ownership and red/green commands are explicit. No product edits are authorized by this document.

Approved scope amendment (2026-10-21): the user explicitly narrowed this release to MPEG Layers II/III and AC3 conversion. Layer I is unsupported and must fail closed based on actual selected MPEG elementary-stream headers, since PMT types cannot distinguish layers. This supersedes the original Layer I generator prerequisite. Tests cover rejection without a full encoder; remaining codec, timing, lifecycle, bounds and route acceptance remain unchanged. No additional approval round is required.

Root must link this plan and obtain human confirmation before product/test implementation. Preserve the already chosen subagent method and model assignments; do not ask the user to choose them again.
