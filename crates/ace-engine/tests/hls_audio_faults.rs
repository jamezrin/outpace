//! Fault-oriented coverage for the bounded HLS normalization child boundary.
#![allow(clippy::duplicate_mod)]

#[path = "support/hls_audio.rs"]
mod support;

// Compile the exact production pump/validator source with integration-test cfg so its
// private test seam is available without adding production configuration or API.
mod hls {
    pub use ace_engine::hls::*;
}
pub use ace_engine::{config, provider, session};
#[path = "../src/hls_audio/process.rs"]
#[allow(dead_code)]
mod process;

// Deliberately name the planned API before its implementation exists. This ensures
// the expected-red run fails at the missing feature boundary, not at a fixture path.
use ace_engine::hls::HlsFailure;
use ace_engine::hls_audio::{ChildAdmission, InputBudget};
use process::test_support;
use std::sync::Arc;
use tokio::sync::watch;

#[tokio::test]
async fn output_between_successful_write_and_accounting_retires_drained_input() {
    let (deadline, writes, output_events, charged) =
        test_support::output_between_write_and_accounting().await;
    assert_eq!(writes, 1, "one budgeted input item was fully written");
    assert_eq!(
        output_events, 1,
        "the injected output event reached the race window"
    );
    assert_eq!(charged, 0, "cleanup releases the input allocation budget");
    assert_eq!(
        deadline, None,
        "fully drained input with output remains idle"
    );
}

#[test]
fn fake_child_entry() {
    let Some(mode) = std::env::var_os("OUTPACE_TEST_HLS_AUDIO_CHILD") else {
        return;
    };
    if let Some(mode) = mode.to_str().and_then(support::child::ChildMode::parse) {
        support::child::run(mode);
    }
}

#[tokio::test]
async fn input_budget_charges_the_owned_backing_allocation() {
    let budget = InputBudget::new();
    let (cancel_tx, mut cancel_rx) = watch::channel(false);
    let owned = vec![0x47; 1_048_577];
    let result = budget.acquire_owned(owned, &mut cancel_rx).await;
    drop(cancel_tx);
    assert!(matches!(result, Err(HlsFailure::InputOverflow)));
    assert_eq!(budget.charged(), 0);
}

#[tokio::test]
async fn input_budget_copies_a_small_view_off_a_large_backing_allocation() {
    let budget = InputBudget::new();
    let (cancel_tx, mut cancel_rx) = watch::channel(false);
    let mut large_backing = Vec::with_capacity(2 * 1_048_576);
    large_backing.extend_from_slice(&support::transport_prefix()[..188]);
    let input = budget
        .acquire_copy(&large_backing[..188], &mut cancel_rx)
        .await
        .expect("small packet should fit after bounded ownership transfer");
    assert!(budget.charged() <= 1_048_576);
    drop(input);
    assert_eq!(budget.charged(), 0);
    drop(cancel_tx);
}

#[tokio::test]
async fn input_budget_rejects_tiny_owned_vec_with_huge_capacity_and_charges_copy() {
    let budget = InputBudget::new();
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let mut huge = Vec::with_capacity(1_048_577);
    huge.extend_from_slice(&[0x47; 188]);
    assert!(matches!(
        budget.acquire_owned(huge, &mut cancel_rx).await,
        Err(HlsFailure::InputOverflow)
    ));
    assert_eq!(budget.charged(), 0);

    let spare = vec![0x47; 188 * 2];
    let item = budget
        .acquire_copy(&spare[..188], &mut cancel_rx)
        .await
        .unwrap();
    assert_eq!(item.charged(), 188);
    assert_eq!(budget.charged(), 188);
    drop(item);
    assert_eq!(budget.charged(), 0);
}

#[tokio::test]
async fn full_input_budget_wait_is_cancellable_and_active_vec_stays_charged() {
    let budget = InputBudget::new();
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let held = budget
        .acquire_copy(&vec![0; 1_048_576], &mut cancel_rx)
        .await
        .unwrap();
    assert_eq!(budget.charged(), 1_048_576);
    let (cancel_tx, mut waiting_cancel) = watch::channel(false);
    let waiting_budget = budget.clone();
    let waiter =
        tokio::spawn(async move { waiting_budget.acquire_copy(&[1], &mut waiting_cancel).await });
    tokio::task::yield_now().await;
    cancel_tx.send_replace(true);
    assert!(matches!(waiter.await.unwrap(), Err(HlsFailure::Pipe)));
    assert_eq!(
        budget.charged(),
        1_048_576,
        "active Vec storage remains charged"
    );
    drop(held);
    assert_eq!(budget.charged(), 0);
}

#[tokio::test]
async fn admission_is_nonblocking_and_released_only_after_child_reap() {
    let _serial = test_support::process_test_lock().await;
    let admission = ChildAdmission::global();
    let mut permits = Vec::new();
    for _ in 0..4 {
        permits.push(admission.try_acquire().expect("first four slots admitted"));
    }
    assert!(matches!(
        admission.try_acquire(),
        Err(HlsFailure::Admission)
    ));
    drop(permits.pop());
    assert!(admission.try_acquire().is_ok());
}

#[tokio::test]
async fn four_supervisors_are_the_global_bound_and_slot_reuse_waits_for_reap() {
    let (immediate, during_grace, after_reap, reaped) =
        test_support::supervisors_hold_four_global_slots_until_reap().await;
    assert_eq!(immediate, 1, "fifth supervisor fails admission immediately");
    assert_eq!(
        during_grace, 1,
        "slots remain unavailable during one-second grace"
    );
    assert_eq!(after_reap, 1, "a slot is reusable after all waits complete");
    assert_eq!(reaped, 4);
}

#[test]
fn command_uses_fixed_numeric_maps_copy_and_pipe_arguments() {
    let args = test_support::command_args();
    let joined = args.join(" ");
    for required in [
        "-copyts",
        "-nostdin",
        "-protocol_whitelist pipe",
        "-probesize 1048576",
        "-analyzeduration 1000000",
        "0:i:512",
        "0:i:513",
        "-c:v copy",
        "-c:a aac",
        "-profile:a aac_low",
        "-ac 2",
        "-ar 48000",
        "-streamid 0:256",
        "-streamid 1:257",
        "-mpegts_pmt_start_pid 4096",
        "-mpegts_flags +resend_headers",
        "-f mpegts",
        "-avoid_negative_ts disabled",
        "-b:a 128k",
        "-sn",
        "-dn",
        "pipe:0",
        "pipe:1",
    ] {
        assert!(
            joined.contains(required),
            "missing argument sequence {required:?}: {args:?}"
        );
    }
    assert!(!args
        .iter()
        .any(|arg| arg.contains(";") || arg.contains("|")));
    assert!(joined.contains("-map_metadata -1"));
    assert!(joined.contains("-map_chapters -1"));
    assert!(joined.contains("-mpegts_service_id 9"));
    assert!(joined.contains("-mpegts_transport_stream_id 7"));
    assert!(joined.contains("-mpegts_copyts 1"));
}

#[test]
fn output_validator_rejects_extra_audio_and_copied_descriptors() {
    let good = support::transport_prefix();
    let pmt = &good[188..376];
    let section = &pmt[5..5 + 26];

    // Add a third elementary stream and recompute section length/CRC.
    let mut extra_stream = section[..section.len() - 4].to_vec();
    extra_stream.extend_from_slice(&[0x06, 0xe1, 0x02, 0xf0, 0]);
    let with_extra = support::pmt_packet_with_section(extra_stream);
    let mut fixture = good.clone();
    fixture[188..376].copy_from_slice(&with_extra);
    assert_eq!(
        test_support::validate_output(&fixture, &[188]),
        Err(HlsFailure::OutputInvalid)
    );

    // An AAC stream descriptor is forbidden even when its elementary PID/type
    // otherwise match the required output mapping.
    let mut described = section[..section.len() - 4].to_vec();
    // Keep program_number (9) and the empty program descriptor loop unchanged.
    // The AAC ES entry starts after the five-byte video entry at offset 17;
    // its descriptor loop length is at offsets 20..22.
    assert_eq!(&described[3..5], &[0, 9]);
    assert_eq!(&described[10..12], &[0xf0, 0]);
    described[20] = 0xf0;
    described[21] = 3;
    described.splice(22..22, [0x0a, 0x01, b'e']);
    fixture = good;
    fixture[188..376].copy_from_slice(&support::pmt_packet_with_section(described));
    assert_eq!(
        test_support::validate_output(&fixture, &[188]),
        Err(HlsFailure::OutputInvalid)
    );
}

#[test]
fn output_validator_rejects_extra_pat_program_and_bad_section_state() {
    let good = support::transport_prefix();
    let pat = &good[..188];
    let section = &pat[5..5 + 16];
    let mut extra = section[..section.len() - 4].to_vec();
    extra[2] += 4;
    extra.extend_from_slice(&[0, 10, 0xe1, 3]);
    let mut fixture = good.clone();
    fixture[..188].copy_from_slice(&support::pat_packet_with_section(extra));
    assert_eq!(
        test_support::validate_output(&fixture, &[188]),
        Err(HlsFailure::OutputInvalid)
    );

    for (index, value) in [(10, 0xc0), (11, 1)] {
        let mut changed = good.clone();
        changed[index] = value;
        let packet: &mut [u8; 188] = (&mut changed[..188]).try_into().unwrap();
        support::set_psi_crc(packet);
        assert_eq!(
            test_support::validate_output(&changed, &[188]),
            Err(HlsFailure::OutputInvalid)
        );
    }
    for (index, value) in [(10, 0xc0), (11, 1)] {
        let mut changed = good.clone();
        changed[188 + index] = value;
        let packet: &mut [u8; 188] = (&mut changed[188..376]).try_into().unwrap();
        support::set_psi_crc(packet);
        assert_eq!(
            test_support::validate_output(&changed, &[188]),
            Err(HlsFailure::OutputInvalid)
        );
    }
}

#[test]
fn output_validator_accepts_repeated_valid_program_tables() {
    let prefix = support::transport_prefix();
    let mut repeated = prefix[..376].to_vec();
    repeated.extend_from_slice(&prefix[..376]);
    repeated.extend_from_slice(&prefix[376..]);
    let (packets, fed) = test_support::validate_output(&repeated, &[188]).unwrap();
    assert_eq!(packets, 5);
    assert_eq!(
        fed, 3,
        "publication starts with the original valid PAT/PMT pair and access point"
    );
}

#[tokio::test]
async fn invalid_source_fails_before_executable_lookup_or_child_admission() {
    let (result, status) = test_support::invalid_source_preflight().await;
    assert_eq!(result, Err(HlsFailure::Unsupported));
    assert!(matches!(
        status,
        ace_engine::hls::HlsStatus::Failed(HlsFailure::Unsupported)
    ));
}

#[test]
fn synthetic_crc_psi_fixture_opens_only_after_fresh_access_point_at_chunk_boundaries() {
    let fixture = support::transport_prefix();
    assert!(
        test_support::validate_output(&fixture[..188], &[188]).is_ok(),
        "PAT fixture"
    );
    assert!(
        test_support::validate_output(&fixture[..376], &[188]).is_ok(),
        "PAT/PMT fixture"
    );
    for chunk in [1, 187, 188, 32_768] {
        let (packets, fed) =
            test_support::validate_output(&fixture, &[chunk]).expect("valid fixture");
        assert_eq!(packets, 3);
        assert_eq!(
            fed, 3,
            "tables and fresh access point are fed at chunk {chunk}"
        );
    }
    let (packets, fed) =
        test_support::validate_output(&fixture[..376], &[188]).expect("tables alone valid");
    assert_eq!(
        (packets, fed),
        (2, 0),
        "no publication before a fresh access point"
    );
}

#[test]
fn output_validator_rejects_recomputed_semantic_mutations_and_wire_corruption() {
    let good = support::transport_prefix();
    let accepted = |bytes: &[u8]| test_support::validate_output(bytes, &[188]).is_ok();
    assert!(accepted(&good));

    // Semantic table mutations have a valid CRC, so rejection is for identity/policy.
    for (offset, value) in [(4, 8), (9, 1), (14, 1), (12, 0x24)] {
        let mut changed = good.clone();
        changed[188 + 5 + offset] = value;
        let pmt: &mut [u8; 188] = (&mut changed[188..376]).try_into().unwrap();
        support::set_psi_crc(pmt);
        assert!(
            !accepted(&changed),
            "reject changed PMT semantic field at {offset}"
        );
    }
    for (offset, value) in [(4, 8), (9, 8)] {
        let mut changed = good.clone();
        changed[5 + offset] = value;
        let pat: &mut [u8; 188] = (&mut changed[..188]).try_into().unwrap();
        support::set_psi_crc(pat);
        assert!(
            !accepted(&changed),
            "reject changed PAT identity at {offset}"
        );
    }

    let mut bad_crc = good.clone();
    bad_crc[188 + 20] ^= 1;
    assert!(!accepted(&bad_crc), "bad CRC rejected");
    let mut malformed = good.clone();
    malformed[188 + 5 + 1] = 0xff;
    assert!(!accepted(&malformed), "malformed PSI length rejected");
    let mut scrambled = good.clone();
    scrambled[2 * 188 + 3] |= 0x80;
    assert!(!accepted(&scrambled), "scrambled packet rejected");
}

#[test]
fn output_validator_rejects_pcr_carried_on_nonvideo_pid() {
    let mut fixture = support::transport_prefix();
    fixture.extend_from_slice(&support::pcr_packet(257, 90_000));
    assert_eq!(
        test_support::validate_output(&fixture, &[188]),
        Err(HlsFailure::OutputInvalid),
        "output PCR must be carried on the fixed video PID 256",
    );
}

#[test]
fn output_validator_rejects_pcr_flag_with_short_adaptation_field() {
    let mut fixture = support::transport_prefix();
    fixture.extend_from_slice(&support::malformed_short_pcr_packet(256));
    assert_eq!(
        test_support::validate_output(&fixture, &[188]),
        Err(HlsFailure::OutputInvalid),
        "PCR_flag requires at least seven adaptation bytes",
    );
}

#[tokio::test]
async fn child_fault_matrix_observes_bounded_pumps_and_complete_cleanup() {
    let cases = [
        // EOF from a pump can win the supervisor select against child.wait; the
        // validated prefix and reap counters below prove these are child exits.
        (test_support::ChildMode::ExitZero, HlsFailure::Pipe),
        (test_support::ChildMode::ExitFailure, HlsFailure::Pipe),
        (test_support::ChildMode::RefuseStdin, HlsFailure::Pipe),
        (test_support::ChildMode::BlockStdin, HlsFailure::Deadline),
        (test_support::ChildMode::NoStdout, HlsFailure::Deadline),
        (
            test_support::ChildMode::PartialStdout,
            HlsFailure::OutputInvalid,
        ),
        (
            test_support::ChildMode::MalformedStdout,
            HlsFailure::OutputInvalid,
        ),
        (test_support::ChildMode::StderrFlood, HlsFailure::ChildExit),
        (
            test_support::ChildMode::EchoValidTs,
            HlsFailure::OutputInvalid,
        ),
    ];
    for (mode, expected) in cases {
        let observed = Arc::new(test_support::Counters::default());
        let result = test_support::run_fault_case(mode, observed.clone()).await;
        let expected_result = match (mode, result) {
            (
                test_support::ChildMode::ExitZero
                | test_support::ChildMode::ExitFailure
                | test_support::ChildMode::RefuseStdin
                | test_support::ChildMode::StderrFlood,
                // These children emitted their intended payload and then exited;
                // stdout EOF and child.wait may win in either order.
                Err(HlsFailure::Pipe | HlsFailure::ChildExit),
            ) => true,
            (
                test_support::ChildMode::PartialStdout
                | test_support::ChildMode::MalformedStdout
                | test_support::ChildMode::EchoValidTs,
                Err(HlsFailure::Pipe | HlsFailure::ChildExit),
            ) => true,
            (_, Err(actual)) => actual == expected,
            _ => false,
        };
        assert!(
            expected_result,
            "mode {mode:?} expected {:?}, got {result:?}",
            expected
        );
        let counters = observed.snapshot();
        assert_eq!(counters.children_started, 1, "child started for {mode:?}");
        assert_eq!(counters.children_reaped, 1, "child reaped for {mode:?}");
        assert_eq!(
            counters.pumps_joined, counters.pumps_started,
            "pumps joined for {mode:?}"
        );
        assert_eq!(counters.pumps_started, 3, "three fixed pumps for {mode:?}");
        assert_eq!(
            counters.harness_prefix_seen, 1,
            "libtest prefix was stripped for {mode:?}: {counters:?}"
        );
        assert!(counters.max_input_bytes <= 1_048_576);
        assert!(
            counters.max_input_bytes > 0,
            "aggregate input charge was observed for {mode:?}"
        );
        assert!(counters.max_stdout_read <= 32_768);
        assert!(counters.max_stderr_tail <= 8_192);
        assert!(counters.max_ts_framing_stage < 188);
        assert!(counters.max_task_count <= 4);
        if mode == test_support::ChildMode::StderrFlood {
            assert!(
                counters.stderr_bytes_read >= 8_192,
                "stderr flood reached the drain"
            );
        }
        if mode == test_support::ChildMode::NoStdout {
            assert!(counters.stdout_read_waits > 0, "stdout read was pending");
        }
        if mode == test_support::ChildMode::PartialStdout {
            assert!(
                counters.stdout_bytes_read >= 187,
                "partial payload was observed"
            );
        }
        if mode == test_support::ChildMode::MalformedStdout {
            assert!(
                counters.stdout_bytes_read >= 188,
                "malformed packet was observed"
            );
        }
        if mode == test_support::ChildMode::EchoValidTs {
            assert!(
                counters.output_packets_received > 0,
                "echo output reached packet boundary"
            );
        }
    }
}

#[tokio::test]
async fn pump_errors_and_panics_are_terminal_and_still_join_every_pump() {
    for fault in [
        test_support::PumpFault::StdinError,
        test_support::PumpFault::StdoutError,
        test_support::PumpFault::StderrError,
        test_support::PumpFault::StdinPanic,
        test_support::PumpFault::StdoutPanic,
        test_support::PumpFault::StderrPanic,
    ] {
        let counters = Arc::new(test_support::Counters::default());
        let result = test_support::run_with_pump_fault(fault, counters.clone()).await;
        let expected = match fault {
            test_support::PumpFault::StdinError
            | test_support::PumpFault::StdoutError
            | test_support::PumpFault::StderrError => HlsFailure::Pipe,
            test_support::PumpFault::StdinPanic
            | test_support::PumpFault::StdoutPanic
            | test_support::PumpFault::StderrPanic => HlsFailure::Pump,
        };
        assert_eq!(
            result,
            Err(expected),
            "injected fault {fault:?} reached its intended pump"
        );
        let snapshot = counters.snapshot();
        assert_eq!(snapshot.children_reaped, snapshot.children_started);
        assert_eq!(snapshot.pumps_joined, snapshot.pumps_started);
    }
}

#[tokio::test]
async fn panic_inside_output_publication_guard_still_reaps_and_joins() {
    let counters = Arc::new(test_support::Counters::default());
    let result = test_support::output_guard_panic_cleanup(counters.clone()).await;
    assert_eq!(result, Err(HlsFailure::Pump));
    let snapshot = counters.snapshot();
    assert_eq!(
        snapshot.output_packets_received, 1,
        "complete synthetic TS packet reached the guarded feed"
    );
    assert_eq!(snapshot.children_reaped, snapshot.children_started);
    assert_eq!(snapshot.pumps_joined, snapshot.pumps_started);
}

#[tokio::test]
async fn cancellation_cleans_up_during_every_pending_pipe_state() {
    for state in [
        test_support::PendingIo::StdinWrite,
        test_support::PendingIo::StdoutRead,
        test_support::PendingIo::StderrRead,
        test_support::PendingIo::InputReceive,
    ] {
        let counters = Arc::new(test_support::Counters::default());
        test_support::cancel_while_pending(state, counters.clone()).await;
        let snapshot = counters.snapshot();
        assert_eq!(snapshot.children_reaped, snapshot.children_started);
        assert_eq!(snapshot.pumps_joined, snapshot.pumps_started);
    }
}

#[tokio::test]
async fn blocked_input_channel_send_is_cancelled_and_joined() {
    test_support::bounded_channel_send_cancels_and_joins().await;
}

#[tokio::test(start_paused = true)]
async fn input_does_not_extend_the_output_stall_deadline() {
    let result =
        test_support::stall_after_first_pending_input(std::time::Duration::from_secs(25)).await;
    assert!(matches!(result, Err(HlsFailure::OutputStall)));
}

#[tokio::test(start_paused = true)]
async fn output_stall_is_healthy_until_the_full_deadline() {
    test_support::stall_boundary_is_healthy_at(std::time::Duration::from_secs(44)).await;
}

#[tokio::test(start_paused = true)]
async fn stall_clock_tracks_acceptance_output_and_draining_transitions() {
    test_support::stall_clock_transitions().await;
}

#[tokio::test]
async fn partial_stdout_does_not_refresh_output_stall_clock() {
    test_support::partial_stdout_does_not_count_as_output().await;
}

#[tokio::test]
async fn real_supervisor_ready_with_partial_stdout_stalls_at_45_seconds() {
    let snapshot = test_support::supervisor_ready_stall_evidence().await;
    assert_eq!(snapshot.max_input_bytes, 1_048_576);
    assert_eq!(snapshot.pumps_started, 3);
    assert_eq!(snapshot.pumps_joined, 3);
    assert_eq!(snapshot.children_started, 1);
    assert_eq!(snapshot.children_reaped, 1);
    assert_eq!(snapshot.kill_requested, 1);
}

#[tokio::test]
async fn real_supervisor_drained_idle_ready_remains_healthy_for_90_seconds() {
    let snapshot = test_support::supervisor_drained_idle_evidence().await;
    assert_eq!(snapshot.writes_started, 1);
    assert_eq!(snapshot.writes_completed, 1);
    assert_eq!(snapshot.pumps_started, 3);
    assert_eq!(snapshot.pumps_joined, 3);
    assert_eq!(snapshot.children_started, 1);
    assert_eq!(snapshot.children_reaped, 1);
}

#[tokio::test]
async fn real_supervisor_handles_many_individually_budgeted_input_chunks() {
    let (snapshot, charged) = test_support::many_chunk_echo_evidence().await;
    assert_eq!(snapshot.writes_completed, 1_025);
    assert!(snapshot.valid_packets >= 14 + 1_024);
    assert_eq!(snapshot.pumps_started, 3);
    assert_eq!(snapshot.pumps_joined, 3);
    assert_eq!(snapshot.max_task_count, 4);
    assert_eq!(snapshot.active_tasks, 0);
    assert_eq!(snapshot.children_started, 1);
    assert_eq!(snapshot.children_reaped, 1);
    assert!(snapshot.max_input_bytes > 0);
    assert!(snapshot.max_input_bytes <= 1_048_576);
    assert_eq!(charged, 0, "all input permits released after cancellation");
}

#[tokio::test]
async fn delayed_child_invocation_keeps_the_original_startup_deadline() {
    let (status, unreaped_at_failure, result, snapshot) =
        test_support::delayed_startup_deadline_evidence().await;
    assert_eq!(
        status,
        ace_engine::hls::HlsStatus::Failed(HlsFailure::Deadline)
    );
    assert!(
        unreaped_at_failure,
        "media failed at startup deadline while child cleanup was still pending"
    );
    assert_eq!(result, Err(HlsFailure::Deadline));
    assert_eq!(snapshot.children_started, 1);
    assert_eq!(snapshot.children_reaped, 1);
    assert_eq!(snapshot.pumps_started, 3);
    assert_eq!(snapshot.pumps_joined, 3);
    assert_eq!(snapshot.active_tasks, 0);
}

#[tokio::test(start_paused = true)]
async fn drained_idle_input_does_not_trigger_the_output_stall_deadline() {
    test_support::drained_idle_source_remains_healthy(std::time::Duration::from_secs(90)).await;
}
