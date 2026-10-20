//! Portable fake-child entry point for the HLS audio process tests.
//!
//! The integration test launches its own test executable with `--exact
//! fake_child_entry --nocapture` and `OUTPACE_TEST_HLS_AUDIO_CHILD=<mode>`. This
//! avoids shell scripts, platform-specific signals, and helper binaries.

use std::io::{self, Read, Write};

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

impl ChildMode {
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "exit-zero" => Self::ExitZero,
            "exit-failure" => Self::ExitFailure,
            "refuse-stdin" => Self::RefuseStdin,
            "block-stdin" => Self::BlockStdin,
            "no-stdout" => Self::NoStdout,
            "partial-stdout" => Self::PartialStdout,
            "malformed-stdout" => Self::MalformedStdout,
            "stderr-flood" => Self::StderrFlood,
            "echo-valid-ts" => Self::EchoValidTs,
            "ready-then-stall" => Self::ReadyThenStall,
            "echo-ready-idle" => Self::EchoReadyIdle,
            _ => return None,
        })
    }
}

/// Run a fake process mode. Called only by `fake_child_entry` when the explicit
/// environment switch is present; ordinary test execution returns immediately.
pub fn run(mode: ChildMode) {
    match mode {
        ChildMode::ExitZero => {}
        ChildMode::ExitFailure => exit_after_prefix(23),
        ChildMode::RefuseStdin => exit_after_prefix(0),
        ChildMode::BlockStdin => {
            // Keep both pipe ends open so the supervisor must cancel and reap a
            // genuinely pending writer/read instead of observing an early EOF.
            std::thread::sleep(std::time::Duration::from_secs(30));
        }
        ChildMode::NoStdout => {
            let mut sink = io::sink();
            let _ = io::copy(&mut io::stdin(), &mut sink);
        }
        ChildMode::PartialStdout => {
            let _ = io::stdout().write_all(&[0x47; 187]);
            let _ = io::stdout().flush();
            std::thread::sleep(std::time::Duration::from_millis(30));
            exit_after_prefix(0);
        }
        ChildMode::MalformedStdout => {
            let _ = io::stdout().write_all(&[0; 188]);
            let _ = io::stdout().flush();
            exit_after_prefix(0);
        }
        ChildMode::StderrFlood => {
            let mut stderr = io::stderr().lock();
            let block = [b'x'; 4096];
            for _ in 0..256 {
                if stderr.write_all(&block).is_err() {
                    break;
                }
                let _ = stderr.flush();
            }
            std::thread::sleep(std::time::Duration::from_millis(30));
        }
        ChildMode::EchoValidTs => {
            let mut input = io::stdin().lock();
            let mut output = io::stdout().lock();
            let mut buf = [0; 8192];
            loop {
                match input.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if output.write_all(&buf[..n]).is_err() {
                            break;
                        }
                        // The parent keeps stdin open while supervising the child.
                        // Flush each chunk so stdout progress does not depend on EOF.
                        if output.flush().is_err() {
                            break;
                        }
                    }
                }
            }
            let _ = output.flush();
        }
        ChildMode::ReadyThenStall => {
            let mut output = io::stdout().lock();
            let _ = output.write_all(&super::ready_output_fixture(12));
            let _ = output.write_all(&[0x47; 187]);
            let _ = output.flush();
            std::thread::sleep(std::time::Duration::from_secs(30));
        }
        ChildMode::EchoReadyIdle => {
            let fixture = super::ready_output_fixture(12);
            let mut input = io::stdin().lock();
            let mut output = io::stdout().lock();
            let mut bytes = vec![0; fixture.len()];
            if input.read_exact(&mut bytes).is_ok() {
                let _ = output.write_all(&bytes);
                let _ = output.flush();
            }
            std::thread::sleep(std::time::Duration::from_secs(30));
        }
    }
    // The test harness writes a completion trailer to stdout after this function
    // returns. Exit here so only the intended fake-child payload follows its prefix.
    // Give the parent pump time to consume libtest's fixed startup prefix before EOF.
    exit_after_prefix(0);
}

fn exit_after_prefix(code: i32) -> ! {
    std::thread::sleep(std::time::Duration::from_millis(250));
    std::process::exit(code);
}
