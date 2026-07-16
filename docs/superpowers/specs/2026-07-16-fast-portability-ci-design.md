# Fast Portability CI Design

## Problem

The Compose and ARMv7 portability workflows compile Rust inside emulated containers or run the
workspace test suite through QEMU. Those jobs routinely approach or exceed their 45–60 minute
timeouts. QEMU is useful for runtime compatibility checks, but it is not required to prove that
Rust code cross-compiles for a target architecture.

## Design

Keep fast, deterministic checks automatic on pull requests:

- Run the Compose build, startup, health, permissions, and shutdown smoke only for native
  `linux/amd64` runners.
- Cross-compile the shipped `outpace` binary for `armv7-unknown-linux-musleabihf` without QEMU.

Keep emulated runtime checks available through `workflow_dispatch`:

- Allow the Compose workflow's manual invocation to opt into `linux/arm64`.
- Allow the ARMv7 workflow's manual invocation to opt into the QEMU container startup smoke.

The release workflow remains responsible for producing the supported multi-architecture images.
This change does not alter the supported platform list or release artifacts.

## Workflow Behavior

The Compose workflow gains a boolean manual input for the ARM64 smoke. Its native AMD64 job runs
on relevant pull requests and pushes as before. A separate ARM64 job runs only when a manual
dispatch explicitly enables it, preventing QEMU setup and emulated compilation during ordinary CI.

The ARMv7 workflow replaces its QEMU workspace-test job with a cross-compiled release binary build
using the existing ARM musl cross toolchain. Its container smoke remains defined but is guarded by
a boolean manual input and therefore never blocks ordinary pull requests.

## Validation

- Parse both workflow files as YAML.
- Run an available GitHub Actions workflow linter.
- Run formatting checks and the normal workspace test suite.
- Inspect event/job conditions to confirm PR and push events cannot schedule emulated jobs.

## Documentation

Update the Linux portability guide so it describes cross-compilation as the automatic gate and
QEMU container startup as an optional manual runtime check.
