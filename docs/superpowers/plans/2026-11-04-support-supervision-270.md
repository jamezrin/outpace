# Support supervision and validation (#270) implementation plan

> Use superpowers:executing-plans inline; obtain one independent whole-branch review before publication.

**Goal:** bounded operator-declared public live support without fake viewers.
**Architecture:** separate generation pins on existing manager sources, owned declaration supervisors, shared admission and bounded native controls.
**Tech Stack:** existing Rust/Tokio/Serde/Axum; no dependencies.
**Spec:** docs/superpowers/specs/2026-11-04-support-service-270-design.md (PR #328 merged).

## Constraints

Unset configuration is off. JSON64KiB,8 declarations, names1–64ASCII alphanumeric/underscore/hyphen, selectors1–4096bytes. Preserve default unlimited admission, public-only support, explicit inbound/seeding configuration, verified-only serving and all cache/inbound bounds. Never expose selectors in status/errors or read the private registry.

## Tasks

- [x] Add duplicate-aware bounded declaration parsing with genuine RED/GREEN for invalid maps, limits, selectors and redaction. Reuse CLI/provider selector grammar; perform no I/O. Baseline config suite must pass first.
- [x] After PR328 merge, refresh topic and incorporate its lifetime contract. Add atomic exact-generation pins and release/reaper/force-stop race tests. No per-pin detached cleanup task.
- [x] Preserve structured opening causes at their producers; retain legacy route mapping. Prove network/capacity retry versus terminal invalid/protected configuration without text matching.
- [x] Add one owned supervisor per declaration; desired control/epoch, bounded1→30second retry,30second progress reset, no overlap or false viewers. Force-stop suppresses support until explicit restart.
- [x] Wire validated configuration before side effects; start only after listeners succeed. Add bounded declared-name native status/start/stop; join service ownership on failure/shutdown.
- [x] Validate source/service/leecher, sharing and replacement/cancellation with actual signed synthetic peers. Retain scoped resource/performance evidence and precise interoperability limits.
- [x] Update operator docs; run affected and final workspace/lint/fmt/hygiene gates; resolve independent topology/restart/cache evidence findings. Stage 2 merged in PR #329; final validation is ready for integration.
- [ ] Commit final validation through normal hooks, publish its PR and merge after exact-head CI/review succeeds. Keep #270 open until that integration completes.

## Review focus

Canceled acquisition after pin publication; last-pin release racing HLS/viewer/new generation; stopped desired intent versus stale completion; hostile/deep/duplicate configuration with private values; persistent failure retaining source capacity; source EOF without retry busyloop. Test at owning real seams, not source-text assertions.

## Final validation scope

Five opt-in Linux namespace cases cover signed daemon reseeding, explicit-source comparison, native playback-gate independence and memory/disk generation restart/cache lifecycle. The historical baseline is the pre-supervision revision with the same explicit-source fixture. Metadata is synthetic SDK input; public acquisition, actual external decoder quality and official interoperability are not claimed. Resource snapshots and structural allocation/parser assessment retain their measurement limits. Reproduction and cleanup requirements: [offline topology guide](../../testing/support-topology.md).
