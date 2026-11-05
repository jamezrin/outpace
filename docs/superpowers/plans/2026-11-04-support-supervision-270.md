# Support supervision (#270 stage 2) implementation plan

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
- [ ] Validate source/service/leecher, sharing and replacement/cancellation with actual signed synthetic peers. Retain scoped resource/performance evidence and precise interoperability limits.
- [ ] Update operator docs, run affected tests and final workspace/lint/fmt/hygiene gates, independent review, normal hooks, PR/CI and exact-head merge. Keep270open until complete acceptance, including topology/docs, is proven.

## Review focus

Canceled acquisition after pin publication; last-pin release racing HLS/viewer/new generation; stopped desired intent versus stale completion; hostile/deep/duplicate configuration with private values; persistent failure retaining source capacity; source EOF without retry busyloop. Test at owning real seams, not source-text assertions.
