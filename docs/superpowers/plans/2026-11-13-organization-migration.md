# Organization Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move Outpace into its organization with independent Android and package-manager repositories and current documentation.

**Architecture:** Transfer the existing engine repository in place. Keep release artifact production in the engine and distribute published artifacts through separate Homebrew and Scoop catalogs.

**Tech Stack:** GitHub REST/GraphQL, Git, Python release tooling, GitHub Actions, Markdown.

**Spec:** `docs/superpowers/specs/2026-11-13-organization-migration-design.md`

## Global Constraints

Keep the root checkout on main and preserve local changes. Use a chore/ topic branch based on main. Leave existing t3/ branches untouched under the maintainer's instruction to proceed. No runtime logic, exposed network defaults, dependencies or hot paths change. Never include real stream identifiers. Do not recreate the original GitHub repository.

## Review Focus

- Original repository URLs must redirect and retain the same repository ID and stars/forks.
- Historical releases have the old attestation identity and old image tags must remain usable.
- Manifest generation must target separate repositories and never recreate catalogs in the engine.
- Android issue/design links must resolve across repositories and engine prerequisites remain engine-owned.
- New image documentation must not claim unpublished tags exist.

### Task 1: Repository transfer and catalog setup

- [x] Capture repository identity/counts, transfer to outpace-stream/outpace-engine, and verify identity/counts.
- [x] Create the three public companion repositories with main branches, descriptions and links.
- [x] Update origin to the transferred repository without changing the root working files.

### Task 2: Separate catalogs and update references

**Files:** `tools/release/package_manifests.py`, its test, `.github/workflows/package-manifests.yml`, `Formula/outpace.rb`, `bucket/outpace.json`, active references throughout the repository, and companion repository CI/readmes/catalogs.

- [x] Add failing tests that explicit separate output roots receive their respective catalog and invalid/missing sums write no output.
- [x] Require explicit catalog destinations in generation tooling, remove catalogs from the engine and install CI to their new repositories.
- [x] Generate both catalogs from published v1.0.1 SHA256SUMS, preserving checksums and archive layout.
- [x] Update active repository/image URLs and wiki installation/release instructions, including existing-install migration and historical provenance.
- [x] Run Python release tests, wiki links, hygiene, format and relevant workflow/static checks; inspect the diff.

### Task 3: Android ownership and publication

- [x] Copy the Android implementation and design with attribution; separate app releases from engine releases.
- [x] Transfer issue 453, update its implementation/release scope and preserve its engine prerequisites.
- [ ] Supersede the engine Android design PR only after the replacement design is safely published.
- [ ] Commit and push compliant topic branches; open and link PRs with validation, performance and wiki notes.
- [ ] Merge verified migration changes when gates permit, republish the wiki from main, and publish/verify the new image namespace. Watch any required PR gates still pending and report exact limits.

## Execution notes

- Repository identity, 14 stars and 3 forks verified after transfer; old URL resolves to the same repository.
- Pre-flight: generator destinations are consumed by separate catalog repos; validated both outputs before writing.
- Android implementation advanced during migration; copied its tracked, clean development snapshot without changing the original checkout. Engine-only 32-bit archive work remains tracked in engine issue #459 and preserved draft PR #454.
- Historical repository attestation lookup returned 404. Original-owner bundle lookup and verification against the signed original repository identity succeeded for the v1.0.1 Linux x86_64 archive.
- Final independent review: no Critical or Important findings. Two stale documentation statements found within the requested reference-update scope were corrected (latest version and independent Android engine-asset availability).

Validation: cargo test --workspace --locked, cargo clippy --workspace --all-targets --locked -- -D warnings, cargo fmt --all --check, 17 Python release tooling tests, and wiki link checking (25 pages) passed. Android testDebugUnitTest/lintDebug/assembleRelease passed with 54 test cases, zero failures/errors and one optional real-archive test skipped. No runtime Rust code changed, so no hot-path performance measurement is required.
