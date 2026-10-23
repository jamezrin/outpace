# Supported Ace compatibility configuration

Intent: graduate the documented Ace HTTP subset to supported configuration without enabling extra routes by default. The operator contract and client inventory already define that subset; this change does not expand it.

## Approved approach

Use `OUTPACE_ACE_COMPAT` as the canonical boolean. Keep compatibility off by default and retain `OUTPACE_EXPERIMENTAL_ACE_COMPAT` as a deprecated fallback during migration. The canonical variable wins whenever present, including explicit false. Parse only the selected variable using the existing exact `1`, `true`, `0`, `false` rules; empty, malformed, or non-Unicode selected values fail startup. An invalid canonical value never falls back; an unused legacy value cannot override or invalidate a valid canonical setting.

Alternatives considered: enable compatibility by default, or retain only the experimental name. Default-on exposes routes to deployments that did not request them; retaining only the old name fails the supported-configuration goal. The user approved the default-off migration.

## Scope and constraints

- Preserve the existing internal/config-file `experimental_ace_compat` field for Rust and serialized configuration compatibility; update its description rather than forcing an unrelated API migration.
- Generic Compose passes the canonical setting with unset-only legacy fallback, so legacy-only installations still work and explicit false wins. Preserve invalid empty values for runtime validation. LXC deployment remains explicitly opted in using the new name.
- Update active README, environment example, support contract, client inventory, native/deployment docs. Historical plans/notes retain their original names as history. Describe the old variable as deprecated without inventing a removal release.
- Remove experimental labeling from the supported subset, while preserving unsupported/deferred capability limits. Correct the nearby stale compatibility `Icy-Name` promise and clarify per-request transcoding versus the existing global HLS audio option where migration touches those paragraphs.
- No route, token, authentication, SSRF, bind, native API, dependency, or audio policy changes. No deployment is applied and no public network probe is needed.

## Ownership and verification

Sol implements and independently reviews. Luna writes and executes tests and all validation. Tests cover default-off, canonical values, legacy-only fallback, conflicts, invalid selected values, ignored legacy values, Compose migration, and route disabling. Existing route/security tests remain authoritative. Validate offline; no Docker containers, live sources, registry reads, or WARP changes are required.
