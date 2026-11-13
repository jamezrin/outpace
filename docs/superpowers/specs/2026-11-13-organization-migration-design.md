# Organization migration design

Approved by the maintainer in the migration conversation.

Transfer and rename the existing repository to `outpace-stream/outpace-engine`, preserving its identity, history, stars, forks, issues, pull requests, releases and wiki. Create public `outpace-android`, `homebrew-outpace` and `scoop-outpace` repositories in the same organization. Move the Android design and issue to the Android repository; engine build changes remain engine-owned. Move the Homebrew formula and Scoop manifest into their dedicated repositories. Keep `packaging/` and release generation tooling with the engine.

Publish future container images as `ghcr.io/outpace-stream/outpace-engine`. Preserve existing personal-namespace images. Update active links, badges, metadata, installers, workflows, Compose defaults, wiki pages and release instructions. Historical package URLs and attestation identities must remain accurate; explain migration where necessary. Keep the executable and release archive names `outpace`.

## Approaches

The approved approach transfers the existing repository and separates package-manager catalogs. Recreating the engine repository would lose its identity and social history. Keeping all catalogs in the engine would retain the unwanted top-level directories. Separating build packaging would unnecessarily couple release builds across repositories.

## Constraints

Keep the root checkout on main and preserve local changes. Use a chore/ topic branch based on main. The maintainer instructed proceeding after being informed about existing t3/ branches; leave them untouched. No runtime logic, exposed network defaults, dependencies or hot paths change. Never include real stream identifiers. Do not recreate the original GitHub repository, which would break its redirects.
