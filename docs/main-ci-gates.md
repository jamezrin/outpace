# Main branch merge gates

[`.github/main-ruleset.json`](../.github/main-ruleset.json) is the reviewed API
payload for the active `main CI merge gates` branch ruleset. Tracking this file
does not apply it: an administrator must create or update the repository ruleset
and verify the effective rules through GitHub's API.

The policy targets exactly `refs/heads/main`, requires a pull request, blocks
deletion and non-fast-forward updates, and grants no bypass actors, including
administrators and Renovate. It permits the repository's existing merge, squash,
and rebase methods. Resolved review threads are required; formal approval count,
Code Owner approval, and last-push approval are not required. A solo maintainer
can therefore merge after the independent review used by the agent workflow,
without a second GitHub account. That independent review remains a process
requirement rather than a GitHub approval-count gate.

## Required checks

All four contexts are bound to GitHub Actions app ID `15368`, verified from
successful main and pull-request check runs. Update the deployed ruleset whenever
renaming one of these jobs; editing the JSON alone does not update GitHub.

| Context | Workflow | Coverage |
| --- | --- | --- |
| `Rust lint` | `ci.yml` | Every PR and push to main; rustfmt and Clippy |
| `Rust test` | `ci.yml` | Every PR and push to main; offline workspace tests |
| `A/B harness test` | `ci.yml` | Every PR and push to main; offline Python fixtures and loopback HTTP |
| `acestream identifier hygiene` | `hygiene.yml` | Every PR and every branch push; tracked identifier scan |

Neither workflow has a path filter or job condition that skips these checks.
Hygiene may run on both a topic-branch push and its PR; these are the same job in
the same workflow, not unrelated producers sharing a name. Inspect the PR's
actual checks when integrating: GitHub evaluates the test merge commit when it
has checks, otherwise the head commit. Do not substitute a green push run for a
failed PR run. See [GitHub's required-check troubleshooting guidance](https://docs.github.com/en/pull-requests/how-tos/merge-and-close-pull-requests/troubleshooting-required-status-checks).

Compose `Smoke (linux/amd64)` and `Cross-build ARMv7 release binary` are
path-filtered. `ARMv7 container startup and health smoke` only runs through an
explicit manual input, and release checks only run for tags. These checks remain
useful when triggered but are not globally required: a documentation-only PR
cannot produce them. Workflow-wide path filters can leave required checks
pending indefinitely.

The required-check rule is strict: the topic branch must include the current
main before merging. If main advances, merge main into the topic branch or rebase
an unpublished branch, then repeat checks and review for the new head. Never
force-push main or bypass failed checks. GitHub's check gate accepts successful,
neutral, and skipped conclusions; the required jobs currently have no skip
conditions. App binding identifies the producer, not an immutable workflow, so
workflow changes still need independent review. See [GitHub's rules documentation](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets).

## Renovate and access

Renovate's non-major updates request platform automerge in `renovate.json`.
The same required checks and current-base rule apply to bot PRs because the
ruleset has no bypass actors. As inventoried for issue #180, GitHub's repository
`allow_auto_merge` setting is false; this policy does not enable it. Thus the
current platform-automerge path cannot be exercised end to end with a bot
credential. If enabled separately, native automerge must still wait for the
required checks; any ordinary fallback merge is governed by the same policy.
See [Renovate's platform-automerge documentation](https://docs.renovatebot.com/configuration-options/#platformautomerge).

Public repositories support branch rulesets even on GitHub Free. Creating or
updating them requires repository administration write permission. Empty bypass
actors remove merge exemptions but cannot prevent an administrator from editing
the policy itself. See [GitHub's ruleset availability](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/about-rulesets)
and [ruleset API](https://docs.github.com/en/rest/repos/rules#create-a-repository-ruleset).

## Apply and verify

Before applying, save the existing repository rulesets, effective main rules,
classic branch protection response (including an unprotected 404), and repository
settings. Re-read them immediately before creation to detect concurrent changes.
After independent review, create the ruleset once:

```sh
gh api --method POST repos/jamezrin/outpace/rulesets \
  --input .github/main-ruleset.json
gh api repos/jamezrin/outpace/rulesets
gh api repos/jamezrin/outpace/rules/branches/main
```

Save the returned ruleset ID and read its full configuration with
`gh api repos/jamezrin/outpace/rulesets/RULESET_ID`. Compare the name, target,
enforcement, bypass actors, conditions, and rules against the payload; the
effective main rules must contain all four rule types. Avoid creating duplicates
on retry. Use an independently reviewed PUT to that ID for future updates.

Validate failing CI on a disposable `test/` PR that changes only the lint step to
exit unsuccessfully. Require a completed failing `Rust lint` check from Actions,
current main ancestry, and GitHub `mergeStateStatus=BLOCKED`, then confirm the
ordinary CLI merge path refuses it without admin or automerge flags. Close it
unmerged. A CLI refusal uses GitHub's policy state; it is not a server merge
mutation test. Never issue a speculative raw merge request or force-push/delete
main to test protection. Read the non-fast-forward and deletion rules instead.
Inspect a failing Renovate PR and the absence of bypass actors separately, and
state the bot-credential/platform-automerge limitation in the evidence.

The passing, independently reviewed policy PR is the positive experiment: wait
for all exact-head checks, confirm it is current with main and all review threads
are resolved, then merge normally without bypass. Read back the rules after
merging and confirm the failing probe stayed unmerged.

Rollback is an explicit administrator recovery operation, never a way to merge
failed CI. If the new policy locks out valid PRs, report the API evidence before
changing anything. The initial inventory for #180 has no existing rulesets or
classic protection; restoring it would remove this issue's newly created ruleset
by its returned ID and reopen the protection gap. Do that only after explicit
recovery authorization, preserving any concurrently added rules. No repository
automerge, collaborator, VPN, playback, Docker, cache, or environment defaults
change with this policy.
