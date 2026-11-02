# Explicit VLC verbosity implementation plan

Status: implementation, offline tests and independent review complete; CI and merge pending.

1. Add focused tests in tools/abcompare/test_vlc_verbosity.py using actual fake subprocesses for probe/client ownership and captured launch argv. Observe selected-level and invalid-input RED while default behavior remains passing.
2. Add bounded validation and client-only argv/config plumbing to vlc_compare.py; expose CLI option without changing defaults. Run focused and full offline Python suites, compilation, hygiene and whitespace gates.
3. Update abcompare README with interpretation and perturbation limits; obtain independent correctness/test-fidelity review, address findings, commit via hooks, PR/CI and exact-head merge. No runtime activation or issue closure follows automatically.
