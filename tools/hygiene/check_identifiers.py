#!/usr/bin/env python3
"""Fail-closed guard against leaking real AceStream identifiers.

Blocks any high-entropy 40-hex string (a real content id or infohash) from
entering tracked files, and hard-blocks committing the private id registry.

This is a mechanical backstop for the "AceStream Identifier Hygiene" rule in
AGENTS.md. It is fail-closed: an unrecognized high-entropy 40-hex value is
rejected until a maintainer vouches for it in `allowed-identifiers.txt`.

It deliberately does NOT read `acestream-ids.txt` — the registry is off-limits
to every automatic harness. Detection is purely structural:

  * obvious synthetic placeholders (the documented `0123…` family, all-zeros,
    `cafe…`, the SHA1("abc") vector, …) are exempt — that is what code should
    use where a 40-hex string is syntactically required;
  * every other 40-hex value must appear on the allowlist, which enumerates the
    legitimate non-placeholder hashes that live in the tree (synthetic-fixture
    digests, catalog-signature vectors, git SHAs quoted in docs, the jemalloc
    build hash in Cargo.lock);
  * anything left over is treated as a real identifier and rejected.

Stream *names* are not machine-detected: any denylist would have to contain the
real names, which would itself violate the policy, and reading the registry is
forbidden. Names remain enforced by review and by AGENTS.md. Because a real name
almost always travels next to its content id, the hex gate still surfaces the
neighborhood of such a leak.

Usage:
    check_identifiers.py                # scan all tracked files (CI)
    check_identifiers.py --staged       # scan staged blobs (pre-commit)
    check_identifiers.py FILE [FILE...]  # scan specific working-tree files
"""
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                      capture_output=True, text=True).stdout.strip()

REGISTRY = "acestream-ids.txt"
ALLOWLIST = os.path.join("tools", "hygiene", "allowed-identifiers.txt")

# Files/dirs whose 40-hex content is never a stream identifier or is handled
# elsewhere. The allowlist file and this script quote hex on purpose.
EXCLUDE_EXACT = {ALLOWLIST, os.path.join("tools", "hygiene", "check_identifiers.py")}
EXCLUDE_PREFIX = ("re/", "references/", "target/", ".git/")
# Only scan text; skip the known binary fixture trees (their bytes are covered
# by the byte-level review during a rewrite, and they carry no ASCII hex ids).
BINARY_SUFFIX = (".bin", ".ts", ".hex", ".png", ".jpg", ".gif", ".ico", ".pdf")

HEX40 = re.compile(rb"(?<![0-9a-fA-F])([0-9a-fA-F]{40})(?![0-9a-fA-F])")

# Synthetic placeholder patterns — exempt. Mirrors the audit used during the
# history rewrite. A value matching any of these is an obvious non-identifier.
SYNTH_PREFIXES = (
    "0123456789abcdef", "123456789abcdef0", "23456789abcdef01",
    "89abcdef0123", "0123456789012345", "fedcba9876543210",
    "00112233445566", "000102030405", "00010a0f10abcdef",
    "1123456789abcdef", "deadbeef", "cafe", "5233302d",
)
SYNTH_EXACT = {
    "a9993e364706816aba3e25717850c26c9cd0d89d",  # SHA1("abc") standard vector
}


def is_synthetic(h: str) -> bool:
    h = h.lower()
    if len(set(h)) <= 5:
        return True
    if h in SYNTH_EXACT:
        return True
    # The documented ramp placeholder is any single-nibble prefix followed by
    # the ascending run; catch it wherever the run appears.
    if "23456789abcdef0123456789abcdef0123456" in h:
        return True
    return any(h.startswith(p) for p in SYNTH_PREFIXES)


def load_allowlist() -> set:
    path = os.path.join(ROOT, ALLOWLIST)
    allow = set()
    if not os.path.exists(path):
        return allow
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            allow.add(line.split()[0].lower())
    return allow


def staged_files() -> list:
    out = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"],
        capture_output=True, text=True).stdout.split("\n")
    return [f for f in out if f]


def tracked_files() -> list:
    out = subprocess.run(["git", "ls-files"], capture_output=True, text=True).stdout
    return [f for f in out.split("\n") if f]


def read_blob(path: str, staged: bool) -> bytes:
    if staged:
        r = subprocess.run(["git", "show", f":{path}"], capture_output=True)
        return r.stdout if r.returncode == 0 else b""
    full = os.path.join(ROOT, path)
    try:
        with open(full, "rb") as f:
            return f.read()
    except (FileNotFoundError, IsADirectoryError):
        return b""


def excluded(path: str) -> bool:
    if path in EXCLUDE_EXACT:
        return True
    if path == "Cargo.lock":
        return True
    if path.startswith(EXCLUDE_PREFIX):
        return True
    if path.endswith(BINARY_SUFFIX):
        return True
    return False


def scan(files, staged, allow):
    findings = []
    for path in files:
        # Absolute rule: the private registry must never be committed.
        if os.path.basename(path) == REGISTRY:
            findings.append((path, 0, "REGISTRY", "the private id registry must never be committed"))
            continue
        if excluded(path):
            continue
        data = read_blob(path, staged)
        if b"\x00" in data[:8192]:  # binary
            continue
        for lineno, line in enumerate(data.split(b"\n"), 1):
            for m in HEX40.finditer(line):
                v = m.group(1).decode().lower()
                if is_synthetic(v) or v in allow:
                    continue
                findings.append((path, lineno, v, "non-placeholder 40-hex not on the allowlist"))
    return findings


def main() -> int:
    args = sys.argv[1:]
    staged = False
    if args and args[0] == "--staged":
        staged, args = True, args[1:]
    if args:
        files = args
    elif staged:
        files = staged_files()
    else:
        files = tracked_files()

    allow = load_allowlist()
    findings = scan(files, staged, allow)

    if not findings:
        return 0

    print("AceStream identifier hygiene check FAILED\n", file=sys.stderr)
    for path, lineno, val, why in findings:
        loc = f"{path}:{lineno}" if lineno else path
        show = val if val in ("REGISTRY",) else val
        print(f"  {loc}: {why}\n      {show}", file=sys.stderr)
    print(
        "\nContent ids, infohashes, and stream names must never enter tracked\n"
        "files (see AGENTS.md → AceStream Identifier Hygiene). Use the synthetic\n"
        "placeholder 0123456789abcdef0123456789abcdef01234567 where a 40-hex\n"
        "string is required, or reference the registry by identifier (cid1, …).\n"
        "If a flagged value is a legitimate non-identifier hash, add it with a\n"
        "justification to tools/hygiene/allowed-identifiers.txt.",
        file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
