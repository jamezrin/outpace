#!/usr/bin/env python3
"""Check that links between GitHub-wiki pages resolve.

Usage: check_links.py [WIKI_DIR]   (default: docs/wiki)

Every Markdown link whose target has no URL scheme is treated as a wiki page
link: `[text](Page-Name)` or `[text](Page-Name#anchor)` or `[text](#anchor)`.
The page must exist as `<Page-Name>.md` in WIKI_DIR, and the anchor must match
a heading slug on that page (GitHub style: lowercase, punctuation dropped,
spaces to hyphens). Fenced code blocks are ignored. `[[wiki-style]]` links are
rejected so the folder keeps one link syntax that also renders on github.com.

Exits non-zero when any link is broken.
"""

import re
import sys
from pathlib import Path

FENCE = re.compile(r"^```.*?^```", re.S | re.M)
HEADING = re.compile(r"^#{1,6}\s+(.*?)\s*#*\s*$", re.M)
LINK = re.compile(r"\[([^\]]*)\]\(([^)\s]+)\)")
WIKI_LINK = re.compile(r"\[\[[^\]]+\]\]")
SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


def slug(heading: str) -> str:
    text = heading.strip().lower().replace("`", "")
    text = "".join(c for c in text if c.isalnum() or c in " -_")
    return text.replace(" ", "-")


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "docs/wiki")
    pages = {p.stem: FENCE.sub("", p.read_text(encoding="utf-8")) for p in root.glob("*.md")}
    if not pages:
        print(f"no wiki pages found in {root}", file=sys.stderr)
        return 1
    anchors = {name: {slug(h) for h in HEADING.findall(text)} for name, text in pages.items()}

    errors = []
    for name, text in sorted(pages.items()):
        for match in WIKI_LINK.finditer(text):
            errors.append(f"{name}.md: use [text](Page-Name) instead of {match.group(0)}")
        for match in LINK.finditer(text):
            target = match.group(2)
            if SCHEME.match(target) or target.startswith(("<", "/")):
                continue
            page, _, anchor = target.partition("#")
            page = page or name
            if page not in pages:
                errors.append(f"{name}.md: link to missing page {target!r}")
            elif anchor and anchor not in anchors[page]:
                errors.append(f"{name}.md: link to missing anchor {target!r}")

    for error in errors:
        print(error, file=sys.stderr)
    print(f"checked {len(pages)} wiki pages: {len(errors)} broken link(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
