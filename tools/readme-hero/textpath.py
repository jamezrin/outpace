"""Turn text into kerned SVG path outlines, so the hero renders identically everywhere.

Fonts come from a pinned Inter release (SIL Open Font License 1.1). They're downloaded once,
verified by SHA-256, and cached in tools/readme-hero/.fonts/ (git-ignored). Only the glyph
outlines end up in the generated SVGs; no font file is committed or embedded.
"""
import hashlib
import io
import os
import urllib.request
import zipfile

from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTFont

INTER_URL = "https://github.com/rsms/inter/releases/download/v4.1/Inter-4.1.zip"
INTER_SHA256 = "9883fdd4a49d4fb66bd8177ba6625ef9a64aa45899767dde3d36aa425756b11e"
WEIGHTS = ("Medium", "SemiBold", "ExtraBold")
FONT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".fonts")


def ensure_fonts(font_dir=FONT_DIR):
    wanted = {w: os.path.join(font_dir, f"Inter-{w}.ttf") for w in WEIGHTS}
    if all(os.path.exists(p) for p in wanted.values()):
        return wanted
    with urllib.request.urlopen(INTER_URL, timeout=60) as response:
        data = response.read()
    digest = hashlib.sha256(data).hexdigest()
    if digest != INTER_SHA256:
        raise SystemExit(f"Inter archive checksum mismatch: {digest}")
    os.makedirs(font_dir, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        for weight, path in wanted.items():
            with open(path, "wb") as out:
                out.write(archive.read(f"extras/ttf/Inter-{weight}.ttf"))
        with open(os.path.join(font_dir, "LICENSE.txt"), "wb") as out:
            out.write(archive.read("LICENSE.txt"))
    return wanted


class Face:
    """One font weight: glyph outlines, advances and GPOS pair kerning."""

    def __init__(self, path):
        self.font = TTFont(path)
        self.glyphs = self.font.getGlyphSet()
        self.cmap = self.font.getBestCmap()
        self.upem = self.font["head"].unitsPerEm
        self.hmtx = self.font["hmtx"]
        self._pairs = self._kern_subtables()
        self._cache = {}

    def _kern_subtables(self):
        gpos = self.font["GPOS"].table
        indices = set()
        for record in gpos.FeatureList.FeatureRecord:
            if record.FeatureTag == "kern":
                indices.update(record.Feature.LookupListIndex)
        out = []
        for i in sorted(indices):
            lookup = gpos.LookupList.Lookup[i]
            for sub in lookup.SubTable:
                if lookup.LookupType == 9:
                    sub = sub.ExtSubTable
                if getattr(sub, "LookupType", 2) == 2 and hasattr(sub, "Format"):
                    out.append((sub, set(sub.Coverage.glyphs), sub.Coverage.glyphs))
        return out

    def kern(self, left, right):
        key = (left, right)
        if key in self._cache:
            return self._cache[key]
        value = 0
        for sub, covered, order in self._pairs:
            if left not in covered:
                continue
            record = None
            if sub.Format == 1:
                pair_set = sub.PairSet[order.index(left)]
                record = next((r.Value1 for r in pair_set.PairValueRecord if r.SecondGlyph == right), None)
                if record is None:
                    continue
            else:
                c1 = sub.ClassDef1.classDefs.get(left, 0)
                c2 = sub.ClassDef2.classDefs.get(right, 0)
                record = sub.Class1Record[c1].Class2Record[c2].Value1
            value = getattr(record, "XAdvance", 0) or 0 if record is not None else 0
            break
        self._cache[key] = value
        return value

    def layout(self, text, size, tracking=0.0):
        """Return [(glyph, x)] in font units scaled to `size`, and the total width."""
        scale = size / self.upem
        x, out, prev = 0.0, [], None
        for ch in text:
            glyph = self.cmap[ord(ch)]
            if prev is not None:
                x += self.kern(prev, glyph) * scale + tracking
            out.append((glyph, x))
            x += self.hmtx[glyph][0] * scale
            prev = glyph
        return out, x

    def path(self, text, size, x, baseline, tracking=0.0, anchor="start"):
        """SVG path data for `text`; anchor is start, middle or end. Returns (d, x0, width)."""
        placed, width = self.layout(text, size, tracking)
        x0 = x - width / 2 if anchor == "middle" else x - width if anchor == "end" else x
        scale = size / self.upem
        pen = SVGPathPen(self.glyphs, ntos=lambda v: f"{v:.2f}".rstrip("0").rstrip("."))
        for glyph, gx in placed:
            self.glyphs[glyph].draw(TransformPen(pen, (scale, 0, 0, -scale, x0 + gx, baseline)))
        return pen.getCommands(), x0, width
