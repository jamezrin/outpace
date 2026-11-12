"""Generate the README hero (docs/assets/hero-{light,dark}.svg).

Usage: python3 tools/readme-hero/generate.py docs/assets

Concept: two peer meshes on either side of a centred wordmark; a few highlighted
strands leave the meshes and converge on the wordmark (many peers in, one stream out).
"""
import math
import random
import sys

W, H = 1200, 360
CX, CY = 600, 168  # convergence point (centre of the wordmark)

rng = random.Random(7)


def mesh(x0, x1, n):
    pts = []
    while len(pts) < n:
        x, y = rng.uniform(x0, x1), rng.uniform(34, H - 34)
        if all(math.dist((x, y), p) > 62 for p in pts):
            pts.append((round(x, 1), round(y, 1)))
    return pts


left = mesh(36, 300, 14)
right = mesh(900, 1164, 14)


def edges(pts):
    out = set()
    for i, p in enumerate(pts):
        near = sorted(range(len(pts)), key=lambda j: math.dist(p, pts[j]))[1:3]
        for j in near:
            out.add(tuple(sorted((i, j))))
    return sorted(out)


def inner(pts, side):
    # nodes nearest the centre feed the stream
    # Only nodes in the upper and middle band feed the stream, so no strand climbs up
    # through the tagline from below.
    key = (lambda p: -p[0]) if side == "left" else (lambda p: p[0])
    band = [p for p in pts if p[1] < CY + 40]
    return sorted(band, key=key)[:4]


THEMES = {
    "light": dict(bg="#fbf7f1", node="#d8cdbf", edge="#e6ddd2", ink="#1f1a16",
                  muted="#6f655b", hot="#ff5b35", warm="#ffad1f", ring="#fbf7f1"),
    "dark": dict(bg="#121014", node="#3b343f", edge="#2a252e", ink="#f6f0e8",
                 muted="#a59a8f", hot="#ff6a42", warm="#ffbb3d", ring="#121014"),
}


def render(theme):
    t = THEMES[theme]
    o = []
    a = o.append
    a(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
      'role="img" aria-labelledby="t d">')
    a('<title id="t">outpace</title>')
    a('<desc id="d">Peers from public AceStream swarms converge into one stream: an open-source, '
      'AceStream-compatible engine written in Rust that serves MPEG-TS and HLS.</desc>')
    a('<defs>')
    a(f'<linearGradient id="flowL" x1="0" y1="0" x2="1" y2="0"><stop stop-color="{t["warm"]}" stop-opacity="0.15"/>'
      f'<stop offset="1" stop-color="{t["hot"]}"/></linearGradient>')
    a(f'<linearGradient id="flowR" x1="1" y1="0" x2="0" y2="0"><stop stop-color="{t["warm"]}" stop-opacity="0.15"/>'
      f'<stop offset="1" stop-color="{t["hot"]}"/></linearGradient>')
    a(f'<linearGradient id="word" x1="0" y1="0" x2="1" y2="0"><stop stop-color="{t["hot"]}"/>'
      f'<stop offset="1" stop-color="{t["warm"]}"/></linearGradient>')
    a(f'<radialGradient id="glow" cx="0.5" cy="0.5" r="0.5"><stop stop-color="{t["hot"]}" stop-opacity="0.16"/>'
      f'<stop offset="1" stop-color="{t["hot"]}" stop-opacity="0"/></radialGradient>')
    a('</defs>')
    a(f'<rect width="{W}" height="{H}" rx="18" fill="{t["bg"]}"/>')
    a(f'<ellipse cx="{CX}" cy="{CY + 10}" rx="360" ry="150" fill="url(#glow)"/>')

    for pts in (left, right):
        for i, j in edges(pts):
            (x1, y1), (x2, y2) = pts[i], pts[j]
            a(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{t["edge"]}" stroke-width="1.5"/>')

    # converging strands: cubic curves from inner mesh nodes to just outside the wordmark
    for side, pts in (("left", left), ("right", right)):
        grad = "flowL" if side == "left" else "flowR"
        end_x = CX - 218 if side == "left" else CX + 218
        for k, (x, y) in enumerate(inner(pts, side)):
            ey = CY - 4 + (k - 1.5) * 6
            c1x = x + (end_x - x) * 0.62
            a(f'<path d="M{x} {y} C{c1x:.1f} {y} {c1x:.1f} {ey:.1f} {end_x} {ey:.1f}" fill="none" '
              f'stroke="url(#{grad})" stroke-width="2.4" stroke-linecap="round"/>')

    hot_nodes = set(inner(left, "left") + inner(right, "right"))
    for pts in (left, right):
        for (x, y) in pts:
            if (x, y) in hot_nodes:
                a(f'<circle cx="{x}" cy="{y}" r="7" fill="{t["hot"]}"/>')
                a(f'<circle cx="{x}" cy="{y}" r="3" fill="{t["ring"]}"/>')
            else:
                r = 4 + (int(x * 7 + y) % 3)
                a(f'<circle cx="{x}" cy="{y}" r="{r}" fill="{t["node"]}"/>')

    font = "ui-sans-serif, -apple-system, 'Segoe UI', 'Helvetica Neue', Helvetica, Arial, sans-serif"
    a(f'<g font-family="{font}" text-anchor="middle">')
    a(f'<text x="{CX}" y="{CY + 34}" font-size="112" font-weight="800" letter-spacing="-4" '
      f'fill="{t["ink"]}">outp<tspan fill="url(#word)">ace</tspan></text>')
    halo = f'stroke="{t["bg"]}" stroke-width="8" stroke-linejoin="round" paint-order="stroke"'
    a(f'<text x="{CX}" y="{CY + 86}" font-size="21" {halo} fill="{t["muted"]}">'
      'Public AceStream swarms in. MPEG-TS and HLS out.</text>')
    a(f'<text x="{CX}" y="{CY + 124}" font-size="13" font-weight="700" letter-spacing="4" '
      f'{halo} fill="{t["hot"]}">OPEN SOURCE  ·  RUST  ·  SELF-HOSTED</text>')
    a('</g>')
    a('</svg>')
    return "\n".join(o) + "\n"


if __name__ == "__main__":
    out = sys.argv[1]
    for theme in THEMES:
        with open(f"{out}/hero-{theme}.svg", "w") as f:
            f.write(render(theme))
