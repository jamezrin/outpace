"""Generate the README hero (docs/assets/hero-{light,dark}.svg).

Usage: python3 tools/readme-hero/generate.py docs/assets

The picture follows how a live AceStream swarm works:
- one swarm: a single peer mesh spans the width, and outpace (the wordmark) sits inside it,
  with peer links continuing behind it;
- one broadcaster signs the stream; its pieces spread peer to peer (the tinted chain);
- outpace downloads from a few upstream peers at once (four bright strands, as the default
  of four active upstreams) and seeds back to others (thinner dashed strands leaving it);
- a subtle CSS animation shows direction (inward downloads, outward uploads) and is
  disabled for readers who prefer reduced motion.
The output is deterministic, so re-running reproduces the committed files.
"""
import heapq
import math
import random
import sys

W, H = 1200, 400
CX, CY = 600, 190                  # centre of the wordmark
TEXT_BOX = (330, 82, 870, 330)     # region kept free of peers (wordmark + subtext)
WORD_L, WORD_R = CX - 214, CX + 214  # horizontal extent of the wordmark
WORD_TOP = CY - 70

rng = random.Random(11)


def in_text_box(x, y, pad=0):
    x0, y0, x1, y1 = TEXT_BOX
    return x0 - pad < x < x1 + pad and y0 - pad < y < y1 + pad


def scatter(n):
    pts = []
    tries = 0
    while len(pts) < n and tries < 20000:
        tries += 1
        x, y = rng.uniform(36, W - 36), rng.uniform(32, H - 32)
        if in_text_box(x, y, pad=8):
            continue
        if all(math.dist((x, y), p) > 64 for p in pts):
            pts.append((round(x, 1), round(y, 1)))
    return pts


peers = scatter(46)


def k_nearest_edges(pts, k=3, max_len=210):
    edges = set()
    for i, p in enumerate(pts):
        near = sorted(range(len(pts)), key=lambda j: math.dist(p, pts[j]))[1:k + 1]
        for j in near:
            if math.dist(p, pts[j]) <= max_len:
                edges.add(tuple(sorted((i, j))))
    return edges


edges = k_nearest_edges(peers)

# Bridge the two halves behind the wordmark so the swarm is visibly one network.
left_side = [i for i, (x, y) in enumerate(peers) if x < TEXT_BOX[0] and TEXT_BOX[1] < y < TEXT_BOX[3]]
right_side = [i for i, (x, y) in enumerate(peers) if x > TEXT_BOX[2] and TEXT_BOX[1] < y < TEXT_BOX[3]]
bridges = []
for li in sorted(left_side, key=lambda i: -peers[i][0])[:3]:
    ri = min(right_side, key=lambda j: abs(peers[j][1] - peers[li][1]) - peers[j][0] * 0.0)
    bridges.append(tuple(sorted((li, ri))))
edges |= set(bridges)


def neighbours(i):
    for a, b in edges:
        if a == i:
            yield b
        elif b == i:
            yield a


def shortest_path(src, dst):
    dist, prev, heap = {src: 0.0}, {}, [(0.0, src)]
    while heap:
        d, u = heapq.heappop(heap)
        if u == dst:
            break
        if d > dist[u]:
            continue
        for v in neighbours(u):
            nd = d + math.dist(peers[u], peers[v])
            if nd < dist.get(v, math.inf):
                dist[v], prev[v] = nd, u
                heapq.heappush(heap, (nd, v))
    path, node = [dst], dst
    while node != src:
        node = prev[node]
        path.append(node)
    return path[::-1]


def nearest(cands, target):
    return min(cands, key=lambda i: math.dist(peers[i], target))


# Four active upstreams: two on each side, close to the wordmark's ends, above the subtext.
def side_upstreams(side_ids, anchor):
    band = [i for i in side_ids if peers[i][1] < CY + 40]
    return sorted(band, key=lambda i: math.dist(peers[i], anchor))[:2]


up_left = side_upstreams([i for i, (x, _) in enumerate(peers) if x < TEXT_BOX[0]], (WORD_L, CY))
up_right = side_upstreams([i for i, (x, _) in enumerate(peers) if x > TEXT_BOX[2]], (WORD_R, CY))
upstreams = up_left + up_right

# Peers outpace seeds to: three in the band above the wordmark.
top_band = [i for i, (x, y) in enumerate(peers)
            if y < TEXT_BOX[1] and TEXT_BOX[0] - 40 < x < TEXT_BOX[2] + 40 and i not in upstreams]
seeded = sorted(top_band, key=lambda i: abs(peers[i][0] - CX))[:3]

# The broadcaster sits far from outpace; its pieces reach one upstream through the mesh.
broadcaster = min(range(len(peers)), key=lambda i: peers[i][0] + abs(peers[i][1] - CY) * 0.6)
chain = shortest_path(broadcaster, nearest(up_left, peers[broadcaster]))
chain_edges = {tuple(sorted(e)) for e in zip(chain, chain[1:])}

THEMES = {
    "light": dict(bg="#fbf7f1", node="#d6cabc", edge="#e3d9cd", ink="#1f1a16", muted="#6f655b",
                  hot="#ff5b35", warm="#ffad1f", chain="#f3b58f", src="#d9480f"),
    "dark": dict(bg="#121014", node="#3d3541", edge="#2b252f", ink="#f6f0e8", muted="#a59a8f",
                 hot="#ff6a42", warm="#ffbb3d", chain="#7a4a35", src="#ff8a5c"),
}


def strand(x0, y0, x1, y1, horizontal=True):
    if horizontal:
        cx = x0 + (x1 - x0) * 0.6
        return f"M{x0} {y0} C{cx:.1f} {y0} {cx:.1f} {y1:.1f} {x1:.1f} {y1:.1f}"
    cy = y0 + (y1 - y0) * 0.6
    return f"M{x0:.1f} {y0:.1f} C{x0:.1f} {cy:.1f} {x1:.1f} {cy:.1f} {x1} {y1}"


def render(theme):
    t = THEMES[theme]
    o = []
    a = o.append
    a(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
      'role="img" aria-labelledby="t d">')
    a('<title id="t">outpace</title>')
    a('<desc id="d">A single live AceStream swarm: a broadcaster\'s pieces spread peer to peer; '
      'outpace sits inside the swarm, downloads from four upstream peers, seeds to others, and '
      'serves the stream as MPEG-TS and HLS.</desc>')
    a('<style>'
      '.flow{stroke-dasharray:3 15;animation:flow 1.6s linear infinite}'
      '.up{stroke-dasharray:5 7;animation:up 1.8s linear infinite}'
      '@keyframes flow{to{stroke-dashoffset:-18}}'
      '@keyframes up{to{stroke-dashoffset:-12}}'
      '@media (prefers-reduced-motion: reduce){.flow,.up{animation:none}}'
      '</style>')
    a('<defs>')
    a(f'<linearGradient id="word" x1="0" y1="0" x2="1" y2="0"><stop stop-color="{t["hot"]}"/>'
      f'<stop offset="1" stop-color="{t["warm"]}"/></linearGradient>')
    a(f'<radialGradient id="glow" cx="0.5" cy="0.5" r="0.5"><stop stop-color="{t["hot"]}" stop-opacity="0.14"/>'
      f'<stop offset="1" stop-color="{t["hot"]}" stop-opacity="0"/></radialGradient>')
    # A soft background-coloured plate dims the peer links that pass behind the text.
    a(f'<radialGradient id="plate" cx="0.5" cy="0.5" r="0.5"><stop offset="0.55" stop-color="{t["bg"]}" stop-opacity="0.88"/>'
      f'<stop offset="1" stop-color="{t["bg"]}" stop-opacity="0"/></radialGradient>')
    a('</defs>')
    a(f'<rect width="{W}" height="{H}" rx="18" fill="{t["bg"]}"/>')

    for i, j in sorted(edges):
        (x1, y1), (x2, y2) = peers[i], peers[j]
        colour, width = (t["chain"], 2.2) if (i, j) in chain_edges else (t["edge"], 1.4)
        a(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{colour}" stroke-width="{width}"/>')

    a(f'<ellipse cx="{CX}" cy="{CY + 40}" rx="330" ry="150" fill="url(#plate)"/>')
    a(f'<ellipse cx="{CX}" cy="{CY + 10}" rx="360" ry="160" fill="url(#glow)"/>')

    # Downloads: four strands from upstream peers into the ends of the wordmark.
    for k, i in enumerate(upstreams):
        x, y = peers[i]
        left = x < CX
        ex = WORD_L if left else WORD_R
        ey = CY - 8 + (k % 2) * 10
        d = strand(x, y, ex, ey)
        a(f'<path d="{d}" fill="none" stroke="{t["hot"]}" stroke-opacity="0.55" stroke-width="2.6" stroke-linecap="round"/>')
        a(f'<path class="flow" d="{d}" fill="none" stroke="{t["hot"]}" stroke-width="2.6" stroke-linecap="round"/>')

    # Uploads: thinner dashed strands leaving the top of the wordmark towards other peers.
    for k, i in enumerate(sorted(seeded, key=lambda i: peers[i][0])):
        x, y = peers[i]
        sx = CX - 120 + k * 120
        d = strand(sx, WORD_TOP + 34, x, y, horizontal=False)
        a(f'<path class="up" d="{d}" fill="none" stroke="{t["warm"]}" stroke-width="1.8" stroke-linecap="round"/>')

    for i, (x, y) in enumerate(peers):
        if i == broadcaster:
            for r, op in ((22, 0.18), (15, 0.32)):
                a(f'<circle cx="{x}" cy="{y}" r="{r}" fill="none" stroke="{t["src"]}" stroke-opacity="{op}" stroke-width="2"/>')
            a(f'<circle cx="{x}" cy="{y}" r="8.5" fill="{t["src"]}"/>')
        elif i in upstreams:
            a(f'<circle cx="{x}" cy="{y}" r="7" fill="{t["hot"]}"/>')
            a(f'<circle cx="{x}" cy="{y}" r="3" fill="{t["bg"]}"/>')
        elif i in seeded:
            a(f'<circle cx="{x}" cy="{y}" r="6" fill="{t["bg"]}" stroke="{t["warm"]}" stroke-width="2.4"/>')
        else:
            r = 4 + (int(x * 7 + y) % 3)
            a(f'<circle cx="{x}" cy="{y}" r="{r}" fill="{t["node"]}"/>')

    font = "ui-sans-serif, -apple-system, 'Segoe UI', 'Helvetica Neue', Helvetica, Arial, sans-serif"
    halo = f'stroke="{t["bg"]}" stroke-width="8" stroke-linejoin="round" paint-order="stroke"'
    a(f'<g font-family="{font}" text-anchor="middle">')
    a(f'<text x="{CX}" y="{CY + 34}" font-size="112" font-weight="800" letter-spacing="-4" '
      f'{halo} fill="{t["ink"]}">outp<tspan fill="url(#word)">ace</tspan></text>')
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
