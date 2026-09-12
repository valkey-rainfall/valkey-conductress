#!/usr/bin/env python3
"""Build every Conductress brand asset from one source of truth.

Run from anywhere:  python3 brand/tools/build.py

Outputs (all checked in, so consumers never need to run this). For the primary
sunset palette, in brand/logo/:
  conductress-hero.svg / .png (1800x680)  opaque scene: starfield, mountain horizon, grid,
                                          streaks, trail, C, wordmark, subtitle; identical
                                          to the sign-on's resting frame
  conductress-lockup.svg                  transparent: streaks, trail, C, wordmark, subtitle
  conductress-wordmark.svg                wordmark alone, outlined, with its chromatic fringe
  conductress-mark.svg                    the keyhole C alone, 7 stripes, 100x100
  conductress-mark-32.svg                 5-stripe cut for 32-63px
  conductress-mark-16.svg                 3-stripe cut for 16px
  conductress-mark-{512,180,32,16}.png, favicon.ico (16+32+48)
Every other palette gets the same set (minus the hero for mono) in its own folder,
with the palette name as a filename suffix. brand/palette.svg is the swatch strip.

PALETTES below is the whole design in data. Three kinds:
  ramp   a gradient runs once across the mark, top to bottom (sunset, valkey)
  bands  flat flag stripes (pride, trans, bi, lesbian, nonbinary)
  mono   one colour, for print/embroidery/engraving; no hero, no fringe

Type is converted to outlines with fontTools so the SVGs render identically with
no font installed. Wordmark: Nimbus Sans Bold (URW's Helvetica). Subtitle:
DejaVu Sans Mono. The two font paths are the only machine-specific part.

Geometry is the design's own: pointy-top hexagon R=46 about (50,50) in a 100-unit
box, keyhole = circle r=22 plus a channel (y 39..61) to the right edge. In the
lockup the lead hex is 126 units wide at (540,110).
"""
from __future__ import annotations

import io
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FONT_SANS = "/usr/share/fonts/urw-base35/NimbusSans-Bold.otf"
FONT_MONO = "/usr/share/fonts/dejavu-sans-mono-fonts/DejaVuSansMono.ttf"

# ------------------------------------------------------------------ colours

CREAM, AMBER, ORANGE, MAGENTA, VIOLET = "#ffe08a", "#ffc94a", "#ff7a3d", "#ff2d7e", "#6d3bd8"
MIDNIGHT, PAPER, GOLD = "#150c26", "#fff6ea", "#c9a86e"
MIDNIGHT_SKY = ("#2a1140", MIDNIGHT, "#07040d")

VALKEY = "#6983ff"  # the Valkey brand periwinkle
VALKEY_RAMP = [(0, "#e3e8ff"), (0.28, "#a4b3ff"), (0.56, VALKEY), (0.82, "#4a5de0"), (1, "#2e3a9e")]

RAINBOW = ["#e40303", "#ff8c00", "#ffed00", "#008026", "#24408e", "#732982"]  # six-stripe pride flag
RAINBOW3 = ["#f24802", "#80b613", "#4c3588"]  # adjacent pairs merged, for 16px
TRANS = ["#5bcefa", "#f5a9b8", "#ffffff", "#f5a9b8", "#5bcefa"]
BI = ["#d60270", "#d60270", "#9b4f96", "#0038a8", "#0038a8"]  # the flag's 2:1:2 proportions
LESBIAN = ["#d52d00", "#ff9a56", "#ffffff", "#d362a4", "#a30262"]
NONBINARY = ["#fcf434", "#ffffff", "#9c59d1", "#2c2c2c"]

# Keys: kind; folder; suffix; label; names (for the swatch strip).
#   ramp:  ramp (stops), trail (3 colours l->r), grid (2), rule (3), sky (3), glow (colour, opacity),
#          fringe (right, left), subtitle
#   bands: bands (top->bottom), small (the 16px cut), sky, glow, fringe, subtitle
#   mono:  color, hero=False
PALETTES = {
    "sunset": dict(
        kind="ramp", folder="logo", suffix="", label="sunset",
        ramp=[(0, CREAM), (0.28, AMBER), (0.56, ORANGE), (0.82, MAGENTA), (1, VIOLET)],
        trail=(VIOLET, MAGENTA, AMBER), grid=(ORANGE, VIOLET), rule=(VIOLET, MAGENTA, AMBER),
        sky=MIDNIGHT_SKY, glow=(ORANGE, 0.07), fringe=(MAGENTA, AMBER), subtitle=GOLD,
        names=["cream", "amber", "orange", "magenta", "violet", "gold", "midnight"],
        swatches=[CREAM, AMBER, ORANGE, MAGENTA, VIOLET, GOLD, MIDNIGHT],
    ),
    "valkey": dict(
        kind="ramp", folder="valkey", suffix="-valkey", label="Valkey periwinkle",
        ramp=VALKEY_RAMP, trail=("#2e3a9e", VALKEY, "#e3e8ff"), grid=(VALKEY, "#2e3a9e"),
        rule=("#2e3a9e", VALKEY, "#e3e8ff"), sky=("#1a1f3d", "#0f1226", "#06070f"), glow=(VALKEY, 0.08),
        fringe=(VALKEY, "#e3e8ff"), subtitle="#8f9bd6",
        names=["pale", "light", "valkey", "deep", "ink"], swatches=[c for _, c in VALKEY_RAMP],
    ),
    "pride": dict(
        kind="bands", folder="pride", suffix="-pride", label="pride rainbow", bands=RAINBOW,
        small=RAINBOW3, sky=("#1b1b28", "#0d0d16", "#050508"), glow=("#ffffff", 0.05),
        fringe=(RAINBOW[0], RAINBOW[4]), subtitle="#9a9ab0",
        names=["red", "orange", "yellow", "green", "blue", "violet"],
    ),
    "trans": dict(
        kind="bands", folder="trans", suffix="-trans", label="trans pride", bands=TRANS, small=TRANS,
        sky=MIDNIGHT_SKY, glow=(TRANS[1], 0.06), fringe=(TRANS[1], TRANS[0]), subtitle="#b8b3d0",
        names=["blue", "pink", "white", "pink", "blue"],
    ),
    "bi": dict(
        kind="bands", folder="bi", suffix="-bi", label="bi pride", bands=BI, small=[BI[0], BI[2], BI[3]],
        sky=MIDNIGHT_SKY, glow=(BI[2], 0.09), fringe=(BI[0], "#6d8dff"), subtitle="#b8b3d0",
        names=["magenta", "magenta", "lavender", "blue", "blue"],
    ),
    "lesbian": dict(
        kind="bands", folder="lesbian", suffix="-lesbian", label="lesbian pride", bands=LESBIAN,
        small=[LESBIAN[1], LESBIAN[2], LESBIAN[3]], sky=MIDNIGHT_SKY, glow=(LESBIAN[1], 0.07),
        fringe=(LESBIAN[4], LESBIAN[1]), subtitle="#b8b3d0",
        names=["dark orange", "orange", "white", "pink", "dark rose"],
    ),
    "nonbinary": dict(
        kind="bands", folder="nonbinary", suffix="-nonbinary", label="nonbinary pride", bands=NONBINARY,
        small=NONBINARY, sky=MIDNIGHT_SKY, glow=(NONBINARY[2], 0.08), fringe=(NONBINARY[2], NONBINARY[0]),
        subtitle="#b8b3d0", names=["yellow", "white", "purple", "black"],
    ),
    "mono-light": dict(
        kind="mono", folder="mono", suffix="-mono-light", label="one colour, light on dark", color=PAPER,
        names=["paper"], swatches=[PAPER],
    ),
    "mono-dark": dict(
        kind="mono", folder="mono", suffix="-mono-dark", label="one colour, dark on light", color=MIDNIGHT,
        names=["midnight"], swatches=[MIDNIGHT],
    ),
}

HEX_PATH = "M50 4 L89.8 27 L89.8 73 L50 96 L10.2 73 L10.2 27 Z"
C_PATH = "M89.8 27 L50 4 L10.2 27 L10.2 73 L50 96 L89.8 73 L89.8 61 L69.05 61 A22 22 0 1 1 69.05 39 L89.8 39 Z"

# ------------------------------------------------------------- text -> path

def text_width(text: str, font_path: str, size: float, spacing: float) -> float:
    from fontTools.ttLib import TTFont

    font = TTFont(font_path)
    cmap = font.getBestCmap()
    hmtx = font["hmtx"]
    s = size / font["head"].unitsPerEm
    return sum(hmtx[cmap[ord(ch)]][0] * s for ch in text) + spacing * (len(text) - 1)


def text_path(text: str, font_path: str, size: float, x: float, baseline: float, spacing: float,
              anchor: str = "middle") -> str:
    """Return an SVG path 'd' for text on the given baseline, centred at x (anchor='middle')
    or starting at x (anchor='start')."""
    from fontTools.pens.svgPathPen import SVGPathPen
    from fontTools.pens.transformPen import TransformPen
    from fontTools.ttLib import TTFont

    font = TTFont(font_path)
    cmap = font.getBestCmap()
    gs = font.getGlyphSet()
    hmtx = font["hmtx"]
    s = size / font["head"].unitsPerEm
    names = [cmap[ord(ch)] for ch in text]
    advances = [hmtx[n][0] * s for n in names]
    width = sum(advances) + spacing * (len(text) - 1)
    x = x - width / 2 if anchor == "middle" else x
    d = []
    for name, adv in zip(names, advances):
        pen = SVGPathPen(gs)
        gs[name].draw(TransformPen(pen, (s, 0, 0, -s, x, baseline)))
        cmd = pen.getCommands()
        if cmd:
            d.append(cmd)
        x += adv + spacing
    return " ".join(d)


# ----------------------------------------------------------------- stripes

RAMP_PLANS = {  # (y, height) inside the 100-box, for the 7 / 5 / 3 stripe cuts
    "big": [(4, 8.5), (17.5, 8.5), (31, 8.5), (44.5, 8.5), (58, 8.5), (71.5, 8.5), (85, 11)],
    "mid": [(4, 12), (22.5, 12), (41, 12), (59.5, 12), (78, 18)],
    "small": [(4, 20), (35, 20), (66, 30)],
}


def band_rows(bands, gap: float):
    """Even bands filling y 4..96; gap units of dark between them, last band solid to the base."""
    n = len(bands)
    pitch = 92 / n
    return [(4 + i * pitch, pitch - (gap if i < n - 1 else 0), c) for i, c in enumerate(bands)]


def stripes(name: str, cut: str, p: str) -> str:
    """Stripe rects inside the 100-box, ready to be clipped by the C. cut: big (64px+), mid (32-63), small (16)."""
    pal = PALETTES[name]
    if pal["kind"] == "ramp":
        return "\n".join(f'      <rect x="8" y="{y}" width="84" height="{h}" fill="url(#{p}-ramp)"/>' for y, h in RAMP_PLANS[cut])
    if pal["kind"] == "mono":
        return "\n".join(f'      <rect x="8" y="{y}" width="84" height="{h}" fill="{pal["color"]}"/>' for y, h in RAMP_PLANS[cut])
    if cut == "big":
        rows = band_rows(pal["bands"], 3)  # gaps keep the striped-sun kinship
    elif cut == "mid":
        rows = band_rows(pal["bands"], 0)  # gaps vanish below 64px; touch instead
    else:
        rows = band_rows(pal["small"], 0)
    return "\n".join(f'      <rect x="8" y="{y:.2f}" width="84" height="{h:.2f}" fill="{c}"/>' for y, h, c in rows)


def stripe_polygons(cut: str = "big", step: float = 0.5, scale: float = 1.0, dx: float = 0.0, dy: float = 0.0) -> list[str]:
    """Each piece of the striped keyhole C as its own closed path, with no clipPath,
    mapped from the 100-box by scale and offset.

    Cutters and embroidery digitizers (Cricut, Ink/Stitch) commonly ignore clipPath,
    so the stencil files carry explicit geometry. Method: walk each stripe band in
    thin rows; in each row the C occupies one x-interval (outside the keyhole) or
    two (either side of the circle); chain vertically adjacent intervals into pieces
    and emit each piece as a ring (left boundary going down, right boundary back up).

    Hexagon: pointy-top, points (50,4)/(50,96), shoulders at y=27 and 73, half-width
    39.8. Keyhole: circle r=22 at (50,50) plus a channel y in [39,61] to the right.
    """
    import math

    def hex_half(y):
        if y < 27:
            return 39.8 * (y - 4) / 23
        if y > 73:
            return 39.8 * (96 - y) / 23
        return 39.8

    def intervals(y):
        """x-intervals of the C at height y."""
        hw = hex_half(y)
        if hw <= 0:
            return []
        L, R = 50 - hw, 50 + hw
        d = 22 * 22 - (y - 50) ** 2
        if d <= 0 and not (39 <= y <= 61):
            return [(L, R)]
        cx = math.sqrt(d) if d > 0 else 0.0
        out = [(L, 50 - cx)] if 50 - cx > L else []
        if not (39 <= y <= 61) and 50 + cx < R:
            out.append((50 + cx, R))
        return out

    def fmt(ring):
        return "M" + " L".join(f"{dx + x * scale:.2f} {dy + y * scale:.2f}" for x, y in ring) + " Z"

    paths = []
    for y0, h in RAMP_PLANS[cut]:
        y1 = y0 + h
        n = max(1, int(math.ceil((y1 - y0) / step)))
        ys = [y0 + (y1 - y0) * k / n for k in range(n + 1)]
        # pieces: list of (left_boundary_pts, right_boundary_pts), grown row by row
        pieces, open_pieces = [], []
        for y in ys:
            ivs = intervals(y)
            nxt = []
            used = set()
            for iv in ivs:
                # attach to an open piece whose last interval overlaps this one in x
                match = None
                for k, (lp, rp) in enumerate(open_pieces):
                    if k in used:
                        continue
                    lx, rx = lp[-1][0], rp[-1][0]
                    if iv[0] < rx and iv[1] > lx:
                        match = k
                        break
                if match is None:
                    nxt.append(([(iv[0], y)], [(iv[1], y)]))
                else:
                    used.add(match)
                    lp, rp = open_pieces[match]
                    lp.append((iv[0], y))
                    rp.append((iv[1], y))
                    nxt.append((lp, rp))
            for k, pc in enumerate(open_pieces):
                if k not in used:
                    pieces.append(pc)
            open_pieces = nxt
        pieces.extend(open_pieces)
        for lp, rp in pieces:
            if len(lp) >= 2:
                paths.append(fmt(lp + rp[::-1]))
    return paths


def ramp_def(name: str, p: str) -> str:
    pal = PALETTES[name]
    if pal["kind"] != "ramp":
        return ""
    stops = "".join(f'<stop offset="{o:.2f}" stop-color="{c}"/>' for o, c in pal["ramp"])
    return f'    <linearGradient id="{p}-ramp" gradientUnits="userSpaceOnUse" x1="0" y1="4" x2="0" y2="96">{stops}</linearGradient>'


def prefix(name: str) -> str:
    return {"sunset": "s", "mono-light": "ml", "mono-dark": "md"}.get(name, name[:2])


def mark_svg(name: str, cut: str) -> str:
    p = prefix(name)
    pal = PALETTES[name]
    if pal["kind"] == "mono":
        polys = "\n".join(f'  <path d="{d}"/>' for d in stripe_polygons(cut))
        return f"""<?xml version="1.0" encoding="UTF-8"?>
<!-- Conductress mark: keyhole C, {pal['label']}, {cut} cut. Generated by brand/tools/build.py.
     Stencil-safe: every piece is an explicit closed path in one flat colour; no clipPath,
     gradients, filters or opacity. Suitable for vinyl cutters, embroidery digitizers, one-ink print. -->
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100" width="100" height="100" role="img" aria-label="Conductress mark" fill="{pal['color']}">
{polys}
</svg>
"""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!-- Conductress mark: keyhole C, {pal['label']} palette, {cut} cut. Generated by brand/tools/build.py -->
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100" width="100" height="100" role="img" aria-label="Conductress mark">
  <defs>
{ramp_def(name, p)}
    <clipPath id="{p}-c"><path d="{C_PATH}"/></clipPath>
  </defs>
  <g clip-path="url(#{p}-c)">
{stripes(name, cut, p)}
  </g>
</svg>
"""


# ------------------------------------------------------------------- scene

def scene_defs(name: str, p: str) -> str:
    pal = PALETTES[name]
    kind = pal["kind"]
    if kind == "mono":
        c = pal["color"]
        # everything solid: a stencil has no 40% ink
        return f"""    <linearGradient id="{p}-trail" x1="0" y1="0" x2="1" y2="0">
      <stop offset="0%" stop-color="{c}"/><stop offset="100%" stop-color="{c}"/>
    </linearGradient>
    <linearGradient id="{p}-rule" gradientUnits="userSpaceOnUse" x1="60" y1="0" x2="840" y2="0">
      <stop offset="0%" stop-color="{c}"/><stop offset="100%" stop-color="{c}"/>
    </linearGradient>"""
    s0, s1, s2 = pal["sky"]
    sky = f"""    <radialGradient id="{p}-sky" cx="50%" cy="38%" r="72%">
      <stop offset="0%" stop-color="{s0}"/><stop offset="60%" stop-color="{s1}"/><stop offset="100%" stop-color="{s2}"/>
    </radialGradient>"""
    if kind == "ramp":
        t0, t1, t2 = pal["trail"]
        g0, g1 = pal["grid"]
        r0, r1, r2 = pal["rule"]
        return f"""{sky}
    <linearGradient id="{p}-trail" x1="0" y1="0" x2="1" y2="0">
      <stop offset="0%" stop-color="{t0}"/><stop offset="55%" stop-color="{t1}"/><stop offset="100%" stop-color="{t2}"/>
    </linearGradient>
    <linearGradient id="{p}-streak" x1="0" y1="0" x2="1" y2="0">
      <stop offset="0%" stop-color="{t0}" stop-opacity="0"/><stop offset="60%" stop-color="{t1}" stop-opacity="0.5"/><stop offset="100%" stop-color="{t2}" stop-opacity="0.9"/>
    </linearGradient>
    <linearGradient id="{p}-grid" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="{g0}" stop-opacity="0.5"/><stop offset="100%" stop-color="{g1}" stop-opacity="0.05"/>
    </linearGradient>
    <linearGradient id="{p}-rule" gradientUnits="userSpaceOnUse" x1="60" y1="0" x2="840" y2="0">
      <stop offset="0%" stop-color="{r0}" stop-opacity="0"/><stop offset="25%" stop-color="{r1}"/><stop offset="75%" stop-color="{r2}"/><stop offset="100%" stop-color="{r2}" stop-opacity="0"/>
    </linearGradient>
{ramp_def(name, p)}"""
    n = len(pal["bands"])
    bands = "".join(
        f'<stop offset="{i/n:.4f}" stop-color="{c}"/><stop offset="{(i+1)/n:.4f}" stop-color="{c}"/>' for i, c in enumerate(pal["bands"])
    )
    return f"""{sky}
    <linearGradient id="{p}-trail" gradientUnits="userSpaceOnUse" x1="0" y1="52" x2="0" y2="168">{bands}</linearGradient>
    <linearGradient id="{p}-streak" x1="0" y1="0" x2="1" y2="0">
      <stop offset="0%" stop-color="#ffffff" stop-opacity="0"/><stop offset="100%" stop-color="#ffffff" stop-opacity="0.55"/>
    </linearGradient>
    <linearGradient id="{p}-grid" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="#ffffff" stop-opacity="0.28"/><stop offset="100%" stop-color="#ffffff" stop-opacity="0.03"/>
    </linearGradient>
    <linearGradient id="{p}-rule" gradientUnits="userSpaceOnUse" x1="60" y1="0" x2="840" y2="0">
      <stop offset="0%" stop-color="#ffffff" stop-opacity="0"/><stop offset="50%" stop-color="#d8d8e6"/><stop offset="100%" stop-color="#ffffff" stop-opacity="0"/>
    </linearGradient>"""


def trail_paths(flat: bool = False) -> str:
    """Ten hexagon outlines fading leftward. Normally by opacity AND width; the flat
    (single-colour, stencil-able) version fades by width alone, since thread, vinyl
    and screens cannot do 40% opacity."""
    out = []
    flat_w = [0.5, 0.65, 0.8, 1.0, 1.25, 1.55, 1.9, 2.3, 2.75, 3.2]
    for i in range(10):
        cx = 370 + 17 * i
        if flat:
            attrs = f'stroke-width="{flat_w[i]:.2f}"'
        else:
            op = [0.17, 0.24, 0.32, 0.40, 0.48, 0.57, 0.66, 0.75, 0.84, 0.92][i]
            attrs = f'stroke-width="{1.4 + 0.2 * i:.1f}" opacity="{op}"'
        out.append(
            f'    <path d="M{cx} 52 L{cx+50.2} 81 L{cx+50.2} 139 L{cx} 168 L{cx-50.2} 139 L{cx-50.2} 81 Z" {attrs}/>'
        )
    return "\n".join(out)


STREAKS = """    <line x1="150" y1="72" x2="372" y2="72" stroke-width="2"/>
    <line x1="118" y1="96" x2="356" y2="96" stroke-width="3.4"/>
    <line x1="166" y1="120" x2="366" y2="120" stroke-width="1.6"/>
    <line x1="104" y1="144" x2="350" y2="144" stroke-width="4.2"/>
    <line x1="182" y1="164" x2="360" y2="164" stroke-width="1.4"/>"""

# perspective grid only: no line along the horizon itself, the mountain silhouettes draw it
GRID = """    <g stroke="url(#{p}-grid)" stroke-width="1.1" fill="none">
      <line x1="450" y1="196" x2="-140" y2="330"/><line x1="450" y1="196" x2="60" y2="330"/>
      <line x1="450" y1="196" x2="248" y2="330"/><line x1="450" y1="196" x2="380" y2="330"/>
      <line x1="450" y1="196" x2="520" y2="330"/><line x1="450" y1="196" x2="652" y2="330"/>
      <line x1="450" y1="196" x2="840" y2="330"/><line x1="450" y1="196" x2="1040" y2="330"/>
    </g>"""

STARS = [
    (736, 110, 1.0, .61), (729, 162, 1.6, .71), (636, 25, 1.4, .62), (64, 157, 0.8, .70), (660, 153, 0.9, .46),
    (726, 97, 1.0, .75), (202, 27, 1.2, .81), (87, 137, 1.6, .43), (714, 151, 1.0, .56), (100, 43, 0.8, .58),
    (849, 113, 1.1, .77), (813, 41, 1.1, .61), (47, 34, 1.6, .50), (694, 162, 0.9, .79), (806, 16, 0.8, .38),
    (70, 34, 1.5, .55), (172, 161, 1.5, .36), (842, 49, 1.4, .47), (809, 123, 1.3, .84), (835, 150, 1.3, .53),
    (692, 71, 1.3, .54), (143, 64, 1.1, .72), (157, 51, 1.0, .36), (81, 29, 1.7, .68), (662, 128, 0.8, .72),
    (145, 164, 1.0, .81), (271, 91, 1.6, .38), (817, 94, 1.6, .76), (799, 171, 1.7, .50), (160, 22, 1.6, .66),
    (784, 66, 1.0, .37), (204, 106, 1.1, .83), (133, 102, 1.1, .78), (638, 141, 1.5, .67),
]
MOUNTAINS = (
    "M0 196 L38 186 L74 191 L112 178 L146 189 L184 181 L221 190 L258 183 L292 191 L326 187 L360 194 L385 196 Z",
    "M515 196 L540 194 L568 189 L606 183 L640 190 L676 180 L712 189 L748 176 L786 188 L820 182 L858 190 L900 185 L900 196 Z",
)


def scenery() -> str:
    """Stars either side of the mark, mountain silhouettes on the horizon, black ground.
    Identical to the sign-on's resting frame (brand/motion), which is the rule."""
    stars = "\n".join(f'    <circle cx="{x}" cy="{y}" r="{r}" opacity="{o:.2f}"/>' for x, y, r, o in STARS)
    mts = "\n".join(f'    <path d="{d}"/>' for d in MOUNTAINS)
    return f"""  <g id="stars" fill="{PAPER}">
{stars}
  </g>
  <g id="mountains" fill="#07040c">
{mts}
  </g>
  <rect id="ground" x="0" y="196" width="900" height="144" fill="#000"/>
"""


def not_lead_clip() -> str:
    """Everything except the lead hexagon, as a UNION of six outward half-planes.

    A single evenodd path would be shorter, but cairosvg ignores clip-rule, and a
    mask vanished entirely in earlier renders. Six same-orientation polygons under
    the default nonzero rule work in browsers and cairosvg alike.
    """
    pts = [(540, 52), (590.2, 81), (590.2, 139), (540, 168), (489.8, 139), (489.8, 81)]
    cx, cy, L = 540.0, 110.0, 5000.0
    polys = []
    for i in range(6):
        ax, ay = pts[i]
        bx, by = pts[(i + 1) % 6]
        dx, dy = bx - ax, by - ay
        ln = (dx * dx + dy * dy) ** 0.5
        dx, dy = dx / ln, dy / ln
        nx, ny = dy, -dx  # outward normal: the one pointing away from the centre
        mx, my = (ax + bx) / 2 - cx, (ay + by) / 2 - cy
        if nx * mx + ny * my < 0:
            nx, ny = -nx, -ny
        a2 = (ax - dx * L, ay - dy * L)
        b2 = (bx + dx * L, by + dy * L)
        quad = [a2, b2, (b2[0] + nx * L, b2[1] + ny * L), (a2[0] + nx * L, a2[1] + ny * L)]
        polys.append('<polygon points="' + " ".join(f"{x:.1f},{y:.1f}" for x, y in quad) + '"/>')
    return "".join(polys)


# ------------------------------------------------------------- type groups

def wordmark_group(name: str, p: str, word_d: str) -> str:
    pal = PALETTES[name]
    if pal["kind"] == "mono":
        return f"""  <g id="wordmark">
    <path d="{word_d}" fill="{pal['color']}"/>
  </g>"""
    right, left = pal["fringe"]
    op = 0.75 if name == "sunset" else 0.7
    return f"""  <g id="wordmark">
    <path d="{word_d}" transform="translate(2 0)" fill="{right}" opacity="{op}"/>
    <path d="{word_d}" transform="translate(-2 0)" fill="{left}" opacity="{op}"/>
    <path d="{word_d}" fill="{PAPER}" filter="url(#{p}-soft)"/>
  </g>"""


def subtitle_group(name: str, p: str, sub_d: str, rules: bool = True) -> str:
    pal = PALETTES[name]
    if pal["kind"] == "mono":
        c = pal["color"]
        # solid rules end where the gradient version has visibly faded out
        return f"""  <g id="subtitle">
    <line x1="240" y1="276" x2="660" y2="276" stroke="{c}" stroke-width="2.2"/>
    <line x1="290" y1="284" x2="610" y2="284" stroke="{c}" stroke-width="1.1"/>
    <path d="{sub_d}" fill="{c}"/>
  </g>"""
    # the hero omits the rules: over the grid horizon they cover the scene
    rule_lines = "" if not rules else f"""
    <line x1="170" y1="276" x2="730" y2="276" stroke="url(#{p}-rule)" stroke-width="2.2"/>
    <line x1="230" y1="284" x2="670" y2="284" stroke="url(#{p}-rule)" stroke-width="1.1"/>"""
    return f"""  <g id="subtitle">{rule_lines}
    <path d="{sub_d}" fill="{pal['subtitle']}"/>
  </g>"""


# ---------------------------------------------------------------- lockups

def lockup_svg(name: str, opaque: bool, word_d: str, sub_d: str, trail: bool = True) -> str:
    pal = PALETTES[name]
    flat = pal["kind"] == "mono"
    p = prefix(name) + ("h" if opaque else "l") + ("" if trail else "p")
    kind = "hero" if opaque else ("lockup" if trail else "lockup, no trail")
    background = ""
    if opaque:
        glow_c, glow_o = pal["glow"]
        background = f"""  <g id="background">
    <rect width="900" height="340" fill="url(#{p}-sky)"/>
    <circle cx="450" cy="110" r="150" fill="{glow_c}" opacity="{glow_o}"/>
  </g>
{scenery()}  <g id="horizon" opacity="0.7">
{GRID.format(p=p)}
  </g>
"""
    streaks = "" if flat else f"""  <g id="speedstreaks" stroke="url(#{p}-streak)" stroke-linecap="round">
{STREAKS}
  </g>

"""
    trail_g = "" if not trail else f"""  <g id="trail" fill="none" stroke="url(#{p}-trail)" stroke-linejoin="round" clip-path="url(#{p}-notlead)">
{trail_paths(flat)}
  </g>

"""
    lead_filter = "" if flat else f' filter="url(#{p}-bloom)"'
    note = (
        "     Single colour, no opacity or gradients anywhere: safe for stencils, vinyl, embroidery and one-ink print."
        if flat
        else "     Type is outlined (Nimbus Sans Bold / DejaVu Sans Mono); the trail is clipped out of the lead\n"
        "     hexagon so the keyhole and stripe gaps show whatever is behind the logo."
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!-- Conductress {kind}, {pal['label']} palette. Generated by brand/tools/build.py; edit that, not this.
{note} -->
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 900 340" width="900" height="340" role="img" aria-label="Conductress">
  <defs>
{scene_defs(name, p)}
    <filter id="{p}-bloom" x="-60%" y="-60%" width="220%" height="220%">
      <feGaussianBlur stdDeviation="7" result="b1"/><feMerge><feMergeNode in="b1"/><feMergeNode in="SourceGraphic"/></feMerge>
    </filter>
    <filter id="{p}-soft" x="-40%" y="-40%" width="180%" height="180%">
      <feGaussianBlur stdDeviation="3" result="b2"/><feMerge><feMergeNode in="b2"/><feMergeNode in="SourceGraphic"/></feMerge>
    </filter>
    <!-- everything except the lead hexagon: the trail is drawn through this so nothing sits behind the C -->
    <clipPath id="{p}-notlead">{not_lead_clip()}</clipPath>
    <clipPath id="{p}-c"><path d="{C_PATH}"/></clipPath>
    <symbol id="{p}-cmark" viewBox="0 0 100 100">
      <g clip-path="url(#{p}-c)">
{stripes(name, "big", p)}
      </g>
    </symbol>
  </defs>

{background}{streaks}{trail_g}  <g id="lead"{lead_filter}>
    <use href="#{p}-cmark" x="477" y="47" width="126" height="126"/>
  </g>

{wordmark_group(name, p, word_d)}

{subtitle_group(name, p, sub_d, rules=not opaque)}
</svg>
"""


def flat_lockup_svg(name: str, word_d: str, sub_d: str, trail: bool) -> str:
    """Single-colour lockup with no defs at all: explicit C polygons, plain strokes,
    outlined type. The trail fades by stroke width only; the plain version drops it."""
    pal = PALETTES[name]
    c = pal["color"]
    # the C at lockup size: the 100-box maps to a 126-unit box. With the trail it sits
    # at (477,47) so the trail has room on the left; without it, it centres over the wordmark.
    mark_x = 477 if trail else 387
    mark = "\n".join(f'    <path d="{d}"/>' for d in stripe_polygons("big", scale=1.26, dx=mark_x, dy=47))
    # trail: each outline is emitted only where it lies outside the lead hexagon
    # (geometry, not clipPath), so nothing is cut or stitched twice
    trail_g = ""
    if trail:
        flat_w = [0.5, 0.65, 0.8, 1.0, 1.25, 1.55, 1.9, 2.3, 2.75, 3.2]
        segs = []
        for i in range(10):
            cx = 370 + 17 * i
            hexpts = [(cx, 52), (cx + 50.2, 81), (cx + 50.2, 139), (cx, 168), (cx - 50.2, 139), (cx - 50.2, 81)]
            segs.append(f'    <path d="{_outline_outside_lead(hexpts)}" stroke-width="{flat_w[i]:.2f}"/>')
        trail_g = f"""  <g id="trail" fill="none" stroke="{c}" stroke-linejoin="round" stroke-linecap="round">
{chr(10).join(segs)}
  </g>
"""
    kind = "lockup" if trail else "lockup, no trail"
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!-- Conductress {kind}, {pal['label']}. Generated by brand/tools/build.py; edit that, not this.
     Stencil-safe: one flat colour, explicit closed paths, outlined type; no clipPath, gradients,
     filters or opacity. For vinyl, embroidery, screen print, engraving. -->
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 900 340" width="900" height="340" role="img" aria-label="Conductress">
{trail_g}  <g id="lead" fill="{c}">
{mark}
  </g>
  <g id="wordmark" fill="{c}">
    <path d="{word_d}"/>
  </g>
  <g id="subtitle" fill="{c}" stroke="{c}">
    <line x1="240" y1="276" x2="660" y2="276" stroke-width="2.2"/>
    <line x1="290" y1="284" x2="610" y2="284" stroke-width="1.1"/>
    <path d="{sub_d}" stroke="none"/>
  </g>
</svg>
"""


def _outline_outside_lead(hexpts) -> str:
    """The part of a trail hexagon's outline that lies outside the lead hexagon, as one
    or more open polylines. The lead is the pointy-top hex centred (540,110), half-width
    50.2, points at y=52/168, shoulders at y=81/139."""
    import math

    def inside_lead(x, y):
        hw = 50.2 * (y - 52) / 29 if y < 81 else (50.2 * (168 - y) / 29 if y > 139 else 50.2)
        return abs(x - 540) < hw - 1e-6

    # sample the closed outline finely and keep runs of points outside the lead
    pts = []
    for (ax, ay), (bx, by) in zip(hexpts, hexpts[1:] + hexpts[:1]):
        n = max(2, int(math.hypot(bx - ax, by - ay) / 0.5))
        pts += [(ax + (bx - ax) * k / n, ay + (by - ay) * k / n) for k in range(n)]
    flags = [not inside_lead(x, y) for x, y in pts]
    if all(flags):
        return "M" + " L".join(f"{x:.2f} {y:.2f}" for x, y in hexpts) + " Z"
    # rotate so the sequence starts at an inside point, then collect outside runs
    start = flags.index(False)
    pts = pts[start:] + pts[:start]
    flags = flags[start:] + flags[:start]
    runs, cur = [], []
    for pt, f in zip(pts, flags):
        if f:
            cur.append(pt)
        elif cur:
            runs.append(cur)
            cur = []
    if cur:
        runs.append(cur)
    return " ".join("M" + " L".join(f"{x:.2f} {y:.2f}" for x, y in run) for run in runs)


def horizontal_svg(name: str) -> str:
    """Mark on the left, wordmark / rule / subtitle stacked to its right and left-aligned,
    vertically centred on the mark. The layout of the terminal (MOTD) lockup. No trail.

    Canvas 860x180. Mark: 160-unit box at (40,10), so the hexagon spans x 56..184, y 16..164,
    centre (120,90). Type column starts at x=232."""
    pal = PALETTES[name]
    p = prefix(name) + "z"
    flat = pal["kind"] == "mono"
    tx = 232
    word_w = text_width("CONDUCTRESS", FONT_SANS, 54, 13)
    word_d = text_path("CONDUCTRESS", FONT_SANS, 54, tx, 96, 13, anchor="start")
    sub_d = text_path("ONLY DATA IS REAL", FONT_MONO, 15, tx, 150, 8, anchor="start")
    rule_r = tx + word_w
    if flat:
        c = pal["color"]
        mark = "\n".join(f'    <path d="{d}"/>' for d in stripe_polygons("big", scale=1.6, dx=40, dy=10))
        return f"""<?xml version="1.0" encoding="UTF-8"?>
<!-- Conductress horizontal lockup, {pal['label']}. Generated by brand/tools/build.py; edit that, not this.
     Stencil-safe: one flat colour, explicit closed paths, outlined type; no clipPath, gradients,
     filters or opacity. -->
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 860 180" width="860" height="180" role="img" aria-label="Conductress">
  <g id="mark" fill="{c}">
{mark}
  </g>
  <g id="wordmark" fill="{c}">
    <path d="{word_d}"/>
  </g>
  <g id="subtitle" fill="{c}" stroke="{c}">
    <line x1="{tx}" y1="114" x2="{rule_r:.1f}" y2="114" stroke-width="2.2"/>
    <line x1="{tx}" y1="122" x2="{rule_r - 60:.1f}" y2="122" stroke-width="1.1"/>
    <path d="{sub_d}" stroke="none"/>
  </g>
</svg>
"""
    # coloured: the mark symbol, the fringed wordmark, and a rule that starts solid at the
    # text edge and fades to the right (the stacked lockup's rule fades at both ends)
    if pal["kind"] == "ramp":
        _, r1, r2 = pal["rule"]
        hrule = f'<stop offset="0%" stop-color="{r1}"/><stop offset="55%" stop-color="{r2}"/><stop offset="100%" stop-color="{r2}" stop-opacity="0"/>'
    else:
        hrule = '<stop offset="0%" stop-color="#d8d8e6"/><stop offset="100%" stop-color="#d8d8e6" stop-opacity="0"/>'
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!-- Conductress horizontal lockup, {pal['label']} palette. Generated by brand/tools/build.py; edit that, not this.
     Type is outlined (Nimbus Sans Bold / DejaVu Sans Mono). Transparent background, dark grounds only. -->
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 860 180" width="860" height="180" role="img" aria-label="Conductress">
  <defs>
{scene_defs(name, p)}
    <linearGradient id="{p}-hrule" gradientUnits="userSpaceOnUse" x1="{tx}" y1="0" x2="{rule_r:.1f}" y2="0">{hrule}</linearGradient>
    <filter id="{p}-bloom" x="-60%" y="-60%" width="220%" height="220%">
      <feGaussianBlur stdDeviation="7" result="b1"/><feMerge><feMergeNode in="b1"/><feMergeNode in="SourceGraphic"/></feMerge>
    </filter>
    <filter id="{p}-soft" x="-40%" y="-40%" width="180%" height="180%">
      <feGaussianBlur stdDeviation="3" result="b2"/><feMerge><feMergeNode in="b2"/><feMergeNode in="SourceGraphic"/></feMerge>
    </filter>
    <clipPath id="{p}-c"><path d="{C_PATH}"/></clipPath>
    <symbol id="{p}-cmark" viewBox="0 0 100 100">
      <g clip-path="url(#{p}-c)">
{stripes(name, "big", p)}
      </g>
    </symbol>
  </defs>
  <g id="mark" filter="url(#{p}-bloom)">
    <use href="#{p}-cmark" x="40" y="10" width="160" height="160"/>
  </g>
{wordmark_group(name, p, word_d)}
  <g id="subtitle">
    <line x1="{tx}" y1="114" x2="{rule_r:.1f}" y2="114" stroke="url(#{p}-hrule)" stroke-width="2.2"/>
    <line x1="{tx}" y1="122" x2="{rule_r - 60:.1f}" y2="122" stroke="url(#{p}-hrule)" stroke-width="1.1"/>
    <path d="{sub_d}" fill="{pal['subtitle']}"/>
  </g>
</svg>
"""


def wordmark_svg(name: str, word_d: str) -> str:
    p = prefix(name) + "w"
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!-- Conductress wordmark, outlined (Nimbus Sans Bold 54/13), {PALETTES[name]['label']}. Generated by brand/tools/build.py -->
<svg xmlns="http://www.w3.org/2000/svg" viewBox="130 200 640 72" width="640" height="72" role="img" aria-label="CONDUCTRESS">
  <defs>
    <filter id="{p}-soft" x="-40%" y="-40%" width="180%" height="180%">
      <feGaussianBlur stdDeviation="3" result="b2"/><feMerge><feMergeNode in="b2"/><feMergeNode in="SourceGraphic"/></feMerge>
    </filter>
  </defs>
{wordmark_group(name, p, word_d)}
</svg>
"""


def palette_svg() -> str:
    """Swatch strip for the guide: one row per palette."""
    rows_data = []
    for pal in PALETTES.values():
        colours = pal.get("swatches") or pal["bands"]
        rows_data.append((pal["label"], list(zip(pal["names"], colours))))
    w, rh = 100, 100
    h = rh * len(rows_data)
    out = []
    for r, (label, row) in enumerate(rows_data):
        y0 = r * rh
        out.append(f'  <path d="{text_path(label, FONT_MONO, 11, 60, y0 + 14, 0)}" fill="#8f83b5"/>')
        for i, (nm, hexv) in enumerate(row):
            x, y = i * w, y0 + 20
            out.append(f'  <rect x="{x}" y="{y}" width="{w}" height="46" fill="{hexv}"/>')
            out.append(f'  <path d="{text_path(nm, FONT_MONO, 10, x + w / 2, y + 60, 0)}" fill="#c9bfe6"/>')
            out.append(f'  <path d="{text_path(hexv, FONT_MONO, 10, x + w / 2, y + 73, 0)}" fill="#8f83b5"/>')
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!-- Conductress palettes. Generated by brand/tools/build.py -->
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 700 {h}" width="700" height="{h}" role="img" aria-label="Conductress colour palettes">
  <rect width="700" height="{h}" fill="#0b0812"/>
{chr(10).join(out)}
</svg>
"""


# ------------------------------------------------------------------ build

def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    print("wrote", path.relative_to(ROOT.parent))


def png(svg_path: Path, out: Path, width: int) -> None:
    import cairosvg

    cairosvg.svg2png(url=str(svg_path), write_to=str(out), output_width=width)
    print("wrote", out.relative_to(ROOT.parent))


def ico(mark16: Path, mark32: Path, out: Path) -> None:
    """16 + 32 + 48 in one .ico. Pillow drops any requested size larger than the
    base image, so the 48px frame must be the base and the smaller ones appended."""
    import cairosvg
    from PIL import Image

    def frame(src, size):
        buf = io.BytesIO(cairosvg.svg2png(url=str(src), output_width=size, output_height=size))
        return Image.open(buf).convert("RGBA")

    f48, f32, f16 = frame(mark32, 48), frame(mark32, 32), frame(mark16, 16)
    f48.save(out, format="ICO", sizes=[(48, 48), (32, 32), (16, 16)], append_images=[f32, f16])
    check = Image.open(out)
    assert sorted(check.info["sizes"]) == [(16, 16), (32, 32), (48, 48)], check.info["sizes"]
    print("wrote", out.relative_to(ROOT.parent))


def main() -> None:
    for f in (FONT_SANS, FONT_MONO):
        if not os.path.exists(f):
            sys.exit(f"font not found: {f} -- edit FONT_SANS/FONT_MONO at the top of this file")
    word_d = text_path("CONDUCTRESS", FONT_SANS, 54, 450, 252, 13)
    sub_d = text_path("ONLY DATA IS REAL", FONT_MONO, 15, 450, 312, 8)
    write(ROOT / "palette.svg", palette_svg())

    for name, pal in PALETTES.items():
        d = ROOT / pal["folder"]
        sfx = pal["suffix"]
        if pal["kind"] != "mono":
            write(d / f"conductress-hero{sfx}.svg", lockup_svg(name, True, word_d, sub_d))
            png(d / f"conductress-hero{sfx}.svg", d / f"conductress-hero{sfx}.png", 1800)
        if pal["kind"] == "mono":
            write(d / f"conductress-lockup{sfx}.svg", flat_lockup_svg(name, word_d, sub_d, trail=True))
            write(d / f"conductress-lockup{sfx}-plain.svg", flat_lockup_svg(name, word_d, sub_d, trail=False))
        else:
            write(d / f"conductress-lockup{sfx}.svg", lockup_svg(name, False, word_d, sub_d))
        write(d / f"conductress-lockup{sfx}-horizontal.svg", horizontal_svg(name))
        write(d / f"conductress-wordmark{sfx}.svg", wordmark_svg(name, word_d))
        write(d / f"conductress-mark{sfx}.svg", mark_svg(name, "big"))
        write(d / f"conductress-mark{sfx}-32.svg", mark_svg(name, "mid"))
        write(d / f"conductress-mark{sfx}-16.svg", mark_svg(name, "small"))
        png(d / f"conductress-mark{sfx}.svg", d / f"conductress-mark{sfx}-512.png", 512)
        png(d / f"conductress-mark{sfx}.svg", d / f"conductress-mark{sfx}-180.png", 180)
        png(d / f"conductress-mark{sfx}-32.svg", d / f"conductress-mark{sfx}-32.png", 32)
        png(d / f"conductress-mark{sfx}-16.svg", d / f"conductress-mark{sfx}-16.png", 16)
        ico(d / f"conductress-mark{sfx}-16.svg", d / f"conductress-mark{sfx}-32.svg", d / f"favicon{sfx}.ico")


if __name__ == "__main__":
    main()
