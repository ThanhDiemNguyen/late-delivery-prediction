"""Draw the diagrams of the README from their typed IR and their layout. Stdlib only.

Each diagram has two sources in docs/diagrams/:
  <name>.json        the topology, in ak-diagram's typed IR for architecture diagrams: the units, the boundaries
                     and the arrows. An arrow label that starts with a flow letter and number ("B3 new export
                     dates") is drawn in its flow's colour with a numbered badge; any other label is a plain
                     arrow. " | " breaks a label into lines.
  <name>_layout.py   LAYOUT: the canvas, the panels, a box per unit, the corner points of every arrow and where
                     its numbered badge sits.

The script writes docs/images/architecture/<name>.svg and <name>.png (through headless Chrome, when found). It
checks the layout first: an arrow may not cross a box it does not start or end at, and a badge or label may not
overlap a box, another label, or another arrow.

Usage: python3 docs/diagrams/draw.py [name ...]   (default: every <name>.json that has a <name>_layout.py)
"""
import importlib
import json
import math
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from xml.sax.saxutils import escape

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "images" / "architecture"
SANS = "'Inter', 'Helvetica Neue', Helvetica, Arial, sans-serif"
MONO = "'JetBrains Mono', 'SF Mono', Menlo, Consolas, monospace"
INK, INK2, MUTED, LINE = "#1f2633", "#465165", "#8790a2", "#ccd1da"
PAGE, PANEL, PANEL_LINE = "#f6f7f9", "#eceff4", "#d6dbe4"
STORE_FILL, STORE_LINE = "#ecf1f9", "#8497ba"
NOVEL_FILL, NOVEL_LINE = "#fcedeb", "#c92a2a"   # a novel idea: a technique the course does not teach
PLAIN = "#5c677d"                                # an arrow that belongs to no numbered flow
CHROME = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "google-chrome", "chromium", "chromium-browser",
]
LABEL = re.compile(r"^([A-Z])(\d+)\s+(.*)$")
CHAR_W = {"sans": 6.3, "mono": 6.6}   # average glyph width at 11 px, for the overlap check


def text_width(text, size=11.0, family="sans"):
    """Estimate the width of a line of text in pixels."""
    return len(text) * CHAR_W[family] * size / 11.0


class Rect:
    """An axis-aligned rectangle used for drawing and for the overlap checks."""

    def __init__(self, x, y, w, h, name=""):
        """Store the corner, the size and a name for the problem messages."""
        self.x, self.y, self.w, self.h, self.name = x, y, w, h, name

    def grow(self, d):
        """Return the rectangle grown by d on every side."""
        return Rect(self.x - d, self.y - d, self.w + 2 * d, self.h + 2 * d, self.name)

    def overlaps(self, other):
        """Return True when the two rectangles share an area."""
        return (self.x < other.x + other.w and other.x < self.x + self.w
                and self.y < other.y + other.h and other.y < self.y + self.h)

    def cut_by(self, a, b):
        """Return True when the horizontal or vertical segment a-b passes through the rectangle's inside."""
        (x1, y1), (x2, y2) = a, b
        if y1 == y2:
            lo, hi = sorted((x1, x2))
            return self.y < y1 < self.y + self.h and lo < self.x + self.w and self.x < hi
        lo, hi = sorted((y1, y2))
        return self.x < x1 < self.x + self.w and lo < self.y + self.h and self.y < hi


def glyph(kind, x, y, color):
    """Return a small outline icon (18 x 18) for a unit's kind, drawn at its top-left corner x, y."""
    s = f'fill="none" stroke="{color}" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"'
    if kind == "database":
        return (f'<g {s}><ellipse cx="{x+9}" cy="{y+4}" rx="7" ry="2.6"/>'
                f'<path d="M{x+2} {y+4}v10c0 1.4 3.1 2.6 7 2.6s7-1.2 7-2.6V{y+4}"/>'
                f'<path d="M{x+2} {y+9}c0 1.4 3.1 2.6 7 2.6s7-1.2 7-2.6"/></g>')
    if kind == "storage":
        return (f'<g {s}><path d="M{x+2} {y+4}h14l-1.6 12.5h-10.8z"/>'
                f'<ellipse cx="{x+9}" cy="{y+4}" rx="7" ry="1.8"/></g>')
    if kind == "queue":
        return "".join(f'<rect x="{x+1}" y="{y+2+i*5.2}" width="16" height="3.4" rx="1.2" {s}/>' for i in range(3))
    if kind == "compute":
        return (f'<g {s}><rect x="{x+2}" y="{y+2}" width="14" height="14" rx="2.5"/>'
                f'<path d="M{x+6} {y+6}h6v6h-6z"/><path d="M{x+6} {y}v2M{x+12} {y}v2M{x+6} {y+16}v2M{x+12} {y+16}v2'
                f'M{x} {y+6}h2M{x} {y+12}h2M{x+16} {y+6}h2M{x+16} {y+12}h2"/></g>')
    if kind == "scheduler":
        return (f'<g {s}><circle cx="{x+9}" cy="{y+9}" r="7.5"/><path d="M{x+9} {y+4.5}v4.5l3 2"/></g>')
    if kind == "ui":
        return (f'<g {s}><rect x="{x+1}" y="{y+2}" width="16" height="13" rx="2"/><path d="M{x+1} {y+6}h16"/>'
                f'<path d="M{x+6} {y+18}h6"/></g>')
    if kind == "catalog":
        return (f'<g {s}><circle cx="{x+4}" cy="{y+9}" r="2.6"/><circle cx="{x+14}" cy="{y+4}" r="2.6"/>'
                f'<circle cx="{x+14}" cy="{y+14}" r="2.6"/><path d="M{x+6.4} {y+8}l5.2-2.8M{x+6.4} {y+10}l5.2 2.8"/></g>')
    if kind == "search":
        return f'<g {s}><circle cx="{x+7.5}" cy="{y+7.5}" r="5.5"/><path d="M{x+11.5} {y+11.5}l5 5"/></g>'
    if kind == "process":
        return (f'<g {s}><rect x="{x+1}" y="{y+2}" width="16" height="14" rx="2"/>'
                f'<path d="M{x+4.5} {y+7}l3 2-3 2M{x+9} {y+12}h4"/></g>')
    if kind == "user":
        return (f'<g {s}><circle cx="{x+9}" cy="{y+5.5}" r="3.6"/>'
                f'<path d="M{x+2} {y+17}c0-4 3.1-6.5 7-6.5s7 2.5 7 6.5"/></g>')
    if kind == "stream":
        return (f'<g {s}><path d="M{x+1} {y+5}c4 0 4 4 8 4s4-4 8-4"/><path d="M{x+1} {y+10}c4 0 4 4 8 4s4-4 8-4"/></g>')
    return ""


def draw(name):
    """Draw one diagram and return the number of layout problems found."""
    ir = json.loads((HERE / f"{name}.json").read_text())
    lay = importlib.import_module(f"{name}_layout").LAYOUT
    flows = lay["flows"]
    units = {c["id"]: c for c in ir["components"]}
    W, H = lay["canvas"]
    svg, problems = [], []
    rects = {}
    svg.append(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
               f'font-family="{SANS}"><title>{escape(ir["meta"]["title"])}</title>')
    heads = {**{k: v["color"] for k, v in flows.items()}, "plain": PLAIN}
    svg.append("<defs>" + "".join(
        f'<marker id="head-{k}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
        f'orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="{c}"/></marker>'
        for k, c in heads.items()) + "</defs>")
    svg.append(f'<rect width="{W}" height="{H}" fill="{PAGE}"/>')

    eyebrow, title = lay["eyebrow"], ir["meta"]["title"]
    svg.append(f'<text x="32" y="46" font-family="{MONO}" font-size="12.5" letter-spacing="2.4" fill="{INK2}">'
               f'{escape(eyebrow)}</text>')
    svg.append(f'<text x="32" y="78" font-size="23" fill="{INK}">{escape(title)}</text>')
    x0 = W - 32
    for kind, text in reversed(lay.get("keys", [])):
        x0 -= text_width(text, 11.5) + 8
        svg.append(f'<text x="{x0:.1f}" y="50" font-size="11.5" fill="{INK2}">{escape(text)}</text>')
        x0 -= 34
        if kind == "arrow":
            svg.append(f'<path d="M{x0} 46h26" stroke="{PLAIN}" stroke-width="1.7" marker-end="url(#head-plain)"/>')
        else:
            fill, line = (STORE_FILL, STORE_LINE) if kind == "store" else (NOVEL_FILL, NOVEL_LINE)
            svg.append(f'<rect x="{x0}" y="38" width="26" height="16" rx="3" fill="{fill}" stroke="{line}"/>')
        x0 -= 28

    for p in lay["panels"]:
        dash = ' stroke-dasharray="6 5"' if p.get("dashed") else ""
        fill = "#f9fafb" if p.get("dashed") else PANEL
        svg.append(f'<rect x="{p["x"]}" y="{p["y"]}" width="{p["w"]}" height="{p["h"]}" rx="12" fill="{fill}" '
                   f'stroke="{PANEL_LINE}" stroke-width="1.2"{dash}/>')
        ly0 = p["y"] + p["h"] - 12 if p.get("label_at") == "bottom" else p["y"] + 24
        svg.append(f'<text x="{p["x"]+16}" y="{ly0}" font-family="{MONO}" font-size="11.5" letter-spacing="1.6" '
                   f'font-weight="600" fill="{INK2}">{escape(p["label"])}'
                   f'<tspan dx="12" font-weight="400" letter-spacing="0.4" fill="{MUTED}">{escape(p.get("note", ""))}</tspan>'
                   f'</text>')

    for uid, b in lay["boxes"].items():
        u = units[uid]
        x, y, w, h = b["x"], b["y"], b["w"], b["h"]
        rects[uid] = Rect(x, y, w, h, uid)
        style = b.get("style", "unit")
        fill, stroke = {"store": (STORE_FILL, STORE_LINE), "novel": (NOVEL_FILL, NOVEL_LINE)}.get(style, ("#ffffff", LINE))
        dash = ' stroke-dasharray="5 4"' if style == "support" else ""
        svg.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="7" fill="{fill}" stroke="{stroke}" '
                   f'stroke-width="1.3"{dash}/>')
        tag, accent = b["tag"], NOVEL_LINE if style == "novel" else INK2
        tw = text_width(tag, 9.5, "mono") + 0.6 * len(tag) + 12   # 0.6: the letter spacing
        svg.append(f'<rect x="{x+8}" y="{y+8}" width="{tw:.1f}" height="16" rx="3" fill="#ffffff" '
                   f'stroke="{NOVEL_LINE if style == "novel" else LINE}"/>'
                   f'<text x="{x+14}" y="{y+19.5}" font-family="{MONO}" font-size="9.5" letter-spacing="0.6" '
                   f'fill="{accent}">{escape(tag)}</text>')
        svg.append(glyph(b.get("icon", ""), x + w - 27, y + 8, INK2))
        lines = b.get("lines", [])
        cy = y + h / 2 - (len(lines) * 14) / 2 + 9
        svg.append(f'<text x="{x+w/2}" y="{cy:.1f}" text-anchor="middle" font-size="15" '
                   f'fill="{NOVEL_LINE if style == "novel" else INK}">{escape(u["label"])}</text>')
        for i, line in enumerate(lines):
            svg.append(f'<text x="{x+w/2}" y="{cy+18+i*14:.1f}" text-anchor="middle" font-family="{MONO}" '
                       f'font-size="10.5" fill="{INK2}">{escape(line)}</text>')

    label_rects, top, segments = [], [], []
    edges = {c["id"]: c for c in ir["connections"]}
    missing = set(edges) - set(lay["edges"])
    if missing:
        problems.append(f"arrows without a layout: {sorted(missing)}")
    for eid, e in lay["edges"].items():
        c = edges[eid]
        m = LABEL.match(c["label"])
        if m and m.group(1) in flows:
            letter, number, text = m.group(1), m.group(2), m.group(3)
            color = flows[letter]["color"]
        else:
            letter, number, text, color = "plain", None, c["label"], PLAIN
        pts = e["route"]
        d = "M" + " L".join(f"{px} {py}" for px, py in pts)
        dash = ' stroke-dasharray="7 5"' if e.get("dashed") else ""
        svg.append(f'<path d="{d}" fill="none" stroke="{PANEL}" stroke-width="7" stroke-linejoin="round"/>')
        svg.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="1.7" stroke-linejoin="round"{dash} '
                   f'marker-end="url(#head-{letter})"/>')
        for a, b2 in zip(pts, pts[1:]):
            segments.append((eid, a, b2))
            if a[0] != b2[0] and a[1] != b2[1]:
                problems.append(f"{eid}: segment {a}-{b2} is not horizontal or vertical")
                continue
            for rid, r in rects.items():
                if rid in (c["from"], c["to"]):
                    continue
                if r.cut_by(a, b2):
                    problems.append(f"{eid}: crosses box {rid}")
        bx, by = e["badge"]
        if number is None:   # no badge: the text starts (right) or ends (left) at the given point
            bx += {"right": -16, "left": 16}.get(e.get("text", "right"), 0)
        badge = f"{letter}{number}"
        if number is not None:
            top.append(f'<circle cx="{bx}" cy="{by}" r="11.5" fill="#ffffff" stroke="{color}" stroke-width="1.6"/>'
                   f'<text x="{bx}" y="{by+3.6}" text-anchor="middle" font-family="{MONO}" font-size="9.5" '
                   f'font-weight="700" fill="{color}">{badge}</text>')
        side = e.get("text", "right")
        lines = text.split(" | ")
        tw = max(text_width(t, 11.5) for t in lines)
        th = 14 * len(lines)
        if side == "right":
            tx, ty, anchor = bx + 16, by + 4 - (len(lines) - 1) * 7, "start"
            lr = Rect(tx, ty - 11, tw, th)
        elif side == "left":
            tx, ty, anchor = bx - 16, by + 4 - (len(lines) - 1) * 7, "end"
            lr = Rect(tx - tw, ty - 11, tw, th)
        elif side == "above":
            tx, ty, anchor = bx, by - 18 - (len(lines) - 1) * 14, "middle"
            lr = Rect(tx - tw / 2, ty - 11, tw, th)
        else:
            tx, ty, anchor = bx, by + 27, "middle"
            lr = Rect(tx - tw / 2, ty - 11, tw, th)
        for i, t in enumerate(lines):
            top.append(f'<text x="{tx}" y="{ty+i*14}" text-anchor="{anchor}" font-size="11.5" fill="{INK2}">'
                       f'{escape(t)}</text>')
        lr.name = f"label {eid}"
        br = Rect(bx - 11.5, by - 11.5, 23, 23, f"badge {eid}")
        for r in (lr, br) if number is not None else (lr,):
            for rid, box in rects.items():
                if r.overlaps(box):
                    problems.append(f"{r.name} overlaps box {rid}")
            for other in label_rects:
                if r.overlaps(other):
                    problems.append(f"{r.name} overlaps {other.name}")
        lr.edge = br.edge = eid
        label_rects += [lr, br] if number is not None else [lr]

    for r in label_rects:
        for eid, a, b2 in segments:
            if eid != r.edge and r.cut_by(a, b2):
                problems.append(f"{r.name} is crossed by arrow {eid}")
    svg += top

    if "legend" in lay:
        ly = lay["legend"]["y"]
        svg.append(f'<text x="32" y="{ly}" font-family="{MONO}" font-size="11.5" letter-spacing="1.6" font-weight="600" '
                   f'fill="{INK2}">FLOWS</text>')
        for i, (k, f) in enumerate(flows.items()):
            col, row = i % 2, i // 2
            x0, y0 = 32 + col * 640, ly + 34 + row * 34
            svg.append(f'<path d="M{x0} {y0}h46" stroke="{f["color"]}" stroke-width="1.7" marker-end="url(#head-{k})"/>'
                       f'<circle cx="{x0+18}" cy="{y0}" r="11.5" fill="#ffffff" stroke="{f["color"]}" stroke-width="1.6"/>'
                       f'<text x="{x0+18}" y="{y0+3.6}" text-anchor="middle" font-family="{MONO}" font-size="9.5" '
                       f'font-weight="700" fill="{f["color"]}">{k}</text>'
                       f'<text x="{x0+62}" y="{y0+5}" font-size="14" fill="{INK}">{escape(f["name"])}</text>'
                       f'<text x="{x0+62+text_width(f["name"], 14)+16}" y="{y0+5}" font-size="11.5" fill="{MUTED}">'
                       f'{escape(f["who"])}</text>')
        for i, note in enumerate(lay["legend"]["notes"]):
            svg.append(f'<text x="1320" y="{ly+30+i*17}" font-size="11.5" fill="{INK2}">{escape(note)}</text>')
    svg.append("</svg>")

    OUT.mkdir(parents=True, exist_ok=True)
    out_svg = OUT / f"{name}.svg"
    out_svg.write_text("\n".join(svg), encoding="utf-8")
    png(out_svg, OUT / f"{name}.png", W, H)
    for p in problems:
        print(f"  layout: {p}")
    print(f"{name}: {len(lay['boxes'])} units, {len(lay['edges'])} arrows, {len(problems)} layout problems -> {out_svg}")
    return len(problems)


def png(svg_path, png_path, w, h):
    """Render the SVG to a PNG at twice its size with headless Chrome, when Chrome is installed."""
    chrome = next((c for c in CHROME if Path(c).exists() or shutil.which(c)), None)
    if not chrome:
        print("  no Chrome found: SVG only")
        return
    page = (f'<!doctype html><meta charset="utf-8"><style>html,body{{margin:0}}svg{{display:block}}</style>'
            f'{svg_path.read_text(encoding="utf-8")}')
    with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8") as f:
        f.write(page)
    subprocess.run([chrome, "--headless=new", "--disable-gpu", "--hide-scrollbars", "--force-device-scale-factor=2",
                    f"--window-size={math.ceil(w)},{math.ceil(h)}", f"--screenshot={png_path}", f"file://{f.name}"],
                   check=True, capture_output=True)
    Path(f.name).unlink()


def main():
    """Draw the named diagrams, or all of them; exit 1 when a layout has problems."""
    sys.path.insert(0, str(HERE))
    names = sys.argv[1:] or sorted(p.name[:-len("_layout.py")] for p in HERE.glob("*_layout.py"))
    sys.exit(1 if sum(draw(n) for n in names) else 0)


if __name__ == "__main__":
    main()
