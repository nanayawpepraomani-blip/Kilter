"""Generate the Kilter OG image (1200x630 PNG) for LinkedIn/Slack/iMessage
link previews.

Run from the repo root:

    pip install Pillow
    python scripts/generate_og.py

Writes demo/og-image.png (~78 KB). If you change pricing or pilot status,
update the strings below and re-run. macOS font paths assumed; on Linux
the script falls back to the default Pillow font if Helvetica isn't found.
"""
from __future__ import annotations
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageFilter

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT = REPO_ROOT / "demo" / "og-image.png"
W, H = 1200, 630

def font(size, bold=False):
    candidates = [
        ("/System/Library/Fonts/HelveticaNeue.ttc", 1 if bold else 0),
        ("/System/Library/Fonts/Helvetica.ttc",     1 if bold else 0),
        ("/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf", 0),
        ("/Library/Fonts/Arial.ttf", 0),
    ]
    for path, idx in candidates:
        if Path(path).exists():
            try:
                return ImageFont.truetype(path, size, index=idx)
            except Exception:
                continue
    return ImageFont.load_default()

# ── work in RGBA so transparent fills survive ────────────────────────
img = Image.new("RGBA", (W, H), (12, 30, 94, 255))
px = img.load()
top = (12, 30, 94)
mid = (18, 48, 160)
bot = (12, 90, 170)
for y in range(H):
    if y < H * 0.5:
        t = y / (H * 0.5)
        r = int(top[0] + (mid[0]-top[0])*t); g = int(top[1] + (mid[1]-top[1])*t); b = int(top[2] + (mid[2]-top[2])*t)
    else:
        t = (y - H*0.5) / (H*0.5)
        r = int(mid[0] + (bot[0]-mid[0])*t); g = int(mid[1] + (bot[1]-mid[1])*t); b = int(mid[2] + (bot[2]-mid[2])*t)
    for x in range(W):
        px[x, y] = (r, g, b, 255)

# ── cyan radial accent on the right (separate layer so alpha works) ──
overlay = Image.new("RGBA", (W, H), (0,0,0,0))
od = ImageDraw.Draw(overlay)
cx, cy = W - 280, H // 2
for r in range(420, 0, -2):
    a = max(0, int(70 - r * 0.18))
    od.ellipse((cx - r, cy - r, cx + r, cy + r), fill=(6, 182, 212, a))
overlay = overlay.filter(ImageFilter.GaussianBlur(8))
img = Image.alpha_composite(img, overlay)
d = ImageDraw.Draw(img)

# ── brand mark (K in a gradient square) ──────────────────────────────
MARK_X, MARK_Y, MARK = 80, 80, 80
mark_layer = Image.new("RGBA", (W, H), (0,0,0,0))
ml = ImageDraw.Draw(mark_layer)
# gradient body
for i in range(MARK):
    t = i / MARK
    r = int(31 + (6 - 31) * t); g = int(62 + (182 - 62) * t); b_ = int(196 + (212 - 196) * t)
    ml.line((MARK_X + i, MARK_Y, MARK_X + i, MARK_Y + MARK), fill=(r, g, b_, 255))
# round the corners by masking
mask = Image.new("L", (MARK, MARK), 0)
mm = ImageDraw.Draw(mask)
mm.rounded_rectangle((0, 0, MARK, MARK), radius=18, fill=255)
crop = mark_layer.crop((MARK_X, MARK_Y, MARK_X + MARK, MARK_Y + MARK))
crop.putalpha(mask)
img.paste(crop, (MARK_X, MARK_Y), crop)

# K letter
mk_font = font(64, bold=True)
k_bbox = d.textbbox((0,0), "K", font=mk_font)
kw, kh = k_bbox[2]-k_bbox[0], k_bbox[3]-k_bbox[1]
d.text((MARK_X + (MARK - kw)/2 - 2, MARK_Y + (MARK - kh)/2 - 10), "K", font=mk_font, fill="white")

# wordmark
wm_font = font(46, bold=True)
d.text((MARK_X + MARK + 22, MARK_Y + 16), "Kilter", font=wm_font, fill="white")

# ── eyebrow pill — draw on transparent layer then composite ─────────
eb_y = 220
eb = font(20, bold=True)
eb_text = "Now in pilot with partner banks"
eb_bbox = d.textbbox((0,0), eb_text, font=eb)
eb_w = eb_bbox[2] - eb_bbox[0]
pill_w = eb_w + 80
pill_layer = Image.new("RGBA", (W, H), (0,0,0,0))
pld = ImageDraw.Draw(pill_layer)
pld.rounded_rectangle((80, eb_y, 80 + pill_w, eb_y + 40), radius=20,
                      fill=(255,255,255,38), outline=(255,255,255,140), width=1)
img = Image.alpha_composite(img, pill_layer)
d = ImageDraw.Draw(img)
# pulse dot
d.ellipse((96, eb_y + 14, 110, eb_y + 28), fill=(16, 185, 129, 255))
d.text((120, eb_y + 8), eb_text, font=eb, fill="white")

# ── headline ─────────────────────────────────────────────────────────
h1 = font(100, bold=True)
d.text((80, 282), "Reconciliation,", font=h1, fill="white")
d.text((80, 388), "automated.", font=h1, fill=(120, 220, 255, 255))

# ── sub ──────────────────────────────────────────────────────────────
sub = font(26)
d.text((80, 510),
       "Self-hosted reconciliation: nostro/GL  ·  mobile money  ·  card-scheme settlements.",
       font=sub, fill=(220, 232, 250, 255))

# ── footer URL + price strip ────────────────────────────────────────
url = font(24, bold=True)
d.text((80, 562), "www.kilter-app.com", font=url, fill="white")
strip = font(20)
d.text((360, 566),
       "·   From $24K / yr   ·   $5K pilot   ·   No per-seat fees",
       font=strip, fill=(180, 210, 250, 255))

img.convert("RGB").save(OUT, "PNG", optimize=True)
print(f"wrote {OUT} ({OUT.stat().st_size // 1024} KB)")
