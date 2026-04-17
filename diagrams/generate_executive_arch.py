#!/usr/bin/env python3
"""
Executive Architecture Diagram (v9 — clean, readable)
Top-to-bottom: Input -> Commander -> Workers -> Supervisor -> Output -> Value
Minimal text, large fonts, generous spacing.
"""
from PIL import Image, ImageDraw, ImageFont
import os, math

W, H = 2800, 3200
img = Image.new("RGB", (W, H), "#FAFBFC")
draw = ImageDraw.Draw(img)

NAVY     = "#1B2A4A"
DARK     = "#2E2E38"
WHITE    = "#FFFFFF"
BLUE     = "#0078D4"
LBLUE    = "#DEECF9"
PURPLE   = "#7B2FF2"
LPURPLE  = "#F0E6FF"
GREEN    = "#107C10"
LGREEN   = "#DFF6DD"
ORANGE   = "#CA5010"
LORANGE  = "#FFF4CE"
RED      = "#D13438"
LRED     = "#FDE7E9"
TEAL     = "#008272"
GRAY     = "#605E5C"
LGRAY    = "#EDEBE9"
BORDER   = "#C8C6C4"
YELLOW   = "#FFB900"

def _font(name, size):
    try:
        base = "/usr/share/fonts/truetype/dejavu/"
        f = {"b": "DejaVuSans-Bold.ttf", "r": "DejaVuSans.ttf"}
        return ImageFont.truetype(base + f[name], size)
    except:
        return ImageFont.load_default()

F_TITLE  = _font("b", 46)
F_SECT   = _font("b", 30)
F_HEAD   = _font("b", 26)
F_SUB    = _font("b", 22)
F_BODY   = _font("r", 20)
F_SMALL  = _font("r", 18)
F_TAG    = _font("b", 16)
F_ARROW  = _font("b", 15)
F_BIG    = _font("b", 54)
F_METRIC = _font("b", 40)

def rrect(x, y, w, h, r=12, fill=None, outline=None, lw=2):
    draw.rounded_rectangle([x, y, x+w, y+h], radius=r, fill=fill, outline=outline, width=lw)

def pill(x, y, text, color, font=None):
    f = font or F_TAG
    bbox = draw.textbbox((0, 0), text, font=f)
    tw = bbox[2] - bbox[0]
    rrect(x, y, tw + 24, 32, 16, color, lw=0)
    draw.text((x + 12, y + 5), text, fill=WHITE, font=f)
    return tw + 24

def down_arrow(cx, y1, y2, color=GRAY, label=None, lw=3):
    draw.line([(cx, y1), (cx, y2)], fill=color, width=lw)
    draw.polygon([(cx, y2), (cx-10, y2-16), (cx+10, y2-16)], fill=color)
    if label:
        bbox = draw.textbbox((0, 0), label, font=F_ARROW)
        tw = bbox[2] - bbox[0]
        mid = (y1 + y2) // 2
        draw.rectangle([cx - tw//2 - 4, mid - 12, cx + tw//2 + 4, mid + 6], fill=WHITE)
        draw.text((cx - tw//2, mid - 10), label, fill=color, font=F_ARROW)

def thick_down(cx, y1, y2, color):
    pts = [(cx-10, y1), (cx+10, y1), (cx+10, y2-22),
           (cx+18, y2-22), (cx, y2), (cx-18, y2-22), (cx-10, y2-22)]
    draw.polygon(pts, fill=color)

def right_arrow(x1, y, x2, color=GRAY, lw=2):
    draw.line([(x1, y), (x2, y)], fill=color, width=lw)
    draw.polygon([(x2, y), (x2-10, y-6), (x2-10, y+6)], fill=color)

# ── TITLE ──
draw.rectangle([0, 0, W, 80], fill=NAVY)
draw.rectangle([0, 80, W, 84], fill=YELLOW)
draw.text((30, 16), "EXECUTIVE OVERVIEW", fill=WHITE, font=F_TITLE)
draw.text((W - 440, 26), "For: Executive Management", fill=YELLOW, font=F_HEAD)

# ══════════════════════════════════════════════════════════════
# TIER 1: INPUT
# ══════════════════════════════════════════════════════════════
rrect(40, 105, W-80, 180, 16, WHITE, BLUE, 3)
draw.text((70, 115), "1", fill=BLUE, font=F_BIG)
draw.text((140, 125), "INPUT -- Users Submit Requirements", fill=BLUE, font=F_HEAD)

channels = [("Web UI", BLUE), ("Azure DevOps", ORANGE), ("Microsoft Teams", PURPLE)]
cx = 80
for cn, cc in channels:
    rrect(cx, 175, 340, 50, 10, WHITE, cc, 2)
    draw.ellipse([cx+8, 183, cx+40, 215], fill=cc)
    draw.text((cx+48, 186), cn, fill=cc, font=F_SUB)
    cx += 380

draw.text((80, 245), "Accepts:", fill=DARK, font=F_SUB)
fmts = ["Plain English", "Gherkin", "Bullets", "Technical spec"]
fx = 220
for fmt in fmts:
    fw = pill(fx, 243, fmt, PURPLE)
    fx += fw + 8

# ── Arrow ──
thick_down(W//2, 290, 340, BLUE)

# ══════════════════════════════════════════════════════════════
# TIER 2: COMMANDER
# ══════════════════════════════════════════════════════════════
rrect(40, 345, W-80, 240, 16, LBLUE, BLUE, 3)
draw.text((70, 355), "2", fill=BLUE, font=F_BIG)
draw.text((140, 365), "COMMANDER -- The Brain", fill=BLUE, font=F_HEAD)
draw.text((140, 400), "Decomposes story -> dispatches agents -> evaluates output", fill=DARK, font=F_BODY)

draw.text((80, 440), "Decision:", fill=DARK, font=F_SUB)
decisions = [("PROCEED", GREEN), ("RETRY", ORANGE), ("REROUTE", PURPLE), ("ESCALATE", RED)]
dx = 240
for dn, dc in decisions:
    dw = pill(dx, 438, dn, dc, F_SUB)
    dx += dw + 12

# Human gate
rrect(1500, 430, 560, 70, 10, LRED, RED, 3)
draw.text((1520, 440), "HUMAN REVIEW GATE", fill=RED, font=F_HEAD)
draw.text((1520, 472), "Approve / Decline via UI or Teams", fill=DARK, font=F_SMALL)

# Pipeline steps
draw.text((80, 510), "Steps:", fill=DARK, font=F_SUB)
steps = [("Interpret", PURPLE), ("Plan", BLUE), ("Review", RED), ("Build", GREEN),
         ("CodeReview", TEAL), ("Deploy", BLUE), ("Validate", GREEN)]
stx = 200
for i, (sn, sc) in enumerate(steps):
    rrect(stx, 508, 150, 36, 8, sc, lw=0)
    draw.text((stx+10, 514), sn, fill=WHITE, font=F_SUB)
    if i < len(steps) - 1:
        right_arrow(stx+152, 526, stx+172, sc)
    stx += 175

draw.text((80, 555), "Self-healing: Fail -> Healer -> Fix -> Re-deploy (max 3x)", fill=ORANGE, font=F_SMALL)

# ── Arrow ──
thick_down(W//2, 585, 635, PURPLE)

# ══════════════════════════════════════════════════════════════
# TIER 3: WORKER AGENTS
# ══════════════════════════════════════════════════════════════
rrect(40, 640, W-80, 260, 16, WHITE, PURPLE, 3)
draw.text((70, 650), "3", fill=PURPLE, font=F_BIG)
draw.text((140, 660), "WORKER AGENTS -- Do the Work", fill=PURPLE, font=F_HEAD)
draw.text((140, 695), "Each agent is specialized. Reports output back to Commander.", fill=DARK, font=F_BODY)

row1 = [("Planner", PURPLE), ("Developer", GREEN), ("Code Review", TEAL),
        ("Deployer", BLUE), ("Validator", GREEN)]
ax = 80
for an, ac in row1:
    rrect(ax, 735, 260, 50, 10, WHITE, ac, 2)
    draw.ellipse([ax+8, 743, ax+36, 771], fill=ac)
    draw.text((ax+44, 747), an, fill=ac, font=F_SUB)
    ax += 280

row2 = [("Healer", ORANGE), ("Discovery", NAVY), ("Convention", "#E3008C"),
        ("PR Delivery", ORANGE), ("Bug Fixer", RED), ("Notifier", PURPLE)]
ax = 80
for an, ac in row2:
    rrect(ax, 800, 220, 46, 10, WHITE, ac, 2)
    draw.ellipse([ax+8, 808, ax+32, 832], fill=ac)
    draw.text((ax+38, 810), an, fill=ac, font=F_SUB)
    ax += 240

draw.text((80, 860), "Bug fix: ADO bug -> AI analyze -> fix code -> re-test -> close", fill=GRAY, font=F_SMALL)

# ── Arrow ──
thick_down(W//2, 900, 950, ORANGE)

# ══════════════════════════════════════════════════════════════
# TIER 4: SUPERVISOR
# ══════════════════════════════════════════════════════════════
rrect(40, 955, W-80, 190, 16, LORANGE, ORANGE, 3)
draw.text((70, 965), "4", fill=ORANGE, font=F_BIG)
draw.text((140, 975), "SUPERVISOR -- Validates & Enforces", fill=ORANGE, font=F_HEAD)
draw.text((140, 1010), "Independent watchdog. Monitors Commander, enforces SLA, quality gates.", fill=DARK, font=F_BODY)

features = [
    ("Independent checks", GREEN), ("Override authority", RED),
    ("SLA enforcement", BLUE), ("Final sign-off", TEAL),
]
sfx = 80
for sfn, sfc in features:
    rrect(sfx, 1055, 24, 24, 4, sfc, lw=0)
    draw.text((sfx+32, 1057), sfn, fill=sfc, font=F_SUB)
    sfx += 320

draw.text((80, 1100), "Cannot be bypassed -- monitors Commander independently", fill=RED, font=F_SMALL)

# ── Arrow ──
thick_down(W//2, 1145, 1195, GREEN)

# ══════════════════════════════════════════════════════════════
# TIER 5: OUTPUT
# ══════════════════════════════════════════════════════════════
rrect(40, 1200, W-80, 240, 16, WHITE, TEAL, 3)
draw.text((70, 1210), "5", fill=TEAL, font=F_BIG)
draw.text((140, 1220), "OUTPUT -- Data Platform (Auto-Built)", fill=TEAL, font=F_HEAD)

# Medallion
rrect(80, 1270, 600, 55, 10, LORANGE, ORANGE, 2)
draw.text((100, 1280), "BRONZE", fill=ORANGE, font=F_HEAD)
draw.text((280, 1288), "Raw data (Parquet)", fill=DARK, font=F_BODY)

down_arrow(380, 1325, 1345, BLUE)

rrect(80, 1345, 600, 55, 10, LBLUE, BLUE, 2)
draw.text((100, 1355), "SILVER", fill=BLUE, font=F_HEAD)
draw.text((280, 1363), "Cleaned + validated", fill=DARK, font=F_BODY)

down_arrow(380, 1400, 1420, GREEN)

# ── Arrow ──
thick_down(W//2, 1440, 1490, GREEN)

# ══════════════════════════════════════════════════════════════
# TIER 6: BUSINESS VALUE
# ══════════════════════════════════════════════════════════════
rrect(40, 1495, W-80, 160, 16, LGREEN, GREEN, 3)
draw.text((70, 1505), "6", fill=GREEN, font=F_BIG)
draw.text((140, 1515), "BUSINESS VALUE", fill=GREEN, font=F_HEAD)

metrics = [
    ("80%", "faster", "Weeks -> hours"),
    ("100%", "auditable", "Full deploy trail"),
    ("0", "manual SQL", "AI writes all code"),
    ("3x", "self-heal", "Auto-fix failures"),
    ("3", "modes", "Green/Brown/Integration"),
]
mx = 80
for mv, ml, md in metrics:
    draw.text((mx, 1560), mv, fill=GREEN, font=F_METRIC)
    draw.text((mx+80, 1565), ml, fill=DARK, font=F_SUB)
    draw.text((mx+80, 1595), md, fill=GRAY, font=F_SMALL)
    mx += 280

draw.text((80, 1625), "Human-in-the-loop: nothing deploys without explicit approval", fill=RED, font=F_SUB)

# ══════════════════════════════════════════════════════════════
# SECTION 2: DEPLOY MODES + SECURITY + COST
# ══════════════════════════════════════════════════════════════
draw.text((40, 1690), "Three Deploy Modes", fill=DARK, font=F_SECT)
draw.rectangle([40, 1728, 360, 1731], fill=ORANGE)

modes = [
    ("GREENFIELD", "Build from scratch -> direct deploy to Synapse", GREEN),
    ("BROWNFIELD", "Additive only -- check_object_exists() skips existing", ORANGE),
    ("INTEGRATION", "Discover client conventions -> generate matching code -> deliver as PR", PURPLE),
]
my = 1745
for mn, md, mc in modes:
    rrect(40, my, W-80, 55, 10, WHITE, mc, 2)
    pill(60, my+12, mn, mc, F_HEAD)
    bbox = draw.textbbox((0, 0), mn, font=F_HEAD)
    tw = bbox[2] - bbox[0]
    draw.text((tw + 100, my+16), md, fill=GRAY, font=F_BODY)
    my += 65

# Security
draw.text((40, my+15), "Security -- 6 Layers", fill=DARK, font=F_SECT)
draw.rectangle([40, my+53, 340, my+56], fill=RED)

layers = [
    ("1", "USER AUTH", "Entra ID SSO -> JWT", BLUE),
    ("2", "SERVICE ID", "Managed Identity (zero passwords)", GREEN),
    ("3", "SECRETS", "Key Vault (MI access only)", RED),
    ("4", "API", "Function Key on all endpoints", ORANGE),
    ("5", "DATA", "Parameterized SQL + input validation", TEAL),
    ("6", "OPS", "Secret health + audit trail + regression", NAVY),
]
ly = my + 70
for ln, lname, ldesc, lc in layers:
    rrect(40, ly, W-80, 42, 8, WHITE, lc, 2)
    draw.ellipse([55, ly+7, 85, ly+37], fill=lc)
    draw.text((64, ly+10), ln, fill=WHITE, font=F_HEAD)
    draw.text((100, ly+4), lname, fill=lc, font=F_SUB)
    draw.text((340, ly+8), ldesc, fill=DARK, font=F_SMALL)
    ly += 48

# Cost + Ops
draw.text((40, ly+15), "Operations & Cost", fill=DARK, font=F_SECT)
draw.rectangle([40, ly+53, 340, ly+56], fill=ORANGE)

rrect(40, ly+68, W-80, 110, 14, WHITE, ORANGE, 2)
ops = [
    ("Auto-Pause Synapse", "Every 30 min", "~$870/mo saved", ORANGE),
    ("Secret Health", "Daily 8 AM", "Teams alert", RED),
    ("DB Cleanup", "Weekly", "Prune old data", NAVY),
    ("Self-Healing", "Automatic", "3x retry", ORANGE),
]
opx = 60
for on, os_val, ores, oc in ops:
    draw.text((opx, ly+78), on, fill=oc, font=F_SUB)
    draw.text((opx, ly+104), os_val, fill=GRAY, font=F_SMALL)
    draw.text((opx, ly+126), ores, fill=GREEN, font=F_SMALL)
    opx += 380

rrect(40, ly+190, W-80, 55, 12, LGREEN, GREEN, 2)
draw.text((60, ly+200), "EST. COST: ~$327-500/mo  |  Single Bicep deploy  |  Auto-pause cuts Synapse cost by 80%", fill=DARK, font=F_BODY)

# ── FOOTER ──
footer_y = ly + 270
draw.rectangle([0, footer_y, W, footer_y+50], fill=NAVY)
draw.text((30, footer_y+12), "BI & Test Automation Platform -- Executive Overview v9.0", fill=WHITE, font=F_BODY)
draw.text((W-460, footer_y+12), "Confidential -- Executive Management", fill=YELLOW, font=F_BODY)

# Crop to content
img = img.crop((0, 0, W, footer_y+50))
out = os.path.join(os.path.dirname(__file__), "executive-architecture.png")
img.save(out, "PNG", optimize=True)
print(f"Saved: {out}  ({os.path.getsize(out)//1024} KB)")
