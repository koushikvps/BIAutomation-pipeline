"""Generate a polished end-to-end platform flow diagram as PNG. EY theme. v6.0 with Ops Module."""

from PIL import Image, ImageDraw, ImageFont
import math
import os

# ── Canvas ──
W, H = 4000, 4550
img = Image.new("RGB", (W, H), (17, 17, 27))
draw = ImageDraw.Draw(img)

# ── Colors ──
BG = (17, 17, 27)
CARD = (28, 28, 40)
CARD_HOVER = (35, 35, 50)
BORDER = (50, 50, 65)
YELLOW = (255, 230, 0)
WHITE = (245, 245, 250)
GRAY = (130, 130, 150)
GRAY_DIM = (80, 80, 100)
GREEN = (46, 204, 113)
RED = (231, 76, 60)
BLUE = (52, 152, 219)
TEAL = (26, 188, 156)
ORANGE = (243, 156, 18)
PURPLE = (155, 89, 182)
DARK_CARD = (22, 22, 34)
MAGENTA = (199, 44, 131)

# ── Fonts ──
def load_font(bold=False, size=20):
    name = "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"
    try:
        return ImageFont.truetype(f"/usr/share/fonts/truetype/dejavu/{name}", size)
    except:
        return ImageFont.load_default()

f_logo = load_font(True, 60)
f_tagline = load_font(False, 16)
f_title = load_font(True, 52)
f_subtitle = load_font(False, 22)
f_section = load_font(True, 28)
f_node_title = load_font(True, 22)
f_node_sub = load_font(False, 16)
f_label = load_font(True, 18)
f_small = load_font(False, 15)
f_tiny = load_font(False, 13)
f_stat_val = load_font(True, 56)
f_stat_label = load_font(False, 16)
f_mode_title = load_font(True, 24)
f_mode_body = load_font(False, 16)
f_circle = load_font(True, 16)
f_loop_label = load_font(True, 20)


# ── Drawing Helpers ──

def rounded_rect(x, y, w, h, r=16, fill=CARD, outline=BORDER, lw=2):
    draw.rounded_rectangle([x, y, x+w, y+h], radius=r, fill=fill, outline=outline, width=lw)

def pill(cx, cy, w, h, fill, text, text_color=BG, font=f_label):
    x, y = cx - w//2, cy - h//2
    draw.rounded_rectangle([x, y, x+w, y+h], radius=h//2, fill=fill)
    tw = draw.textbbox((0,0), text, font=font)[2] - draw.textbbox((0,0), text, font=font)[0]
    th = draw.textbbox((0,0), text, font=font)[3] - draw.textbbox((0,0), text, font=font)[1]
    draw.text((cx - tw//2, cy - th//2 - 1), text, fill=text_color, font=font)

def text_center(cx, cy, text, font=f_node_title, fill=WHITE):
    bb = draw.textbbox((0,0), text, font=font)
    tw, th = bb[2]-bb[0], bb[3]-bb[1]
    draw.text((cx - tw//2, cy - th//2), text, fill=fill, font=font)

def text_left(x, y, text, font=f_small, fill=GRAY):
    draw.text((x, y), text, fill=fill, font=font)

def arrow_down(cx, y1, y2, color=YELLOW, w=2, head=10):
    draw.line([(cx, y1), (cx, y2)], fill=color, width=w)
    draw.polygon([(cx, y2), (cx-head, y2-head*1.5), (cx+head, y2-head*1.5)], fill=color)

def arrow_right(x1, x2, cy, color=YELLOW, w=2, head=10):
    draw.line([(x1, cy), (x2, cy)], fill=color, width=w)
    draw.polygon([(x2, cy), (x2-head*1.5, cy-head), (x2-head*1.5, cy+head)], fill=color)

def diagonal_arrow(x1, y1, x2, y2, color=YELLOW, w=2):
    draw.line([(x1, y1), (x2, y2)], fill=color, width=w)
    angle = math.atan2(y2-y1, x2-x1)
    sz = 10
    draw.polygon([
        (x2, y2),
        (x2 - sz*math.cos(angle-0.4), y2 - sz*math.sin(angle-0.4)),
        (x2 - sz*math.cos(angle+0.4), y2 - sz*math.sin(angle+0.4)),
    ], fill=color)

def flow_node(cx, cy, w, h, title, subtitle="", accent=YELLOW, glow=False):
    x, y = cx - w//2, cy - h//2
    if glow:
        for i in range(3):
            draw.rounded_rectangle([x-i*2, y-i*2, x+w+i*2, y+h+i*2],
                                   radius=14, outline=(*accent, 30), width=1)
    rounded_rect(x, y, w, h, r=12, fill=CARD, outline=(*accent, 120) if len(accent)==3 else accent, lw=2)
    draw.rounded_rectangle([x, y, x+w, y+5], radius=3, fill=accent)
    text_center(cx, cy - (10 if subtitle else 0), title, f_node_title, WHITE)
    if subtitle:
        text_center(cx, cy + 14, subtitle, f_node_sub, GRAY)

def section_divider(y, label, color=YELLOW):
    for i in range(W):
        alpha = 1.0 - abs(i - W/2) / (W/2)
        c = tuple(int(color[j] * alpha + BG[j] * (1-alpha)) for j in range(3))
        draw.line([(i, y), (i, y+1)], fill=c)
    bb = draw.textbbox((0,0), label, font=f_section)
    tw = bb[2] - bb[0]
    pad = 30
    lx = W//2 - tw//2 - pad
    rounded_rect(lx, y-18, tw + pad*2, 38, r=19, fill=BG, outline=color, lw=2)
    text_center(W//2, y, label, f_section, color)

def connector_dot(cx, cy, r=6, color=YELLOW):
    draw.ellipse([cx-r, cy-r, cx+r, cy+r], fill=color)


# ══════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════

draw.rectangle([0, 0, 8, H], fill=YELLOW)
draw.text((50, 35), "EY", fill=YELLOW, font=f_logo)
draw.text((50, 105), "Building a better working world", fill=GRAY_DIM, font=f_tagline)
draw.text((50, 165), "End-to-End Platform Flow", fill=WHITE, font=f_title)
draw.text((50, 230), "How 13 AI agents transform a business story into a deployed, tested, self-healing data model", fill=GRAY, font=f_subtitle)

stats_y = 290
pill_data = [("13 Agents", YELLOW), ("55 Endpoints", YELLOW), ("3 Orchestrators", YELLOW), ("3 Timers", MAGENTA), ("3 Modes", YELLOW)]
px = 60
for label, color in pill_data:
    pill(px + 70, stats_y, 155, 34, color, label, BG, f_label)
    px += 185

# ══════════════════════════════════════════════════════════
# SECTION 1: ENTRY POINTS
# ══════════════════════════════════════════════════════════
section_divider(370, "  ENTRY POINTS  ")

entry_y = 460
flow_node(700, entry_y, 340, 80, "Azure DevOps", "Story or Bug Work Item", BLUE, True)
flow_node(2000, entry_y, 340, 80, "Web UI", "Free-text or Story ID", YELLOW, True)
flow_node(3300, entry_y, 340, 80, "Microsoft Teams", "Adaptive Card Actions", PURPLE, True)

arrow_down(700, entry_y + 40, entry_y + 110, BLUE)
arrow_down(2000, entry_y + 40, entry_y + 110, YELLOW)
arrow_down(3300, entry_y + 40, entry_y + 110, PURPLE)

line_y = entry_y + 120
draw.line([(700, line_y), (3300, line_y)], fill=GRAY_DIM, width=2)
connector_dot(700, line_y, 5, BLUE)
connector_dot(2000, line_y, 5, YELLOW)
connector_dot(3300, line_y, 5, PURPLE)
arrow_down(2000, line_y, line_y + 60, YELLOW)

interp_y = line_y + 100
flow_node(2000, interp_y, 420, 80, "Universal Story Interpreter", "Parses any format: Gherkin, bullets, plain English", YELLOW, True)

split_y = interp_y + 60
arrow_down(2000, split_y, split_y + 30, YELLOW)
connector_dot(2000, split_y + 40, 7, YELLOW)
diagonal_arrow(2000, split_y + 40, 800, split_y + 120, YELLOW, 2)
diagonal_arrow(2000, split_y + 40, 2000, split_y + 120, ORANGE, 2)
diagonal_arrow(2000, split_y + 40, 3200, split_y + 120, TEAL, 2)

pill(800, split_y + 145, 200, 32, YELLOW, "BI Pipeline", BG)
pill(2000, split_y + 145, 200, 32, ORANGE, "Bug Fix", BG)
pill(3200, split_y + 145, 200, 32, TEAL, "Test Automation", BG)

# ══════════════════════════════════════════════════════════
# SECTION 2: THREE PARALLEL PIPELINES
# ══════════════════════════════════════════════════════════
pipes_top = split_y + 200
section_divider(pipes_top, "  THREE ORCHESTRATED PIPELINES  ")

col_y_start = pipes_top + 70
BI_X = 800
BUG_X = 2000
TEST_X = 3200
NODE_W = 360
NODE_H = 72
NODE_GAP = 18

col_h_bi = 9 * (NODE_H + NODE_GAP) + 60
col_h_bug = 8 * (NODE_H + NODE_GAP) + 60
col_h_test = 6 * (NODE_H + NODE_GAP) + 60
max_col_h = max(col_h_bi, col_h_bug, col_h_test)

for cx, color, h in [(BI_X, YELLOW, col_h_bi), (BUG_X, ORANGE, col_h_bug), (TEST_X, TEAL, col_h_test)]:
    rounded_rect(cx - NODE_W//2 - 30, col_y_start - 10, NODE_W + 60, h, r=16, fill=DARK_CARD, outline=(*color, 50), lw=1)

def col_header(cx, y, title, subtitle, color):
    pill(cx, y + 20, NODE_W + 20, 40, color, title, BG, f_label)
    text_center(cx, y + 50, subtitle, f_tiny, GRAY_DIM)

col_header(BI_X, col_y_start, "PRODUCT 1: BI PIPELINE", "9 steps  |  7 agents  |  44 endpoints", YELLOW)
col_header(BUG_X, col_y_start, "BUG FIX ORCHESTRATOR", "8 steps  |  Bug Fixer Agent  |  3 endpoints", ORANGE)
col_header(TEST_X, col_y_start, "PRODUCT 2: TEST AUTOMATION", "5 steps  |  6 agents  |  11 endpoints", TEAL)

# BI Pipeline nodes
bi_steps = [
    ("Planner Agent", "Analyze requirements, detect mode", YELLOW),
    ("Discovery Agent", "Scan existing env (Integration Mode)", GRAY_DIM),
    ("Human Review Gate", "Approve or decline the plan", ORANGE),
    ("Developer Agent", "Generate Bronze/Silver/Gold SQL + ADF", YELLOW),
    ("Code Review Agent", "7-category AI quality review", YELLOW),
    ("Healer Agent", "Auto-fix review findings", GREEN),
    ("Pre-Deploy Validation", "Syntax + schema checks", BLUE),
    ("Deploy to Synapse", "Execute DDL + ADF pipelines", GREEN),
    ("Post-Deploy Validation", "DQ scoring, row counts, nulls", BLUE),
]

ny = col_y_start + 75
for i, (title, sub, color) in enumerate(bi_steps):
    flow_node(BI_X, ny, NODE_W, NODE_H, title, sub, color)
    if i < len(bi_steps) - 1:
        arrow_down(BI_X, ny + NODE_H//2, ny + NODE_H//2 + NODE_GAP, color)
    ny += NODE_H + NODE_GAP

bi_out_y = ny + 10
rounded_rect(BI_X - NODE_W//2, bi_out_y, NODE_W, 50, r=10, fill=CARD, outline=GREEN, lw=2)
text_center(BI_X, bi_out_y + 15, "Synapse + ADF + ADLS deployed", f_small, GREEN)
text_center(BI_X, bi_out_y + 34, "OR: Pull Request (Integration Mode)", f_tiny, GRAY_DIM)
arrow_down(BI_X, ny - NODE_GAP + 5, bi_out_y, GREEN)

# Bug Fix nodes
bug_steps = [
    ("Fetch Bug from ADO", "Read title, repro, expected vs actual", ORANGE),
    ("Find Original Artifacts", "Search Config DB for source story", ORANGE),
    ("Bug Fixer Agent", "AI: root cause + corrected code", YELLOW),
    ("Code Review Agent", "Review the fix (7 categories)", YELLOW),
    ("Human Review Gate", "Approve or decline the fix", ORANGE),
    ("Deploy Fix", "Corrected SQL to Synapse", GREEN),
    ("Re-Test", "Validate fix resolves the issue", BLUE),
    ("Update ADO Bug", "Set to Resolved + fix report", GREEN),
]

ny = col_y_start + 75
for i, (title, sub, color) in enumerate(bug_steps):
    flow_node(BUG_X, ny, NODE_W, NODE_H, title, sub, color)
    if i < len(bug_steps) - 1:
        arrow_down(BUG_X, ny + NODE_H//2, ny + NODE_H//2 + NODE_GAP, color)
    ny += NODE_H + NODE_GAP

bug_out_y = ny + 10
rounded_rect(BUG_X - NODE_W//2, bug_out_y, NODE_W, 40, r=10, fill=CARD, outline=GREEN, lw=2)
text_center(BUG_X, bug_out_y + 12, "ADO Bug #1234 -> Resolved", f_small, GREEN)
arrow_down(BUG_X, ny - NODE_GAP + 5, bug_out_y, GREEN)

# Test Automation nodes
test_steps = [
    ("Test Router Agent", "Classify: UI / Data / Both", TEAL),
    ("Test Planner Agent", "Generate test scenarios", TEAL),
    ("Test Generator Agent", "Playwright scripts + SQL tests", TEAL),
    ("Test Executor", "UI: QA's Edge browser | Data: server-side", BLUE),
    ("Test Reporter Agent", "ADO Test Plans + Cases + Bugs + Excel", GREEN),
]

ny = col_y_start + 75
for i, (title, sub, color) in enumerate(test_steps):
    flow_node(TEST_X, ny, NODE_W, NODE_H, title, sub, color)
    if i < len(test_steps) - 1:
        arrow_down(TEST_X, ny + NODE_H//2, ny + NODE_H//2 + NODE_GAP, color)
    ny += NODE_H + NODE_GAP

test_out_y = ny + 10
rounded_rect(TEST_X - NODE_W//2, test_out_y, NODE_W, 50, r=10, fill=CARD, outline=GREEN, lw=2)
text_center(TEST_X, test_out_y + 10, "ADO Test Plans + Excel Report", f_small, GREEN)
text_center(TEST_X, test_out_y + 30, "+ Teams Adaptive Cards", f_small, GREEN)
arrow_down(TEST_X, ny - NODE_GAP + 5, test_out_y, GREEN)

bug_found_y = test_out_y + 70
rounded_rect(TEST_X - NODE_W//2, bug_found_y, NODE_W, 55, r=10, fill=CARD, outline=RED, lw=3)
text_center(TEST_X, bug_found_y + 16, "Bug Found?", f_node_title, RED)
text_center(TEST_X, bug_found_y + 38, "Auto-creates ADO Bug work item", f_tiny, GRAY)
arrow_down(TEST_X, test_out_y + 50, bug_found_y, RED)

# THE KEY ARROW: Bug Found -> Bug Fix Orchestrator
bug_fix_start_y = col_y_start + 75
arrow_y = bug_found_y + 27
draw.line([(TEST_X - NODE_W//2, arrow_y), (BUG_X + NODE_W//2 + 50, arrow_y)], fill=RED, width=3)
draw.line([(BUG_X + NODE_W//2 + 50, arrow_y), (BUG_X + NODE_W//2 + 50, bug_fix_start_y)], fill=RED, width=3)
diagonal_arrow(BUG_X + NODE_W//2 + 50, bug_fix_start_y, BUG_X + NODE_W//2 + 5, bug_fix_start_y, RED, 3)
text_center((TEST_X - NODE_W//2 + BUG_X + NODE_W//2 + 50)//2, arrow_y - 18,
            "Triggers Bug Fix", f_label, RED)

# ══════════════════════════════════════════════════════════
# SECTION 3: SELF-CORRECTING LOOP
# ══════════════════════════════════════════════════════════
loop_section_y = col_y_start + max_col_h + 140
section_divider(loop_section_y, "  SELF-CORRECTING LOOP  ")

loop_y = loop_section_y + 70
rounded_rect(150, loop_y, W - 300, 160, r=20, fill=DARK_CARD, outline=YELLOW, lw=2)
text_center(W//2, loop_y + 25, "If a bug is found, AI fixes it and re-tests automatically -- no human coding required", f_subtitle, GRAY)

loop_nodes = [
    ("Story", BLUE, 380), ("Build", YELLOW, 830), ("Deploy", GREEN, 1280),
    ("Test", TEAL, 1730), ("Bug?", RED, 2180), ("Fix", ORANGE, 2630),
    ("Re-Test", BLUE, 3080), ("Green", GREEN, 3530),
]
circle_y = loop_y + 100
circle_r = 32
for label, color, x in loop_nodes:
    draw.ellipse([x-circle_r, circle_y-circle_r, x+circle_r, circle_y+circle_r], fill=color)
    draw.ellipse([x-circle_r+4, circle_y-circle_r+4, x+circle_r-4, circle_y-circle_r+12],
                 fill=(*color, 180) if len(color)==3 else color)
    text_center(x, circle_y, label, f_circle, BG if color not in [RED] else WHITE)
for i in range(len(loop_nodes) - 1):
    x1 = loop_nodes[i][2] + circle_r + 4
    x2 = loop_nodes[i+1][2] - circle_r - 4
    arrow_right(x1, x2, circle_y, GRAY_DIM, 2, 8)

loop_back_y = circle_y + circle_r + 15
draw.line([(2180, circle_y + circle_r), (2180, loop_back_y + 5)], fill=RED, width=2)
draw.line([(2180, loop_back_y + 5), (3530, loop_back_y + 5)], fill=RED, width=2)
draw.line([(3530, loop_back_y + 5), (3530, circle_y + circle_r)], fill=GREEN, width=2)

# ══════════════════════════════════════════════════════════
# SECTION 4: OPS MODULE (NEW)
# ══════════════════════════════════════════════════════════
ops_section_y = loop_y + 210
section_divider(ops_section_y, "  OPS MODULE: AUTOMATED MAINTENANCE  ", MAGENTA)

ops_top = ops_section_y + 55

# Big ops container
ops_container_w = W - 200
ops_container_h = 360
ops_x = 100
rounded_rect(ops_x, ops_top, ops_container_w, ops_container_h, r=18, fill=DARK_CARD, outline=MAGENTA, lw=2)
draw.rounded_rectangle([ops_x, ops_top, ops_x + ops_container_w, ops_top + 6], radius=3, fill=MAGENTA)

# Title inside container
text_center(W//2, ops_top + 30, "Zero-Touch Operations  --  3 Timer Triggers + 7 HTTP Endpoints", f_subtitle, MAGENTA)

# Timer triggers row (top half)
timer_y = ops_top + 75
timer_w = 1060
timer_h = 110
timer_gap = 80
timer_start_x = ops_x + (ops_container_w - 3 * timer_w - 2 * timer_gap) // 2

timers = [
    ("Auto-Pause Synapse", "Every 30 minutes", BLUE, [
        "Checks Config DB for recent pipeline activity",
        "Pauses Synapse if idle > 30 min",
        "Teams notification on pause",
        "Saves ~$870/month if left running 24/7",
    ]),
    ("Secret Health Check", "Daily at 8 AM UTC", ORANGE, [
        "Validates ADO PAT (HTTP call)",
        "Tests AI Foundry key (LLM call)",
        "Tests SQL password (connection)",
        "Teams alert if any expired/invalid",
    ]),
    ("DB Retention Cleanup", "Weekly: Sunday 2 AM", GREEN, [
        "Purges execution_log > 90 days",
        "Keeps last 5 artifact versions/object",
        "Cleans deployment_log history",
        "Prevents unbounded DB growth",
    ]),
]

for i, (title, schedule, color, items) in enumerate(timers):
    tx = timer_start_x + i * (timer_w + timer_gap)
    rounded_rect(tx, timer_y, timer_w, timer_h, r=10, fill=CARD, outline=color, lw=2)
    draw.rounded_rectangle([tx, timer_y, tx + timer_w, timer_y + 5], radius=3, fill=color)
    # Clock icon text
    text_left(tx + 15, timer_y + 14, title, f_label, WHITE)
    pill(tx + timer_w - 120, timer_y + 22, 200, 24, color, schedule, BG, f_tiny)
    for j, item in enumerate(items):
        text_left(tx + 20, timer_y + 45 + j * 17, f"\u2022  {item}", f_tiny, GRAY)

# HTTP endpoints row (bottom half)
ep_y = timer_y + timer_h + 30
ep_data = [
    ("/api/ops/dashboard", "Full health overview", MAGENTA),
    ("/api/ops/agent-stats", "Failure rates, durations", YELLOW),
    ("/api/ops/secret-health", "Credential validation", ORANGE),
    ("/api/ops/synapse-idle", "Idle check + pause", BLUE),
    ("/api/ops/regression-test", "LLM drift detection", RED),
    ("/api/ops/cleanup", "Manual DB purge", GREEN),
    ("/api/ops/pause-synapse", "Force pool pause", BLUE),
]
ep_w = 480
ep_h = 55
ep_gap = 20
ep_per_row = 4
ep_start_x = ops_x + (ops_container_w - ep_per_row * ep_w - (ep_per_row - 1) * ep_gap) // 2

for i, (route, desc, color) in enumerate(ep_data):
    col = i % ep_per_row
    row = i // ep_per_row
    ex = ep_start_x + col * (ep_w + ep_gap)
    ey = ep_y + row * (ep_h + 10)
    rounded_rect(ex, ey, ep_w, ep_h, r=8, fill=CARD, outline=color, lw=1)
    draw.rounded_rectangle([ex, ey, ex + 4, ey + ep_h], radius=2, fill=color)
    text_left(ex + 14, ey + 8, route, f_small, color)
    text_left(ex + 14, ey + 30, desc, f_tiny, GRAY_DIM)

# ══════════════════════════════════════════════════════════
# SECTION 5: THREE MODES
# ══════════════════════════════════════════════════════════
modes_y = ops_top + ops_container_h + 50
section_divider(modes_y, "  THREE DEPLOYMENT MODES  ")

mode_top = modes_y + 60
mode_w = 1100
mode_h = 300
mode_gap = 70
mode_start_x = (W - 3*mode_w - 2*mode_gap) // 2

modes = [
    ("GREENFIELD", "New platform from scratch", GREEN, [
        "Deploy all infrastructure via Bicep IaC",
        "Create Synapse pool, schemas, tables",
        "Our default naming conventions",
        "Direct deploy to Synapse + ADF",
        "Full medallion architecture (B/S/G)",
        "Best for: New projects, POCs",
    ]),
    ("BROWNFIELD", "Our existing deployment", BLUE, [
        "Detect existing tables/views first",
        "Skip objects that already exist",
        "Additive changes only (safe)",
        "Direct deploy to Synapse + ADF",
        "Preserves existing data",
        "Best for: Iterating on deployed models",
    ]),
    ("INTEGRATION MODE", "Client's existing platform", YELLOW, [
        "Discovery Agent scans their environment",
        "Auto-detect naming conventions",
        "Generate code matching their style",
        "Deliver as Pull Request to their repo",
        "Their CI/CD pipeline deploys",
        "Best for: Enterprise clients (Day 1)",
    ]),
]

for i, (title, subtitle, color, items) in enumerate(modes):
    mx = mode_start_x + i * (mode_w + mode_gap)
    rounded_rect(mx, mode_top, mode_w, mode_h, r=14, fill=CARD, outline=color, lw=2)
    draw.rounded_rectangle([mx, mode_top, mx + mode_w, mode_top + 6], radius=3, fill=color)
    text_left(mx + 25, mode_top + 20, title, f_mode_title, color)
    text_left(mx + 25, mode_top + 50, subtitle, f_small, GRAY)
    for j, item in enumerate(items):
        text_left(mx + 35, mode_top + 85 + j * 30, f"\u2022  {item}", f_mode_body, GRAY)

# ══════════════════════════════════════════════════════════
# SECTION 6: INFRASTRUCTURE
# ══════════════════════════════════════════════════════════
infra_y = mode_top + mode_h + 60
section_divider(infra_y, "  AZURE INFRASTRUCTURE  ")

res_top = infra_y + 60
resources = [
    ("Synapse Dedicated Pool", "DW100c  \u2022  Auto-pause on idle", BLUE),
    ("Function App (BI)", "EP1  \u2022  44 endpoints + 3 timers", YELLOW),
    ("Function App (Test)", "Shared EP1  \u2022  11 endpoints", TEAL),
    ("Azure SQL Database", "Config + Source + Metadata", BLUE),
    ("Key Vault", "Secrets  \u2022  Managed Identity", GREEN),
    ("ADLS Gen2 Storage", "Bronze lake + agent artifacts", BLUE),
    ("Azure Data Factory", "Incremental load pipelines", BLUE),
    ("Azure AI Foundry", "Phi-4  \u2022  LLM for 13 agents", YELLOW),
    ("Application Insights", "Telemetry + monitoring", PURPLE),
    ("Log Analytics", "Logs  \u2022  90-day retention", PURPLE),
]

res_cols = 5
res_w = 660
res_h = 65
res_gap_x = 30
res_gap_y = 20
res_start_x = (W - res_cols * res_w - (res_cols-1) * res_gap_x) // 2

for i, (name, desc, color) in enumerate(resources):
    col = i % res_cols
    row = i // res_cols
    rx = res_start_x + col * (res_w + res_gap_x)
    ry = res_top + row * (res_h + res_gap_y)
    rounded_rect(rx, ry, res_w, res_h, r=8, fill=CARD, outline=color, lw=2)
    draw.rounded_rectangle([rx, ry, rx + res_w, ry + 5], radius=3, fill=color)
    text_left(rx + 15, ry + 14, name, f_label, WHITE)
    text_left(rx + 15, ry + 38, desc, f_tiny, GRAY_DIM)

# ══════════════════════════════════════════════════════════
# SECTION 7: ALL 13 AGENTS
# ══════════════════════════════════════════════════════════
agents_y = res_top + 2 * (res_h + res_gap_y) + 40
section_divider(agents_y, "  13 AI AGENTS  ")

agent_top = agents_y + 55

text_left(100, agent_top, "BI Pipeline (7 agents)", f_label, YELLOW)
bi_agents = [
    ("Planner", YELLOW), ("Developer", YELLOW), ("Code Review", YELLOW), ("Validator", BLUE),
    ("Healer", GREEN), ("Discovery", GRAY_DIM), ("Bug Fixer", ORANGE),
]
ax = 100
for name, color in bi_agents:
    draw.ellipse([ax, agent_top + 30, ax + 40, agent_top + 70], fill=color)
    text_left(ax + 48, agent_top + 40, name, f_small, WHITE)
    ax += 210 + len(name) * 3

text_left(100, agent_top + 90, "Test Automation (6 agents)", f_label, TEAL)
test_agents = [
    ("Router", TEAL), ("Test Planner", TEAL), ("Data Planner", TEAL),
    ("Generator", TEAL), ("Executor", BLUE), ("Reporter", GREEN),
]
ax = 100
for name, color in test_agents:
    draw.ellipse([ax, agent_top + 120, ax + 40, agent_top + 160], fill=color)
    text_left(ax + 48, agent_top + 130, name, f_small, WHITE)
    ax += 220 + len(name) * 3

# ══════════════════════════════════════════════════════════
# SECTION 8: INTEGRATIONS
# ══════════════════════════════════════════════════════════
int_y = agent_top + 195
section_divider(int_y, "  INTEGRATIONS & OUTPUTS  ")

int_top = int_y + 55
integrations = [
    ("Azure DevOps", "Stories + Bugs + Test Plans + PRs", BLUE),
    ("Microsoft Teams", "Adaptive Cards + Webhook Alerts", PURPLE),
    ("Web UI (15 pages)", "Real-time pipeline + test viz", YELLOW),
    ("Excel Reports", "Formatted test results workbook", GREEN),
    ("ADO Git Repos", "PR delivery + artifact versioning", BLUE),
]

int_w = 660
int_gap = 30
int_start = (W - len(integrations) * int_w - (len(integrations)-1) * int_gap) // 2
for i, (name, desc, color) in enumerate(integrations):
    ix = int_start + i * (int_w + int_gap)
    rounded_rect(ix, int_top, int_w, 70, r=10, fill=CARD, outline=color, lw=2)
    draw.rounded_rectangle([ix, int_top, ix + int_w, int_top + 5], radius=3, fill=color)
    text_left(ix + 18, int_top + 16, name, f_label, WHITE)
    text_left(ix + 18, int_top + 42, desc, f_tiny, GRAY_DIM)

# ══════════════════════════════════════════════════════════
# FOOTER
# ══════════════════════════════════════════════════════════
footer_y = int_top + 110
section_divider(footer_y, "  THE BOTTOM LINE  ")

quote_y = footer_y + 55
text_center(W//2, quote_y, '"Your platform.  Your conventions.  Your CI/CD.  Our intelligence."', f_title, WHITE)

stat_y = quote_y + 80
stat_data = [
    ("13", "AI Agents", YELLOW), ("55", "Endpoints", YELLOW), ("3", "Orchestrators", YELLOW),
    ("3", "Timers", MAGENTA), ("3", "Modes", YELLOW), ("$0", "New Licenses", GREEN),
]
sx = (W - len(stat_data) * 550) // 2
for val, label, color in stat_data:
    text_center(sx + 275, stat_y, val, f_stat_val, color)
    text_center(sx + 275, stat_y + 60, label, f_stat_label, GRAY)
    sx += 550

draw.rectangle([0, H-40, W, H], fill=YELLOW)
text_center(W//2, H-30, "EY Core Assurance   |   Data & Analytics   |   AI Automation Practice   |   April 2026", f_small, BG)


# ══════════════════════════════════════════════════════════
# SAVE
# ══════════════════════════════════════════════════════════
output = os.path.join(os.path.dirname(__file__), "platform-flow.png")
img.save(output, "PNG")
print(f"Saved: {output} ({os.path.getsize(output) // 1024}KB)")
