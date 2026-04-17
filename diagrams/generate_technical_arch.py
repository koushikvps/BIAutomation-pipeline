#!/usr/bin/env python3
"""
Technical Architecture — Solution Architect Diagram (v9 — clean, readable)
Vertical flow: Users → Entra ID → Supervisor → Commander → Agents → Data → IaC
Designed for readability: large fonts, generous spacing, minimal text per box.
"""
from PIL import Image, ImageDraw, ImageFont
import os, math

W, H = 3200, 4200
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
LTEAL    = "#D4EFED"
GRAY     = "#605E5C"
LGRAY    = "#EDEBE9"
BORDER   = "#C8C6C4"
YELLOW   = "#FFB900"
PINK     = "#E3008C"

def _font(name, size):
    try:
        base = "/usr/share/fonts/truetype/dejavu/"
        f = {"b": "DejaVuSans-Bold.ttf", "r": "DejaVuSans.ttf"}
        return ImageFont.truetype(base + f[name], size)
    except:
        return ImageFont.load_default()

F_TITLE = _font("b", 44)
F_ZONE  = _font("b", 26)
F_HEAD  = _font("b", 24)
F_SUB   = _font("b", 20)
F_BODY  = _font("r", 18)
F_SMALL = _font("r", 16)
F_TAG   = _font("b", 15)
F_ARROW = _font("b", 14)

def rrect(x, y, w, h, r=12, fill=None, outline=None, lw=2):
    draw.rounded_rectangle([x, y, x+w, y+h], radius=r, fill=fill, outline=outline, width=lw)

def zone_label(x, y, text, color):
    bbox = draw.textbbox((0, 0), text, font=F_ZONE)
    tw = bbox[2] - bbox[0]
    rrect(x, y, tw + 30, 36, 8, color, lw=0)
    draw.text((x + 15, y + 5), text, fill=WHITE, font=F_ZONE)

def pill(x, y, text, color, font=None):
    f = font or F_TAG
    bbox = draw.textbbox((0, 0), text, font=f)
    tw = bbox[2] - bbox[0]
    rrect(x, y, tw + 20, 28, 14, color, lw=0)
    draw.text((x + 10, y + 4), text, fill=WHITE, font=f)
    return tw + 20

def icon_box(x, y, w, h, icon, title, subtitle, color, bg=WHITE):
    rrect(x, y, w, h, 10, bg, color, 2)
    draw.ellipse([x+14, y+14, x+46, y+46], fill=color)
    draw.text((x+22, y+18), icon, fill=WHITE, font=F_SUB)
    draw.text((x+58, y+12), title, fill=DARK, font=F_SUB)
    draw.text((x+58, y+38), subtitle, fill=GRAY, font=F_SMALL)

def down_arrow(cx, y1, y2, color=GRAY, label=None, lw=3):
    draw.line([(cx, y1), (cx, y2)], fill=color, width=lw)
    draw.polygon([(cx, y2), (cx-8, y2-14), (cx+8, y2-14)], fill=color)
    if label:
        bbox = draw.textbbox((0, 0), label, font=F_ARROW)
        tw = bbox[2] - bbox[0]
        mid = (y1 + y2) // 2
        draw.rectangle([cx - tw//2 - 4, mid - 10, cx + tw//2 + 4, mid + 6], fill=WHITE)
        draw.text((cx - tw//2, mid - 9), label, fill=color, font=F_ARROW)

def right_arrow(x1, y, x2, color=GRAY, lw=2):
    draw.line([(x1, y), (x2, y)], fill=color, width=lw)
    draw.polygon([(x2, y), (x2-10, y-6), (x2-10, y+6)], fill=color)

# ── TITLE ──
draw.rectangle([0, 0, W, 76], fill=NAVY)
draw.rectangle([0, 76, W, 80], fill=YELLOW)
draw.text((30, 14), "TECHNICAL ARCHITECTURE", fill=WHITE, font=F_TITLE)
draw.text((W - 480, 24), "BI Automation Platform v9.0", fill=YELLOW, font=F_HEAD)

# ══════════════════════════════════════════════════════════════
# LAYER 1: EXTERNAL ACTORS  (y=100..230)
# ══════════════════════════════════════════════════════════════
rrect(30, 100, W-60, 140, 16, WHITE, RED, 2)
zone_label(40, 92, "EXTERNAL ACTORS", RED)

actors = [
    (70,   "U", "End Users",    "Web UI (MSAL SSO)",   BLUE),
    (570,  "A", "Azure DevOps", "REST API (PAT auth)",  ORANGE),
    (1070, "T", "Teams",        "Webhooks + Bot",       PURPLE),
    (1570, "Q", "QA Machine",   "Playwright (Edge)",    GREEN),
    (2070, "P", "Power BI",     "DirectQuery to Gold",  BLUE),
    (2570, "S", "SharePoint",   "Graph API (OAuth2)",   GREEN),
]
for ax, ic, title, sub, color in actors:
    icon_box(ax, 120, 460, 100, ic, title, sub, color)

# ══════════════════════════════════════════════════════════════
# LAYER 2: ENTRA ID  (y=270..420)
# ══════════════════════════════════════════════════════════════
down_arrow(W//2, 240, 280, NAVY, "authenticate")
rrect(30, 280, W-60, 150, 16, LPURPLE, NAVY, 3)
zone_label(40, 272, "ENTRA ID (AZURE AD) — IDENTITY PERIMETER", NAVY)

entra_items = [
    (70,   "E", "SSO + JWT Tokens",     "User auth via MSAL.js",        NAVY),
    (820,  "M", "Managed Identities",   "Zero-trust service auth",      GREEN),
    (1570, "K", "Key Vault (RBAC)",     "Secrets: SQL, AI, ADO, Teams", RED),
    (2320, "L", "Monitoring",           "App Insights + Log Analytics",  PURPLE),
]
for ax, ic, title, sub, color in entra_items:
    icon_box(ax, 315, 710, 95, ic, title, sub, color)

# ══════════════════════════════════════════════════════════════
# LAYER 3: SUPERVISOR + COMMANDER  (y=470..940)
# ══════════════════════════════════════════════════════════════
down_arrow(W//2, 430, 470, ORANGE, "JWT + MI tokens")

# Supervisor outer boundary
rrect(30, 470, W-60, 490, 16, "#FFF8F0", ORANGE, 3)
zone_label(40, 462, "SUPERVISOR — Independent Quality Watchdog", ORANGE)

# Supervisor info strip
rrect(60, 510, W-120, 60, 10, LORANGE, ORANGE, 2)
draw.text((80, 518), "Validates Commander plans  |  Enforces SLA  |  Overrides on quality failure  |  Final sign-off", fill=DARK, font=F_BODY)
draw.text((80, 544), "Independent from Commander — cannot be bypassed", fill=ORANGE, font=F_SMALL)

# Commander inner box
rrect(60, 590, W-120, 350, 14, WHITE, BLUE, 3)
zone_label(70, 582, "COMMANDER — Task Decomposition & Dispatch", BLUE)

# Commander description
rrect(90, 630, 1400, 80, 10, LBLUE, BLUE, 2)
draw.text((110, 640), "Story in → LLM decomposes → dispatches agents → evaluates output", fill=DARK, font=F_BODY)
draw.text((110, 668), "Decision engine: PROCEED | RETRY (with feedback) | REROUTE | ESCALATE", fill=BLUE, font=F_SMALL)

# Human Review Gate
rrect(1530, 630, 590, 80, 10, LRED, RED, 3)
draw.text((1550, 640), "HUMAN REVIEW GATE", fill=RED, font=F_HEAD)
draw.text((1550, 670), "Approve / Decline via UI or Teams", fill=DARK, font=F_SMALL)

# Orchestration chain
draw.text((90, 730), "Pipeline Steps:", fill=DARK, font=F_SUB)
steps = [
    ("Register", TEAL), ("Notify", PURPLE), ("Planner", PURPLE),
    ("REVIEW", RED), ("Developer", GREEN), ("CodeReview", TEAL),
    ("Deploy", BLUE), ("Validate", GREEN), ("Post-Val", GREEN),
]
sx = 90
for i, (label, color) in enumerate(steps):
    rrect(sx, 760, 160, 40, 8, color, lw=0)
    draw.text((sx+10, 766), label, fill=WHITE, font=F_SUB)
    if i < len(steps) - 1:
        right_arrow(sx+162, 780, sx+182, color)
    sx += 185

# Self-heal + Bug fix
draw.text((90, 820), "Self-Healing:", fill=ORANGE, font=F_SUB)
draw.text((280, 822), "Fail -> Healer Agent -> Fix -> Re-deploy (max 3 retries, then escalate)", fill=GRAY, font=F_BODY)

draw.text((90, 855), "Bug Fix Loop:", fill=RED, font=F_SUB)
bf = [("Fetch Bug", RED), ("Analyze", PURPLE), ("Fix", GREEN), ("Review", TEAL),
      ("Approve", RED), ("Deploy", BLUE), ("Re-test", GREEN), ("Close", ORANGE)]
bfx = 280
for i, (bn, bc) in enumerate(bf):
    pw = pill(bfx, 853, bn, bc)
    bfx += pw + 8

draw.text((90, 895), "Integration:", fill=PURPLE, font=F_SUB)
im = [("Discover", NAVY), ("Conventions", TEAL), ("Generate", GREEN),
      ("Review", RED), ("PR to ADO", ORANGE)]
imx = 280
for i, (iname, ic) in enumerate(im):
    pw = pill(imx, 893, iname, ic)
    imx += pw + 8

# ══════════════════════════════════════════════════════════════
# LAYER 4: WORKER AGENTS  (y=990..1400)
# ══════════════════════════════════════════════════════════════
down_arrow(W//2, 960, 1000, PURPLE, "dispatch tasks")

rrect(30, 1000, W-60, 420, 16, WHITE, PURPLE, 3)
zone_label(40, 992, "WORKER AGENTS — Report Back to Commander", PURPLE)

# Row 1: Core pipeline agents
draw.text((70, 1040), "Core Pipeline:", fill=DARK, font=F_SUB)
row1 = [
    (70,   "P", "Planner Agent",    "Story -> execution plan (LLM)",   PURPLE),
    (670,  "D", "Developer Agent",  "Plan -> SQL + ADF code (LLM)",    GREEN),
    (1270, "R", "Code Review",      "7-category quality check (LLM)", TEAL),
    (1870, "A", "ADF Deployer",     "Pipeline deploy (MI Bearer)",    BLUE),
    (2470, "S", "SQL Deployer",     "DDL to Synapse (ODBC)",          TEAL),
]
for ax, ic, title, sub, color in row1:
    icon_box(ax, 1070, 560, 80, ic, title, sub, color)

# Row 2: Validation + healing
draw.text((70, 1170), "Validation & Healing:", fill=DARK, font=F_SUB)
row2 = [
    (70,   "V", "Validator (Pre/Post)", "Schema + data quality checks",   GREEN),
    (670,  "H", "Healer Agent",         "Auto-fix failures (LLM, 3x)",    ORANGE),
    (1270, "B", "Bug Fixer Agent",      "ADO bug -> fix -> re-test (LLM)", RED),
]
for ax, ic, title, sub, color in row2:
    icon_box(ax, 1200, 560, 80, ic, title, sub, color)

# Row 3: Integration + ops agents
draw.text((70, 1300), "Integration & Ops:", fill=DARK, font=F_SUB)
row3 = [
    (70,   "I", "Discovery Agent",    "Scan existing Synapse/ADF/ADLS", NAVY),
    (670,  "C", "Convention Adapter",  "Auto-detect naming patterns",    PINK),
    (1270, "G", "PR Delivery",         "Push artifacts as ADO PR",       ORANGE),
    (1870, "N", "Teams Notifier",      "Adaptive cards (4 types)",       PURPLE),
]
for ax, ic, title, sub, color in row3:
    icon_box(ax, 1330, 560, 80, ic, title, sub, color)

# Test Automation
rrect(2470, 1200, 660, 210, 12, WHITE, RED, 2)
draw.text((2490, 1210), "Test Automation (6 agents)", fill=RED, font=F_SUB)
test_agents = ["TestRouter (AI)", "TestPlanner (AI)", "DataTestPlanner (AI)",
               "TestGenerator (AI)", "DataTestExecutor", "TestReporter"]
ty = 1240
for ta in test_agents:
    draw.text((2500, ty), "* " + ta, fill=GRAY, font=F_SMALL)
    ty += 26

# ══════════════════════════════════════════════════════════════
# LAYER 5: DATA TIER  (y=1460..2160)
# ══════════════════════════════════════════════════════════════
down_arrow(W//2, 1420, 1470, TEAL, "SQL / REST / Storage")

rrect(30, 1470, W-60, 720, 16, WHITE, TEAL, 3)
zone_label(40, 1462, "DATA & PROCESSING TIER", TEAL)

# Synapse box with medallion
rrect(60, 1520, 900, 400, 12, WHITE, TEAL, 2)
draw.text((80, 1530), "Azure Synapse Analytics", fill=TEAL, font=F_HEAD)
draw.text((80, 1560), "Dedicated SQL Pool DW100c  |  TDS 1433", fill=GRAY, font=F_BODY)

rrect(80, 1600, 860, 70, 10, LORANGE, ORANGE, 2)
draw.text((100, 1610), "BRONZE", fill=ORANGE, font=F_HEAD)
draw.text((260, 1618), "External Tables -> read Parquet from ADLS", fill=DARK, font=F_BODY)

down_arrow(510, 1670, 1695, BLUE)

rrect(80, 1695, 860, 70, 10, LBLUE, BLUE, 2)
draw.text((100, 1705), "SILVER", fill=BLUE, font=F_HEAD)
draw.text((260, 1713), "Views -- cleaned, deduplicated, typed", fill=DARK, font=F_BODY)

down_arrow(510, 1765, 1790, GREEN)

rrect(80, 1790, 860, 70, 10, LGREEN, GREEN, 2)
draw.text((100, 1800), "GOLD", fill=GREEN, font=F_HEAD)
draw.text((260, 1808), "Views -- business-ready, star schema", fill=DARK, font=F_BODY)

draw.text((80, 1880), "Auth: SQL Auth (password from Key Vault)", fill=RED, font=F_SMALL)

# ADF
rrect(1000, 1520, 660, 200, 12, WHITE, BLUE, 2)
draw.text((1020, 1530), "Azure Data Factory", fill=BLUE, font=F_HEAD)
draw.text((1020, 1560), "Bronze ingestion pipelines", fill=GRAY, font=F_BODY)
draw.text((1020, 1600), "Source DB -> Copy Activity -> ADLS Parquet", fill=DARK, font=F_SMALL)
draw.text((1020, 1628), "Schedule: Daily trigger (2 AM UTC)", fill=GRAY, font=F_SMALL)
draw.text((1020, 1656), "Auth: MI -> management.azure.com", fill=GREEN, font=F_SMALL)
draw.text((1020, 1684), "RBAC: Storage Blob Data Contributor", fill=GREEN, font=F_SMALL)

# ADLS
rrect(1000, 1740, 660, 160, 12, WHITE, GREEN, 2)
draw.text((1020, 1750), "ADLS Gen2 (Data Lake)", fill=GREEN, font=F_HEAD)
draw.text((1020, 1780), "bronze/ container -> *.parquet files", fill=GRAY, font=F_BODY)
draw.text((1020, 1810), "Read by Synapse external tables", fill=DARK, font=F_SMALL)
draw.text((1020, 1838), "Written by ADF Copy Activity", fill=DARK, font=F_SMALL)

# Config DB
rrect(1700, 1520, 530, 380, 12, WHITE, ORANGE, 2)
draw.text((1720, 1530), "Azure SQL Database", fill=ORANGE, font=F_HEAD)
draw.text((1720, 1560), "Source + Config DB (Basic 5 DTU)", fill=GRAY, font=F_BODY)
config_tables = [
    "pipeline_registry", "execution_log",
    "artifact_versions", "column_lineage",
    "source_connectors", "data_quality_rules",
    "convention_rulesets", "semantic_definitions",
]
cty = 1600
for ct in config_tables:
    draw.text((1730, cty), "* " + ct, fill=DARK, font=F_SMALL)
    cty += 26
draw.text((1720, 1870), "Auth: SQL Auth (ODBC, TLS 1.2)", fill=RED, font=F_SMALL)

# AI Foundry
rrect(2270, 1520, 660, 200, 12, WHITE, PURPLE, 2)
draw.text((2290, 1530), "Azure AI Foundry", fill=PURPLE, font=F_HEAD)
draw.text((2290, 1560), "Model: Phi-4 / GPT-4o", fill=GRAY, font=F_BODY)
draw.text((2290, 1600), "HTTPS POST /chat/completions", fill=DARK, font=F_SMALL)
draw.text((2290, 1628), "Used by 8 LLM-powered agents", fill=DARK, font=F_SMALL)
draw.text((2290, 1656), "Auth: API Key from Key Vault", fill=RED, font=F_SMALL)

# Ops Module
rrect(2270, 1740, 660, 160, 12, WHITE, NAVY, 2)
draw.text((2290, 1750), "Ops Module (3 Timers)", fill=NAVY, font=F_HEAD)
timers = [
    ("Auto-pause Synapse", "Every 30 min", "Saves ~$870/mo"),
    ("Secret health check", "Daily 8 AM", "Teams alert"),
    ("DB cleanup", "Weekly Sun 2 AM", "Prune old logs"),
]
tiy = 1790
for tn, ts, td in timers:
    draw.text((2300, tiy), "* " + tn, fill=DARK, font=F_SMALL)
    draw.text((2580, tiy), ts, fill=GRAY, font=F_SMALL)
    draw.text((2780, tiy), td, fill=GREEN, font=F_SMALL)
    tiy += 30

# ══════════════════════════════════════════════════════════════
# LAYER 6: SECURITY SUMMARY  (y=2220..2640)
# ══════════════════════════════════════════════════════════════
rrect(30, 2230, W-60, 430, 16, WHITE, RED, 3)
zone_label(40, 2222, "SECURITY & AUTH MATRIX", RED)

auth_rows = [
    ("User -> Web UI",       "Entra SSO",    "MSAL.js -> JWT",                    BLUE),
    ("UI -> Function App",   "Function Key", "x-functions-key header",            GREEN),
    ("Func -> Key Vault",    "Managed ID",   "System MI -> RBAC Secrets User",     GREEN),
    ("Func -> Synapse",      "SQL Auth",     "User+Password via ODBC (TLS 1.2)",  RED),
    ("Func -> Config DB",    "SQL Auth",     "User+Password via ODBC (TLS 1.2)",  RED),
    ("Func -> AI Foundry",   "API Key",      "AI_API_KEY header (HTTPS)",         PURPLE),
    ("Func -> ADO",          "PAT",          "Base64 Authorization header",       ORANGE),
    ("Func -> ADF",          "MI Bearer",    "ManagedIdentityCredential -> ARM",   GREEN),
    ("Func -> Teams",        "Webhook URL",  "Secret embedded in URL",            PURPLE),
    ("ADF -> ADLS",          "MI RBAC",      "Storage Blob Data Contributor",     GREEN),
    ("Synapse -> ADLS",      "Storage Cred", "External data source credential",   TEAL),
]
ay = 2280
for source, method, detail, color in auth_rows:
    rrect(60, ay, 120, 30, 15, color, lw=0)
    draw.text((72, ay+5), method, fill=WHITE, font=F_TAG)
    draw.text((200, ay+5), source, fill=DARK, font=F_BODY)
    draw.text((600, ay+5), detail, fill=GRAY, font=F_BODY)
    ay += 36

# Network note
rrect(1700, 2280, 530, 180, 12, LRED, RED, 2)
draw.text((1720, 2290), "NETWORK STATUS", fill=RED, font=F_HEAD)
draw.text((1720, 2320), "All public endpoints (no VNet)", fill=DARK, font=F_BODY)
draw.text((1720, 2360), "Phase 2: VNet + Private Endpoints", fill=GREEN, font=F_SMALL)
draw.text((1720, 2388), "+ NSGs + Azure Firewall", fill=GREEN, font=F_SMALL)

# 3 modes
rrect(2280, 2280, 860, 180, 12, WHITE, ORANGE, 2)
draw.text((2300, 2290), "THREE DEPLOY MODES", fill=ORANGE, font=F_HEAD)
modes = [
    ("GREENFIELD", "Build from scratch -> direct deploy", GREEN),
    ("BROWNFIELD", "Existing platform -> additive only", ORANGE),
    ("INTEGRATION", "Client platform -> PR delivery", PURPLE),
]
mdy = 2325
for mn, md, mc in modes:
    pill(2310, mdy, mn, mc, F_SUB)
    draw.text((2530, mdy+4), md, fill=GRAY, font=F_SMALL)
    mdy += 45

# ══════════════════════════════════════════════════════════════
# LAYER 7: IaC FOOTER
# ══════════════════════════════════════════════════════════════
rrect(30, 2700, W-60, 120, 12, LGRAY, NAVY, 2)
zone_label(40, 2692, "INFRASTRUCTURE AS CODE — BICEP", NAVY)

draw.text((60, 2740), "Single command:", fill=DARK, font=F_SUB)
draw.text((260, 2742), "az deployment group create -g {rg} -f main.bicep -p environment=dev", fill=GRAY, font=F_BODY)

bicep_modules = [
    "main.bicep", "storage.bicep", "keyvault.bicep", "keyvault-rbac.bicep",
    "synapse.bicep", "sql-source.bicep", "data-factory.bicep",
    "function-app.bicep", "function-app-test.bicep", "monitoring.bicep", "openai.bicep",
]
bmx = 60
for bm in bicep_modules:
    pw = pill(bmx, 2775, bm, NAVY)
    bmx += pw + 6

# ── FOOTER BAR ──
draw.rectangle([0, 2850, W, 2900], fill=NAVY)
draw.text((30, 2862), "BI & Test Automation Platform -- Technical Architecture v9.0", fill=WHITE, font=F_BODY)
draw.text((W-450, 2862), "Confidential -- Solution Architects", fill=YELLOW, font=F_BODY)

# Crop to content
img = img.crop((0, 0, W, 2900))
out = os.path.join(os.path.dirname(__file__), "technical-architecture.png")
img.save(out, "PNG", optimize=True)
print(f"Saved: {out}  ({os.path.getsize(out)//1024} KB)")
