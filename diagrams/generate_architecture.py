"""Generate Technical + Executive architecture diagrams as vertical-flow PNGs."""
from PIL import Image, ImageDraw, ImageFont
import os

W, H = 4200, 6800
BG = "#0d1117"
CARD_BG = "#161b22"
BORDER = "#30363d"
WHITE = "#f0f6fc"
MUTED = "#8b949e"
BLUE = "#58a6ff"
GREEN = "#3fb950"
ORANGE = "#f0883e"
RED = "#da3633"
YELLOW = "#d29922"
PURPLE = "#bc8cff"
TEAL = "#39d2c0"
PINK = "#f778ba"

COLORS = {
    "external": "#1f6feb", "entra": "#da3633", "supervisor": "#bc8cff",
    "commander": "#f0883e", "worker": "#238636", "rag": "#d29922",
    "data": "#58a6ff", "storage": "#39d2c0", "security": "#da3633",
    "iac": "#8b949e",
}

try:
    FONT_B = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 28)
    FONT_M = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 22)
    FONT_S = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 18)
    FONT_XS = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 15)
    FONT_T = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 42)
    FONT_L = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 20)
except:
    FONT_B = FONT_M = FONT_S = FONT_XS = FONT_T = FONT_L = ImageFont.load_default()


def rounded_rect(draw, xy, fill, outline=None, r=12):
    x0, y0, x1, y1 = xy
    draw.rounded_rectangle(xy, radius=r, fill=fill, outline=outline, width=2)


def pill(draw, x, y, text, bg, fg="#ffffff", font=None):
    f = font or FONT_XS
    tw = draw.textlength(text, font=f)
    rounded_rect(draw, (x, y, x + tw + 16, y + 26), fill=bg, r=13)
    draw.text((x + 8, y + 4), text, fill=fg, font=f)
    return tw + 20


def section_header(draw, y, label, color, icon=""):
    rounded_rect(draw, (40, y, W - 40, y + 44), fill=color)
    draw.text((60, y + 8), f"{icon}  {label}", fill="#ffffff", font=FONT_B)
    return y + 44


def card(draw, x, y, w, h, title, lines, color=BLUE, title_font=None):
    rounded_rect(draw, (x, y, x + w, y + h), fill=CARD_BG, outline=BORDER)
    draw.line([(x, y + 36), (x + w, y + 36)], fill=color, width=3)
    draw.text((x + 12, y + 8), title, fill=color, font=title_font or FONT_L)
    for i, line in enumerate(lines):
        draw.text((x + 12, y + 44 + i * 22), line, fill=MUTED, font=FONT_XS)


def arrow_down(draw, x, y, length=40, color=MUTED):
    draw.line([(x, y), (x, y + length)], fill=color, width=3)
    draw.polygon([(x - 8, y + length - 8), (x + 8, y + length - 8), (x, y + length + 4)], fill=color)


def generate_technical():
    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img)

    # Title
    d.text((40, 20), "TECHNICAL ARCHITECTURE", fill=WHITE, font=FONT_T)
    d.text((W - 540, 30), "BI Automation Platform v10.0", fill=BLUE, font=FONT_B)
    y = 80

    # ── 1. EXTERNAL ACTORS ──
    y = section_header(d, y, "EXTERNAL ACTORS", COLORS["external"], "1")
    y += 10
    actors = [
        ("End Users", "Web UI (MSAL SSO)"), ("Azure DevOps", "REST API (PAT auth)"),
        ("Microsoft Teams", "Webhook + Bot"), ("QA Machine", "6-agent test framework"),
        ("Power BI", "DirectQuery to Gold"), ("SharePoint", "Graph API (M/I)")
    ]
    cw = (W - 120) // 6
    for i, (name, desc) in enumerate(actors):
        card(d, 50 + i * cw, y, cw - 10, 70, name, [desc], BLUE)
    y += 90
    arrow_down(d, W // 2, y, 30, BLUE)
    y += 45

    # ── 2. ENTRA ID ──
    y = section_header(d, y, "ENTRA ID (AZURE AD) — IDENTITY PERIMETER", COLORS["entra"], "2")
    y += 10
    items = [
        ("SSO + JWT Tokens", "User auth via MSAL.js"), ("Managed Identities", "Zero-trust service auth"),
        ("Key Vault (RBAC)", "Secrets: SQL, AI, ADO, Teams"), ("Monitoring", "App Insights + Log Analytics")
    ]
    cw = (W - 120) // 4
    for i, (name, desc) in enumerate(items):
        card(d, 50 + i * cw, y, cw - 10, 70, name, [desc], RED)
    y += 90
    arrow_down(d, W // 2, y, 30, RED)
    y += 45

    # ── 3. SUPERVISOR ──
    y = section_header(d, y, "SUPERVISOR — Independent Quality Watchdog", COLORS["supervisor"], "3")
    y += 10
    rounded_rect(d, (50, y, W - 50, y + 90), fill=CARD_BG, outline=PURPLE)
    d.text((70, y + 10), "Validates Commander plans | Enforces SLA | Overrides on quality failure | Final sign-off", fill=PURPLE, font=FONT_M)
    d.text((70, y + 40), "Independent from Commander — cannot be bypassed. Monitors all agent outputs before deploy.", fill=MUTED, font=FONT_S)
    d.text((70, y + 65), "Quality gates: schema validation, naming convention check, SQL injection scan, cardinality check", fill=MUTED, font=FONT_XS)
    y += 100
    arrow_down(d, W // 2, y, 30, PURPLE)
    y += 45

    # ── 4. COMMANDER ──
    y = section_header(d, y, "COMMANDER — Task Decomposition & Dynamic Dispatch", COLORS["commander"], "4")
    y += 10
    rounded_rect(d, (50, y, W // 2 + 50, y + 120), fill=CARD_BG, outline=ORANGE)
    d.text((70, y + 10), "Story in → LLM decomposes → dispatches agents → evaluates output", fill=ORANGE, font=FONT_M)
    d.text((70, y + 38), "Decisions: PROCEED | RETRY (with feedback) | REROUTE | ESCALATE", fill=MUTED, font=FONT_S)
    d.text((70, y + 62), "Pipeline: Interpret → Plan → REVIEW → Build → CodeReview → Deploy → Validate", fill=MUTED, font=FONT_XS)
    d.text((70, y + 82), "Self-Healing: Fail → Healer → Fix → Re-deploy (max 3 retries, then escalate)", fill=GREEN, font=FONT_XS)
    d.text((70, y + 100), "Integration: Discover → Conventions → Generate → Review → PR to ADO", fill=TEAL, font=FONT_XS)
    # Human Review Gate
    rounded_rect(d, (W // 2 + 80, y, W - 50, y + 120), fill="#1a1204", outline=YELLOW)
    d.text((W // 2 + 100, y + 10), "HUMAN REVIEW GATE", fill=YELLOW, font=FONT_B)
    d.text((W // 2 + 100, y + 45), "Approve / Decline via UI or Teams", fill=MUTED, font=FONT_S)
    d.text((W // 2 + 100, y + 72), "Approved plans → indexed into RAG KB", fill=GREEN, font=FONT_XS)
    d.text((W // 2 + 100, y + 92), "Declined plans → anti-patterns in KB", fill=RED, font=FONT_XS)
    y += 130
    arrow_down(d, W // 2, y, 30, ORANGE)
    y += 45

    # ── 5. RAG KNOWLEDGE BASE ──
    y = section_header(d, y, "RAG KNOWLEDGE BASE — Grounds ALL LLM Calls", COLORS["rag"], "5")
    y += 10
    # Main RAG box
    rounded_rect(d, (50, y, W - 50, y + 260), fill="#1a1608", outline=YELLOW)
    d.text((70, y + 10), "Every agent LLM call auto-retrieves grounding context from the knowledge base", fill=YELLOW, font=FONT_M)
    d.text((70, y + 38), "LLM prompt prefix: \"Use ONLY tables/columns from GROUNDING CONTEXT. Do NOT invent names.\"", fill=MUTED, font=FONT_S)

    # KB sources
    sources_y = y + 70
    src_w = (W - 160) // 4
    src_data = [
        ("BROWNFIELD", "Synapse INFORMATION_SCHEMA\ntables, columns, types,\ndistributions, constraints", BLUE),
        ("GREENFIELD", "Source DB scan (ODBC)\nIndustry templates\n(Retail/Finance/Healthcare/SaaS)", GREEN),
        ("INTEGRATION MODE", "Discovery Agent auto-index\nConvention Adapter rules\nADF pipelines, ADLS structure", TEAL),
        ("FEEDBACK LOOP", "Approved plans → indexed\nDeclined plans → anti-patterns\nValidator corrections → learning", ORANGE),
    ]
    for i, (title, desc, color) in enumerate(src_data):
        sx = 70 + i * src_w
        rounded_rect(d, (sx, sources_y, sx + src_w - 16, sources_y + 100), fill=CARD_BG, outline=color, r=8)
        d.text((sx + 10, sources_y + 6), title, fill=color, font=FONT_L)
        for j, line in enumerate(desc.split("\n")):
            d.text((sx + 10, sources_y + 30 + j * 18), line, fill=MUTED, font=FONT_XS)

    # Storage line
    stor_y = sources_y + 110
    d.text((70, stor_y), "Storage:", fill=YELLOW, font=FONT_L)
    d.text((170, stor_y), "Azure AI Search  index: rag-knowledge-base  (managed, durable)", fill=WHITE, font=FONT_S)
    d.text((170, stor_y + 22), "Hybrid search (vector + BM25 keyword) | Managed Identity auth | Auto-index creation on first deploy", fill=MUTED, font=FONT_XS)
    # Retrieval line
    d.text((70, stor_y + 48), "Retrieval:", fill=YELLOW, font=FONT_L)
    d.text((190, stor_y + 48), "Azure AI Search hybrid query (milliseconds) → top-15 schemas, glossary, joins, conventions → injected into prompt", fill=WHITE, font=FONT_S)
    y += 270
    arrow_down(d, W // 2, y, 30, YELLOW)
    y += 45

    # ── 6. WORKER AGENTS ──
    y = section_header(d, y, "WORKER AGENTS — Report Back to Commander", COLORS["worker"], "6")
    y += 10
    # Core pipeline agents
    d.text((60, y + 4), "Core Pipeline:", fill=GREEN, font=FONT_L)
    agents_core = [
        ("Planner", "Story → execution plan (LLM)"), ("Developer", "Plan → SQL + ADF code (LLM)"),
        ("Code Review", "AI code quality check (LLM)"), ("ADF Deployer", "Pipeline deploy (REST)"),
        ("SQL Deployer", "DDL to Synapse (ODBC)")
    ]
    aw = (W - 120) // 5
    for i, (name, desc) in enumerate(agents_core):
        card(d, 50 + i * aw, y + 28, aw - 10, 65, name, [desc], GREEN)
    y += 100
    # Validation & healing
    d.text((60, y + 4), "Validation & Healing:", fill=ORANGE, font=FONT_L)
    agents_vh = [
        ("Validator", "Schema + data quality checks"), ("Healer Agent", "Auto-fix failures (LLM, 3x)"),
        ("Bug Fixer", "ADO bug → AI fix → re-test"), ("Teams Notifier", "Adaptive cards (4 types)")
    ]
    aw = (W - 120) // 4
    for i, (name, desc) in enumerate(agents_vh):
        card(d, 50 + i * aw, y + 28, aw - 10, 65, name, [desc], ORANGE)
    y += 100
    # Integration & Ops
    d.text((60, y + 4), "Integration & Ops:", fill=TEAL, font=FONT_L)
    agents_io = [
        ("Discovery Agent", "Scan Synapse/ADF/ADLS → RAG"), ("Convention Adapter", "Detect patterns → RAG"),
        ("PR Delivery", "Push to ADO repo"), ("Test Automation", "6 agents: Router, Generator,\nExecutor, Planner, Reporter")
    ]
    aw = (W - 120) // 4
    for i, (name, desc) in enumerate(agents_io):
        lines = desc.split("\n")
        h = 65 + max(0, (len(lines) - 1) * 18)
        card(d, 50 + i * aw, y + 28, aw - 10, h, name, lines, TEAL)
    y += 115
    arrow_down(d, W // 2, y, 30, GREEN)
    y += 45

    # ── 7. DATA & PROCESSING TIER ──
    y = section_header(d, y, "DATA & PROCESSING TIER", COLORS["data"], "7")
    y += 10
    # Row 1: Synapse + ADF + AI
    dw = (W - 140) // 3
    # Synapse
    card(d, 50, y, dw, 180, "Azure Synapse Analytics", [
        "Dedicated SQL Pool DW100c | TDS 1.433",
        "",
        "BRONZE  External Tables → read Parquet from ADLS",
        "SILVER  Views — cleansed, deduplicated, typed",
        "GOLD    Views — business-ready, star schema",
        "",
        "Auth: SQL Auth (password from Key Vault)"
    ], BLUE)
    # ADF + ADLS
    card(d, 60 + dw, y, dw, 180, "Azure Data Factory + ADLS Gen2", [
        "ADF: Bronze ingestion pipelines",
        "  Source DB → Copy Activity → ADLS Parquet",
        "  Schedule: Daily trigger + 2 AM UTC",
        "  Auth: MI → Storage Blob Data Contributor",
        "",
        "ADLS Gen2 (Data Lake):",
        "  /bronze/ → raw Parquet files",
        "  /silver-staging/ → intermediate",
        "  /agent-artifacts/ → plans, reports",
        "  /agent-artifacts/ → plans, reports"
    ], BLUE)
    # Config DB + AI
    card(d, 70 + 2 * dw, y, dw, 180, "Config DB + Azure AI Foundry", [
        "Azure SQL (Config DB):",
        "  config.pipeline_registry, execution_log",
        "  config.artifact_versions, column_lineage",
        "  config.source_connectors, feedback",
        "  catalog.business_glossary, approved_joins",
        "  catalog.naming_conventions, deployment_log",
        "",
        "AI Foundry: Phi-4 / GPT-4o",
        "  Used by 9 LLM-powered agents",
        "  Auth: API Key from Key Vault"
    ], BLUE)
    y += 200
    arrow_down(d, W // 2, y, 30, BLUE)
    y += 45

    # ── 8. STORAGE MAP ──
    y = section_header(d, y, "COMPLETE STORAGE MAP", COLORS["storage"], "8")
    y += 10
    stores = [
        ("Synapse Dedicated Pool", "bipool", "Medallion data (stg/clean/rpt), config schema, catalog schema", BLUE),
        ("Azure AI Search", "rag-knowledge-base", "RAG vector + keyword index (managed), hybrid search", GREEN),
        ("Azure SQL (Config DB)", "config.*, catalog.*", "Pipeline state, lineage, glossary, connectors, feedback", ORANGE),
        ("Key Vault", "Secrets", "SQL passwords, ADO PAT, AI key, Teams webhook, connector creds", RED),
        ("Azure Storage (internal)", "Durable Functions", "Orchestration state, timer triggers, Commander/Supervisor state", PURPLE),
    ]
    sw = (W - 120) // 5
    for i, (name, detail, desc, color) in enumerate(stores):
        card(d, 50 + i * sw, y, sw - 10, 100, name, [detail, "", desc], color)
    y += 115
    arrow_down(d, W // 2, y, 30, TEAL)
    y += 45

    # ── 9. SECURITY & AUTH ──
    y = section_header(d, y, "SECURITY & AUTH MATRIX", COLORS["security"], "9")
    y += 10
    auth_rows = [
        ("Entra SSO", "User → Web UI", "MSAL.js → JWT", BLUE),
        ("Function Key", "UI → Function App", "x-functions-key header", GREEN),
        ("Managed ID", "Func → Key Vault", "System MI → RBAC Secrets User", PURPLE),
        ("SQL Auth", "Func → Synapse", "User+Password via ODBC (TLS 1.2)", ORANGE),
        ("API Key", "Func → AI Foundry", "AI_API_KEY header (HTTPS)", YELLOW),
        ("PAT", "Func → ADO", "Base64 Authorization header", RED),
        ("MI Bearer", "Func → ADF", "ManagedIdentityCredential → ARM", TEAL),
        ("MI RBAC", "ADF → ADLS", "Storage Blob Data Contributor", GREEN),
        ("Blob Lease", "Func → ADLS RAG", "30s lease for write concurrency", YELLOW),
    ]
    row_h = 26
    for i, (method, flow, detail, color) in enumerate(auth_rows):
        rx = 60
        ry = y + i * row_h
        pw = pill(d, rx, ry, method, color)
        d.text((rx + pw + 10, ry + 4), flow, fill=WHITE, font=FONT_XS)
        d.text((rx + 340, ry + 4), detail, fill=MUTED, font=FONT_XS)
    y += len(auth_rows) * row_h + 20

    # Network + Deploy modes side by side
    rounded_rect(d, (50, y, W // 2 - 20, y + 80), fill=CARD_BG, outline=BORDER)
    d.text((70, y + 8), "NETWORK STATUS", fill=RED, font=FONT_L)
    d.text((70, y + 32), "All public endpoints (no VNet)", fill=MUTED, font=FONT_S)
    d.text((70, y + 54), "Phase 2: VNet → Private endpoints + NSGs + Azure Firewall", fill=MUTED, font=FONT_XS)

    rounded_rect(d, (W // 2 + 20, y, W - 50, y + 80), fill=CARD_BG, outline=BORDER)
    d.text((W // 2 + 40, y + 8), "THREE DEPLOY MODES", fill=GREEN, font=FONT_L)
    modes = [("GREENFIELD", "Build from scratch → direct deploy", GREEN),
             ("BROWNFIELD", "Existing platform → additive only", ORANGE),
             ("INTEGRATION", "Client platform → PR delivery", TEAL)]
    for i, (m, desc, c) in enumerate(modes):
        pill(d, W // 2 + 40, y + 34 + i * 18, m, c)
        d.text((W // 2 + 180, y + 36 + i * 18), desc, fill=MUTED, font=FONT_XS)
    y += 95
    arrow_down(d, W // 2, y, 30, MUTED)
    y += 45

    # ── 10. IaC ──
    y = section_header(d, y, "INFRASTRUCTURE AS CODE — BICEP", COLORS["iac"], "10")
    y += 10
    d.text((60, y + 4), "Single command: az deployment group create -g {rg} -f main.bicep -p environment=dev", fill=MUTED, font=FONT_S)
    y += 30
    bicep_modules = ["main.bicep", "storage.bicep", "keyvault.bicep", "keyvault-rbac.bicep",
                     "synapse.bicep", "sql-source.bicep", "data-factory.bicep", "storage-rbac.bicep",
                     "function-app.bicep", "function-app-test.bicep", "monitoring.bicep", "openai.bicep"]
    bx = 60
    for mod in bicep_modules:
        pw = pill(d, bx, y, mod, "#21262d", MUTED)
        bx += pw + 6
        if bx > W - 200:
            bx = 60
            y += 30

    y += 50
    d.text((40, y), "BI & Test Automation Platform — Technical Architecture v10.0", fill=MUTED, font=FONT_S)
    d.text((W - 420, y), "Confidential — Solution Architects", fill=MUTED, font=FONT_S)

    out = os.path.join(os.path.dirname(__file__), "technical-architecture.png")
    img.save(out, "PNG", optimize=True)
    print(f"Technical: {out} ({os.path.getsize(out) // 1024} KB)")


def generate_executive():
    W2, H2 = 3800, 5800
    img = Image.new("RGB", (W2, H2), BG)
    d = ImageDraw.Draw(img)

    d.text((40, 20), "EXECUTIVE OVERVIEW", fill=WHITE, font=FONT_T)
    d.text((W2 - 480, 30), "For: Executive Management", fill=ORANGE, font=FONT_B)
    y = 80

    # 1. INPUT
    y = section_header(d, y, "1  INPUT — Users Submit Requirements", COLORS["external"], "")
    y += 10
    cw = (W2 - 120) // 3
    for i, (name, desc) in enumerate([("Web UI", "MSAL SSO login"), ("Azure DevOps", "Work item sync"), ("Microsoft Teams", "Adaptive card bot")]):
        card(d, 50 + i * cw, y, cw - 10, 60, name, [desc], BLUE)
    y += 70
    d.text((60, y), "Accepts:", fill=MUTED, font=FONT_S)
    bx = 160
    for fmt, c in [("Plain English", GREEN), ("Gherkin", BLUE), ("Bullets", ORANGE), ("Technical spec", PURPLE)]:
        bx += pill(d, bx, y - 2, fmt, c) + 6
    y += 30
    arrow_down(d, W2 // 2, y, 35, BLUE)
    y += 50

    # 2. COMMANDER
    y = section_header(d, y, "2  COMMANDER — The Brain", COLORS["commander"], "")
    y += 10
    rounded_rect(d, (50, y, W2 // 2 + 80, y + 100), fill=CARD_BG, outline=ORANGE)
    d.text((70, y + 10), "Decomposes story → dispatches agents → evaluates output", fill=ORANGE, font=FONT_M)
    d.text((70, y + 38), "Decisions:", fill=MUTED, font=FONT_S)
    bx = 200
    for dec, c in [("PROCEED", GREEN), ("RETRY", YELLOW), ("REROUTE", ORANGE), ("ESCALATE", RED)]:
        bx += pill(d, bx, y + 36, dec, c) + 6
    d.text((70, y + 68), "Steps: Interpret → Plan → Review → Build → CodeReview → Deploy → Validate", fill=MUTED, font=FONT_XS)
    # Human Review
    rounded_rect(d, (W2 // 2 + 110, y, W2 - 50, y + 100), fill="#1a1204", outline=YELLOW)
    d.text((W2 // 2 + 130, y + 10), "HUMAN REVIEW GATE", fill=YELLOW, font=FONT_B)
    d.text((W2 // 2 + 130, y + 42), "Approve / Decline via UI or Teams", fill=MUTED, font=FONT_S)
    d.text((W2 // 2 + 130, y + 68), "Approved → feeds RAG knowledge base", fill=GREEN, font=FONT_XS)
    y += 110
    arrow_down(d, W2 // 2, y, 35, ORANGE)
    y += 50

    # 3. RAG KNOWLEDGE BASE
    y = section_header(d, y, "3  RAG KNOWLEDGE BASE — Prevents Hallucination", COLORS["rag"], "")
    y += 10
    rounded_rect(d, (50, y, W2 - 50, y + 200), fill="#1a1608", outline=YELLOW)
    d.text((70, y + 10), "Grounds every AI decision in YOUR actual data — not guesses", fill=YELLOW, font=FONT_M)
    # 4 source cards
    sy = y + 44
    sw2 = (W2 - 160) // 4
    for i, (title, desc, color) in enumerate([
        ("Brownfield", "Scans your Synapse\ntables & columns", BLUE),
        ("Greenfield", "Source DB scan +\nindustry templates", GREEN),
        ("Integration", "Discovery Agent\nauto-indexes findings", TEAL),
        ("Feedback Loop", "Approved plans indexed\nDeclined → anti-patterns", ORANGE),
    ]):
        sx = 70 + i * sw2
        rounded_rect(d, (sx, sy, sx + sw2 - 16, sy + 70), fill=CARD_BG, outline=color, r=8)
        d.text((sx + 10, sy + 6), title, fill=color, font=FONT_L)
        for j, ln in enumerate(desc.split("\n")):
            d.text((sx + 10, sy + 30 + j * 18), ln, fill=MUTED, font=FONT_XS)
    d.text((70, sy + 82), "Stored on: Azure AI Search (managed service) — durable, no local cache needed, Managed Identity auth", fill=MUTED, font=FONT_S)
    d.text((70, sy + 108), "Retrieval: Hybrid search (vector + BM25 keyword) → top-15 relevant schemas injected into every LLM prompt", fill=MUTED, font=FONT_S)
    y += 210
    arrow_down(d, W2 // 2, y, 35, YELLOW)
    y += 50

    # 4. WORKER AGENTS
    y = section_header(d, y, "4  WORKER AGENTS — Do the Work", COLORS["worker"], "")
    y += 10
    d.text((60, y), "Each agent is specialized. Reports output back to Commander. Grounded by RAG.", fill=MUTED, font=FONT_S)
    y += 28
    agent_pills = [
        ("Planner", GREEN), ("Developer", GREEN), ("Code Review", GREEN), ("Deployer", BLUE), ("Validator", GREEN),
        ("Healer", ORANGE), ("Discovery", TEAL), ("Convention", PINK), ("PR Delivery", TEAL), ("Bug Fixer", ORANGE), ("Notifier", PURPLE)
    ]
    bx = 60
    for name, c in agent_pills:
        pw = pill(d, bx, y, name, c)
        bx += pw + 8
        if bx > W2 - 200:
            bx = 60
            y += 34
    y += 44
    arrow_down(d, W2 // 2, y, 35, GREEN)
    y += 50

    # 5. SUPERVISOR
    y = section_header(d, y, "5  SUPERVISOR — Validates & Enforces", COLORS["supervisor"], "")
    y += 10
    rounded_rect(d, (50, y, W2 - 50, y + 80), fill=CARD_BG, outline=PURPLE)
    d.text((70, y + 10), "Independent watchdog. Monitors Commander, enforces SLA, quality gates.", fill=PURPLE, font=FONT_M)
    bx = 70
    for item, c in [("Independent checks", GREEN), ("Override authority", RED), ("SLA enforcement", BLUE), ("Final sign-off", "#238636")]:
        bx += pill(d, bx, y + 44, item, c) + 8
    y += 95
    arrow_down(d, W2 // 2, y, 35, PURPLE)
    y += 50

    # 6. OUTPUT
    y = section_header(d, y, "6  OUTPUT — Data Platform (Auto-Built)", COLORS["data"], "")
    y += 10
    layers = [("BRONZE", "Raw data (Parquet from ADLS)", ORANGE), ("SILVER", "Cleansed, validated, typed", BLUE), ("GOLD", "Business-ready, star schema → Power BI", GREEN)]
    lw = (W2 - 120) // 3
    for i, (name, desc, c) in enumerate(layers):
        card(d, 50 + i * lw, y, lw - 10, 60, name, [desc], c)
    y += 75
    arrow_down(d, W2 // 2, y, 35, GREEN)
    y += 50

    # 7. STORAGE
    y = section_header(d, y, "7  WHERE DATA LIVES", COLORS["storage"], "")
    y += 10
    stores = [
        ("Synapse Pool", "Medallion data + config tables", BLUE),
        ("ADLS Gen2", "RAG index + bronze Parquet + plans", GREEN),
        ("Config DB (SQL)", "Pipeline state, lineage, glossary", ORANGE),
        ("Key Vault", "All secrets (zero plaintext)", RED),
        ("Azure Storage", "Durable Functions state", PURPLE),
    ]
    sw2 = (W2 - 120) // 5
    for i, (name, desc, c) in enumerate(stores):
        card(d, 50 + i * sw2, y, sw2 - 10, 60, name, [desc], c)
    y += 80
    arrow_down(d, W2 // 2, y, 35, TEAL)
    y += 50

    # 8. BUSINESS VALUE
    y = section_header(d, y, "8  BUSINESS VALUE", "#da3633", "")
    y += 10
    metrics = [("80%", "faster", "Weeks → hours"), ("100%", "auditable", "Full deploy trail"),
               ("0", "manual SQL", "AI writes all code"), ("3x", "self-heal", "Auto-fix failures"),
               ("3", "modes", "Green/Brown/Integration"), ("0", "hallucination", "RAG grounded")]
    mw = (W2 - 120) // 6
    for i, (val, label, desc) in enumerate(metrics):
        mx = 50 + i * mw
        d.text((mx + 10, y), val, fill=RED, font=FONT_T)
        d.text((mx + 10, y + 46), label, fill=WHITE, font=FONT_L)
        d.text((mx + 10, y + 68), desc, fill=MUTED, font=FONT_XS)
    y += 95
    d.text((60, y), "Human-in-the-loop: nothing deploys without explicit approval", fill=RED, font=FONT_M)
    y += 35

    # Deploy modes
    d.text((60, y), "Three Deploy Modes:", fill=WHITE, font=FONT_B)
    y += 30
    for mode, desc, c in [("GREENFIELD", "Build from scratch → direct deploy to Synapse (RAG uses templates + source scan)", GREEN),
                           ("BROWNFIELD", "Existing platform → additive only, check_object_exists() skips existing (RAG syncs Synapse)", ORANGE),
                           ("INTEGRATION", "Client platform → discover conventions → generate matching code → PR delivery (RAG from Discovery)", TEAL)]:
        pill(d, 60, y, mode, c)
        d.text((210, y + 4), desc, fill=MUTED, font=FONT_XS)
        y += 28
    y += 15

    # Security
    d.text((60, y), "Security — 6 Layers:", fill=WHITE, font=FONT_B)
    y += 30
    for layer, desc, c in [("USER AUTH", "Entra ID SSO → JWT", BLUE), ("SERVICE ID", "Managed Identity (zero passwords)", GREEN),
                            ("SECRETS", "Key Vault (MI access only)", RED), ("API", "Function Key on all endpoints", ORANGE),
                            ("DATA", "Parameterized SQL + input validation", YELLOW), ("OPS", "Secret health + audit trail + regression", PURPLE)]:
        pw = pill(d, 60, y, layer, c)
        d.text((60 + pw + 10, y + 4), desc, fill=MUTED, font=FONT_XS)
        y += 26
    y += 15

    # Ops
    d.text((60, y), "Operations & Cost:", fill=WHITE, font=FONT_B)
    y += 30
    ops = [("Auto-Pause Synapse", "Every 30 min → ~$870/mo saved"), ("Secret Health", "Daily 8 AM → Teams alert"),
           ("DB Cleanup", "Weekly → prune old data"), ("Self-Healing", "Automatic 3x retry")]
    ow = (W2 - 120) // 4
    for i, (name, desc) in enumerate(ops):
        card(d, 50 + i * ow, y, ow - 10, 55, name, [desc], TEAL)
    y += 70
    d.text((60, y), "EST. COST: ~$327-500/mo | Single Bicep deploy | Auto-pause cuts Synapse cost by 80%", fill=GREEN, font=FONT_M)
    y += 40
    d.text((40, y), "BI & Test Automation Platform — Executive Overview v10.0", fill=MUTED, font=FONT_S)
    d.text((W2 - 440, y), "Confidential — Executive Management", fill=MUTED, font=FONT_S)

    out = os.path.join(os.path.dirname(__file__), "executive-architecture.png")
    img.save(out, "PNG", optimize=True)
    print(f"Executive: {out} ({os.path.getsize(out) // 1024} KB)")


if __name__ == "__main__":
    generate_technical()
    generate_executive()
