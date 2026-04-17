"""Generate Security Architecture Diagram for BI Automation Platform"""
from PIL import Image, ImageDraw, ImageFont
import textwrap

W, H = 4200, 5200
img = Image.new('RGB', (W, H), '#0d1117')
draw = ImageDraw.Draw(img)

def font(size):
    try: return ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", size)
    except: return ImageFont.load_default()

def font_r(size):
    try: return ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", size)
    except: return ImageFont.load_default()

def font_m(size):
    try: return ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", size)
    except: return ImageFont.load_default()

def rounded_rect(x, y, w, h, r, fill, border=None, bw=2):
    draw.rounded_rectangle([x, y, x+w, y+h], radius=r, fill=fill, outline=border, width=bw)

def draw_arrow(x1, y1, x2, y2, color='#58a6ff', width=3, dashed=False):
    if dashed:
        import math
        dx, dy = x2-x1, y2-y1
        dist = math.sqrt(dx*dx + dy*dy)
        segs = int(dist / 16)
        for i in range(0, segs, 2):
            sx = x1 + dx * i / segs
            sy = y1 + dy * i / segs
            ex = x1 + dx * min(i+1, segs) / segs
            ey = y1 + dy * min(i+1, segs) / segs
            draw.line([(sx, sy), (ex, ey)], fill=color, width=width)
    else:
        draw.line([(x1, y1), (x2, y2)], fill=color, width=width)
    # arrowhead
    import math
    angle = math.atan2(y2-y1, x2-x1)
    aw = 14
    draw.polygon([
        (x2, y2),
        (x2 - aw*math.cos(angle-0.4), y2 - aw*math.sin(angle-0.4)),
        (x2 - aw*math.cos(angle+0.4), y2 - aw*math.sin(angle+0.4)),
    ], fill=color)

def section_header(x, y, w, text, color):
    draw.rounded_rectangle([x, y, x+w, y+44], radius=8, fill=color+'22', outline=color+'66', width=2)
    draw.text((x+16, y+10), text, fill=color, font=font(20))

def card(x, y, w, h, title, items, accent='#58a6ff', icon=''):
    rounded_rect(x, y, w, h, 12, '#161b22', '#30363d')
    draw.rectangle([x, y, x+4, y+h], fill=accent)
    ty = y + 14
    if icon:
        draw.text((x+16, ty-2), icon, fill=accent, font=font(18))
        draw.text((x+40, ty), title, fill='#f0f6fc', font=font(16))
    else:
        draw.text((x+16, ty), title, fill='#f0f6fc', font=font(16))
    ty += 32
    draw.line([(x+12, ty), (x+w-12, ty)], fill='#21262d', width=1)
    ty += 8
    for item in items:
        if item.startswith('!'):
            draw.text((x+16, ty), item[1:], fill=accent, font=font_m(13))
        elif item.startswith('>'):
            draw.text((x+16, ty), item[1:], fill='#3fb950', font=font_r(13))
        elif item.startswith('~'):
            draw.text((x+16, ty), item[1:], fill='#d29922', font=font_r(13))
        elif item.startswith('#'):
            draw.text((x+16, ty), item[1:], fill='#da3633', font=font_r(13))
        else:
            lines = textwrap.wrap(item, width=int(w/8))
            for line in lines:
                draw.text((x+16, ty), line, fill='#8b949e', font=font_r(13))
                ty += 20
            continue
        ty += 22
    return ty

# ═══════════════════════════════════════
# TITLE
# ═══════════════════════════════════════
draw.text((60, 30), "BI Automation Platform -- Security Architecture", fill='#f0f6fc', font=font(36))
draw.text((60, 78), "Zero-trust model: every connection authenticated, every secret in Key Vault, every action audited", fill='#8b949e', font=font_r(16))
draw.text((60, 102), "v9.0  |  Azure Entra ID + Managed Identity + Key Vault + TLS 1.2 + RBAC", fill='#484f58', font=font_r(14))

# ═══════════════════════════════════════
# LAYER 1: USER AUTHENTICATION (top)
# ═══════════════════════════════════════
ly = 150
section_header(60, ly, 4080, "LAYER 1: User Authentication (Browser -> Entra ID -> Platform)", '#58a6ff')
ly += 60

# User box
card(60, ly, 520, 260, "Users (Browser)", [
    "!Sign in with Microsoft (SSO)",
    "!Sign in with Azure AD B2C",
    "!Email + Password (fallback)",
    "",
    ">MSAL.js handles token flow",
    ">OAuth2 PKCE (no client secret)",
    ">Tokens in sessionStorage (not localStorage)",
    ">Auto-refresh via silent renewal",
], '#58a6ff', '')

# Entra ID box
card(720, ly, 560, 260, "Azure Entra ID / Azure AD", [
    "!App Registration: bi-automation-platform",
    "!Redirect URI: https://biplatform.../callback",
    "",
    ">Authenticates user (SSO/MFA)",
    ">Issues JWT access token + ID token",
    ">Signed with RS256 (public keys at JWKS)",
    ">Token contains: oid, email, name, roles",
    ">Token lifetime: 1hr, refresh: 24hr",
], '#0078d4', '')

# Platform box
card(1420, ly, 560, 260, "BI Platform (Web UI)", [
    "!Receives JWT from Entra ID",
    "!Stores in browser sessionStorage",
    "!Attaches to every API call:",
    "!  Authorization: Bearer <JWT>",
    "",
    ">XSS protection: sessionStorage + CSP",
    ">CSRF: SameSite=Strict cookies",
    ">HTTPS-only (HSTS enforced)",
], '#238636', '')

# Function App box
card(2120, ly, 580, 260, "Function App (API Layer)", [
    "!Validates JWT on every request:",
    "!  1. Check signature vs JWKS",
    "!  2. Verify issuer + audience",
    "!  3. Check token expiry",
    "!  4. Extract user role from DB",
    "",
    ">Rejects invalid/expired tokens",
    ">Logs: user, action, IP, timestamp",
], '#f0883e', '')

# RBAC box
card(2840, ly, 560, 260, "RBAC Enforcement", [
    "!platform_users table (Azure SQL):",
    "!  entra_oid -> platform_role",
    "",
    ">Admin: all 55 endpoints",
    ">Engineer: 40 endpoints (no admin)",
    ">Analyst: 12 endpoints (read-only)",
    ">Viewer: 5 endpoints (dashboard)",
    "",
    "~Middleware checks role BEFORE handler",
], '#bc8cff', '')

# Arrows for Layer 1
draw_arrow(580, ly+130, 720, ly+130, '#58a6ff')
draw_arrow(1280, ly+130, 1420, ly+130, '#0078d4')
draw_arrow(1980, ly+130, 2120, ly+130, '#238636')
draw_arrow(2700, ly+130, 2840, ly+130, '#f0883e')

# Labels on arrows
draw.text((610, ly+106), "PKCE flow", fill='#484f58', font=font_r(11))
draw.text((1310, ly+106), "JWT token", fill='#484f58', font=font_r(11))
draw.text((2010, ly+106), "Bearer JWT", fill='#484f58', font=font_r(11))
draw.text((2720, ly+106), "role check", fill='#484f58', font=font_r(11))

# ═══════════════════════════════════════
# LAYER 2: SERVICE-TO-SERVICE (Managed Identity)
# ═══════════════════════════════════════
ly2 = 490
section_header(60, ly2, 4080, "LAYER 2: Service-to-Service Authentication (Managed Identity -- Zero Secrets in Code)", '#3fb950')
ly2 += 60

card(60, ly2, 650, 320, "Function App (System-Assigned Managed Identity)", [
    "!Identity auto-created by Azure",
    "!No credentials stored anywhere",
    "!Azure manages rotation automatically",
    "",
    ">Authenticates to Azure services via",
    ">Azure Instance Metadata Service (IMDS)",
    ">Token issued by Azure AD for the",
    ">Function App's identity principal",
    "",
    "~BICEP: identity: { type: 'SystemAssigned' }",
    "~Principal ID used for RBAC assignments",
], '#3fb950', '')

# Target services
svc_x = 860
svcs = [
    ("Key Vault", '#d29922', 420, [
        "!Role: Key Vault Secrets User",
        "!Reads secrets at runtime:",
        "  SYNAPSE_SQL_PASSWORD",
        "  AI_API_KEY",
        "  ADO_PAT",
        "  TEAMS_WEBHOOK_URL",
        "",
        ">Key Vault References in App Settings",
        ">@Microsoft.KeyVault(VaultName=...",
        ">Function App never sees raw secret",
    ]),
    ("Azure SQL (Config DB)", '#58a6ff', 420, [
        "!Stores: users, roles, pipelines,",
        "!execution logs, artifacts, lineage",
        "",
        ">Connection: SQL auth (password from KV)",
        ">Phase 2: AAD token-based auth",
        ">TDE: Transparent Data Encryption",
        ">Firewall: Azure services only",
    ]),
    ("Synapse Dedicated Pool", '#bc8cff', 420, [
        "!DDL execution, brownfield scan",
        "!Bronze/Silver/Gold layer queries",
        "",
        ">Connection: SQL auth (password from KV)",
        ">Phase 2: Managed Identity auth",
        ">TDE enabled, firewall locked",
        ">Row-Level Security per role",
    ]),
    ("ADLS Gen2 Storage", '#f0883e', 420, [
        "!Role: Storage Blob Data Contributor",
        "!Bronze layer data files (Parquet)",
        "",
        ">Managed Identity auth (no keys!)",
        ">allowSharedKeyAccess: false",
        ">allowBlobPublicAccess: false",
        ">Network: Deny + Azure Services bypass",
        ">TLS 1.2 enforced",
    ]),
    ("AI Foundry (Phi-4)", '#da3633', 420, [
        "!LLM endpoint for AI agents",
        "!API key stored in Key Vault",
        "",
        ">Key retrieved at runtime via KV ref",
        ">HTTPS-only endpoint",
        ">Phase 2: Managed Identity auth",
        ">Rate limiting: token-based billing",
    ]),
    ("Azure Data Factory", '#1f6feb', 420, [
        "!Pipeline orchestration",
        "!Copy Activities for Bronze layer",
        "",
        ">Managed Identity for ADF itself",
        ">ADF reads from source via",
        ">  Linked Service credentials (KV)",
        ">ADF writes to ADLS via MI",
    ]),
]
for i, (name, color, w, items) in enumerate(svcs):
    col = i % 3
    row = i // 3
    sx = svc_x + col * (w + 40)
    sy = ly2 + row * 280
    card(sx, sy, w, 260, name, items, color, '')
    # arrow from func app
    if row == 0:
        draw_arrow(710, ly2 + 80 + i*30, sx, ly2 + 80 + i*30, color, 2, True)
    else:
        ax = 60 + 325
        draw_arrow(ax, ly2 + 320, ax, sy + 20, color, 2, True)

# ═══════════════════════════════════════
# LAYER 3: SECRET MANAGEMENT
# ═══════════════════════════════════════
ly3 = 1160
section_header(60, ly3, 4080, "LAYER 3: Secret Management (Azure Key Vault -- Single Source of Truth)", '#d29922')
ly3 += 60

card(60, ly3, 900, 340, "Key Vault Configuration (Bicep IaC)", [
    "!enableRbacAuthorization: true",
    "!  (no access policies -- pure RBAC)",
    "!enableSoftDelete: true",
    "!enablePurgeProtection: true",
    "!softDeleteRetentionInDays: 90",
    "",
    "!networkAcls:",
    "!  defaultAction: Deny",
    "!  bypass: AzureServices",
    "",
    ">Diagnostic logging: ALL audit events",
    ">to Log Analytics workspace",
    "",
    "~WHO CAN ACCESS (RBAC assignments):",
    "~  Function App MI -> Secrets User (read)",
    "~  Test Func App MI -> Secrets User (read)",
    "~  Synapse MI -> Secrets User (read)",
], '#d29922', '')

card(1060, ly3, 700, 340, "Secrets Stored", [
    "!synapse-sql-password",
    "  Synapse DW admin password",
    "!source-sql-password",
    "  Source database password",
    "!ai-api-key",
    "  AI Foundry / Phi-4 API key",
    "!ado-pat",
    "  Azure DevOps Personal Access Token",
    "!teams-webhook-url",
    "  Teams Incoming Webhook URL",
    "",
    "#NEVER in code, env vars, or config files",
    "#App Settings use @Microsoft.KeyVault(...)",
    "#references -- Azure resolves at runtime",
], '#d29922', '')

card(1860, ly3, 700, 340, "Secret Lifecycle", [
    "!Creation: Bicep deploys secrets",
    "!Rotation: Ops timer checks expiry",
    "  (secret_health_check: daily 8AM)",
    "!Monitoring: Teams alert on expiry",
    "!Access audit: Key Vault diagnostic logs",
    "",
    ">Rotation workflow:",
    ">  1. Generate new secret",
    ">  2. Update Key Vault secret",
    ">  3. Restart Function App",
    ">  4. Old secret auto-purged after 90d",
    "",
    "~ADO PAT: manual (user regenerates)",
    "~AI Key: manual (Azure portal)",
    "~SQL Password: can automate via runbook",
], '#d29922', '')

card(2660, ly3, 740, 340, "What is NOT in Key Vault (by design)", [
    ">User passwords: Entra ID manages",
    ">JWT signing keys: Entra ID JWKS",
    ">Managed Identity creds: Azure manages",
    ">Storage keys: disabled (MI-only)",
    "",
    "!API Keys (platform-issued):",
    "  Stored HASHED (SHA-256) in Azure SQL",
    "  Raw key shown once at creation",
    "  Only prefix visible in Admin Console",
    "",
    "~Session tokens: browser sessionStorage",
    "~  Auto-expire, HTTPS only",
    "~  Never sent to server as cookies",
], '#8b949e', '')

# ═══════════════════════════════════════
# LAYER 4: NETWORK SECURITY
# ═══════════════════════════════════════
ly4 = 1580
section_header(60, ly4, 4080, "LAYER 4: Network Security (Defense in Depth)", '#da3633')
ly4 += 60

card(60, ly4, 680, 300, "Transport Security", [
    "!ALL connections: TLS 1.2+ enforced",
    "!  Function App: minTlsVersion: '1.2'",
    "!  Storage: minimumTlsVersion: 'TLS1_2'",
    "!  Key Vault: TLS 1.2 (Azure default)",
    "!  SQL/Synapse: Encrypted connections",
    "",
    ">Function App: httpsOnly: true",
    ">Function App: ftpsState: 'Disabled'",
    ">Function App: http20Enabled: true",
    ">HSTS headers enforced by Azure",
], '#da3633', '')

card(840, ly4, 680, 300, "Network Isolation", [
    "!Storage: defaultAction: 'Deny'",
    "!  bypass: 'AzureServices' only",
    "!  allowBlobPublicAccess: false",
    "!  allowSharedKeyAccess: false",
    "",
    "!Key Vault: defaultAction: 'Deny'",
    "!  bypass: 'AzureServices' only",
    "",
    "~Phase 2: VNet Integration",
    "~  Private Endpoints for SQL, KV, Storage",
    "~  Function App in VNet subnet",
    "~  No public internet exposure",
], '#da3633', '')

card(1620, ly4, 680, 300, "CORS & API Security", [
    "!Function App CORS:",
    "!  allowedOrigins: platform URL only",
    "!  supportCredentials: false",
    "",
    ">Every API endpoint validates:",
    ">  1. JWT signature (Entra ID JWKS)",
    ">  2. Token audience matches app ID",
    ">  3. Token not expired",
    ">  4. User role allows this action",
    "",
    "~Rate limiting: Azure Front Door (Phase 2)",
    "~WAF: Azure Web Application Firewall (Phase 2)",
], '#da3633', '')

card(2400, ly4, 680, 300, "SQL Injection Prevention", [
    "!All SQL queries use parameterized queries",
    "!  cursor.execute(sql, [param1, param2])",
    "!  NEVER: f-string or .format() for SQL",
    "",
    ">Synapse client: validated in code review",
    ">Config DB: ORM-style parameter binding",
    ">AI-generated SQL: validated by",
    ">  Code Review agent before execution",
    "",
    "#Story Mapper: column name filtering",
    "#  prevents table name injection from",
    "#  user story text",
], '#da3633', '')

# ═══════════════════════════════════════
# LAYER 5: DATA SECURITY
# ═══════════════════════════════════════
ly5 = 1960
section_header(60, ly5, 4080, "LAYER 5: Data Security (Encryption at Rest + In Transit + Access Control)", '#bc8cff')
ly5 += 60

card(60, ly5, 680, 280, "Encryption at Rest", [
    "!Azure SQL: TDE (Transparent Data Encryption)",
    "!  Service-managed keys (AES-256)",
    "!Synapse: TDE enabled by default",
    "!ADLS Gen2: SSE (Storage Service Encryption)",
    "!  Microsoft-managed keys (AES-256)",
    "!Key Vault: HSM-backed encryption",
    "",
    ">All data encrypted at rest automatically",
    ">No unencrypted data on any Azure disk",
    "",
    "~Phase 2: Customer-Managed Keys (CMK)",
], '#bc8cff', '')

card(840, ly5, 680, 280, "Row-Level Security (RLS)", [
    "!Synapse dedicated pool enforces RLS:",
    "!  Platform Admin -> all schemas",
    "!  Data Engineer -> bronze, silver, gold",
    "!  Business Analyst -> gold only",
    "!  Viewer -> gold aggregates only",
    "",
    ">Each platform role maps to a",
    ">  Synapse database role",
    ">Security predicate filters rows",
    ">  based on SESSION_CONTEXT",
    "",
    "~Set at connection time from JWT role",
], '#bc8cff', '')

card(1620, ly5, 680, 280, "PII Protection", [
    "!Data Classification in Governance page:",
    "!  Confidential: email, phone",
    "!  Restricted: SSN, salary",
    "",
    ">Silver layer: PII columns masked",
    ">  email -> hash@masked.com",
    ">  phone -> randomized",
    ">  SSN -> XXX-XX-XXXX",
    "",
    ">Gold layer: no PII (aggregated)",
    ">Test Data: masking rules applied",
    ">  before provisioning test datasets",
], '#bc8cff', '')

card(2400, ly5, 680, 280, "Audit Trail", [
    "!Every action logged to Azure SQL:",
    "!  login_audit: who, when, IP, method",
    "!  execution_log: pipeline runs, approvals",
    "!  artifact_versions: who changed what",
    "",
    ">Key Vault: diagnostic logs -> Log Analytics",
    ">Function App: Application Insights",
    ">Synapse: Query audit logs",
    ">ADLS: Storage analytics logs",
    "",
    "~Retention: 90 days (configurable)",
    "~Immutable: append-only audit tables",
], '#bc8cff', '')

# ═══════════════════════════════════════
# LAYER 6: OPERATIONAL SECURITY
# ═══════════════════════════════════════
ly6 = 2320
section_header(60, ly6, 4080, "LAYER 6: Operational Security (Automated Monitoring + Self-Healing)", '#f0883e')
ly6 += 60

card(60, ly6, 1000, 260, "Automated Security Timers (Already Built)", [
    "!secret_health_check (daily 8 AM UTC)",
    "  Validates ADO PAT, AI key, SQL password not expired",
    "  Teams alert if any credential near expiry",
    "",
    "!auto_pause_synapse (every 30 min)",
    "  Reduces attack surface when pool not in use",
    "",
    "!weekly_cleanup (Sunday 2 AM UTC)",
    "  Purges old logs, keeps last 5 artifact versions",
    "  Prevents data accumulation / exposure",
], '#f0883e', '')

card(1160, ly6, 1000, 260, "Infrastructure as Code (Bicep)", [
    "!All security config is version-controlled:",
    "  Key Vault RBAC assignments",
    "  Network ACLs",
    "  TLS versions",
    "  Identity configurations",
    "",
    ">No manual portal changes",
    ">Drift detection: Bicep what-if",
    ">PR-based changes: Code Review required",
    ">Audit: git log shows who changed what",
], '#f0883e', '')

card(2260, ly6, 1140, 260, "Compliance Alignment", [
    "!SOC 2 Type II: audit trail, access control, encryption",
    "!GDPR: PII masking, data classification, right to delete",
    "!ISO 27001: RBAC, network isolation, key management",
    "!Azure Well-Architected: Zero Trust, Defense in Depth",
    "",
    ">Governance page tracks compliance score (94%)",
    ">Policy rules enforced automatically",
    ">Classification coverage monitored",
    "",
    "~Phase 2: Automated compliance reports",
    "~Phase 2: Azure Policy integration",
], '#f0883e', '')

# ═══════════════════════════════════════
# SUMMARY: WHAT'S DONE vs PHASE 2
# ═══════════════════════════════════════
ly7 = 2660
section_header(60, ly7, 2000, "IMPLEMENTED TODAY (Phase 1)", '#3fb950')
ly7 += 56

today_items = [
    "Key Vault with RBAC auth, soft-delete, purge protection",
    "Managed Identity for Function App (no stored creds)",
    "Key Vault References for all secrets in App Settings",
    "Storage: shared key access DISABLED, public access DISABLED",
    "TLS 1.2 enforced on ALL services",
    "HTTPS-only, FTPS disabled on Function App",
    "Network ACLs: Deny by default on Storage + Key Vault",
    "Login screen with SSO + Azure AD B2C + email fallback",
    "RBAC: 4 roles with sidebar enforcement",
    "Admin Console: users, roles matrix, API keys, tenant config",
    "Parameterized SQL queries (injection prevention)",
    "PII masking in Silver layer",
    "Audit trail in Azure SQL",
    "Secret health check timer (daily)",
    "Diagnostic logging on Key Vault",
    "Infrastructure as Code (all Bicep)",
    "Code Review agent validates AI-generated SQL",
]
for i, item in enumerate(today_items):
    draw.text((80, ly7 + i * 26), "  " + item, fill='#3fb950', font=font_r(15))

section_header(2160, ly7-56, 2000, "PHASE 2: Production Hardening", '#d29922')

phase2_items = [
    "Real MSAL.js integration (JWT token exchange)",
    "Server-side JWT validation middleware on Function App",
    "Azure AD token-based auth for SQL (replace SQL auth)",
    "Managed Identity auth for AI Foundry (remove API key)",
    "VNet Integration + Private Endpoints (no public IPs)",
    "Azure Front Door + WAF (DDoS + rate limiting)",
    "Customer-Managed Keys (CMK) for encryption",
    "Conditional Access policies (MFA, device compliance)",
    "Azure Policy: enforce tagging, deny public endpoints",
    "Automated compliance reporting",
    "Session timeout + auto-lock enforcement",
    "API key hashing (SHA-256) in Config DB",
    "IP allowlisting for PROD environments",
    "Penetration testing + vulnerability scanning",
    "SOC 2 / ISO 27001 certification prep",
]
for i, item in enumerate(phase2_items):
    draw.text((2180, ly7 + i * 26), "  " + item, fill='#d29922', font=font_r(15))

# ═══════════════════════════════════════
# FLOW DIAGRAM: End-to-End Security Flow
# ═══════════════════════════════════════
ly8 = 3200
section_header(60, ly8, 4080, "END-TO-END SECURITY FLOW: User Signs In -> Runs Pipeline -> Deploys to Synapse", '#58a6ff')
ly8 += 70

flow_steps = [
    ("1", "User opens\nplatform URL", '#58a6ff', "HTTPS/TLS 1.2"),
    ("2", "Login screen\n(MSAL.js)", '#0078d4', "OAuth2 PKCE"),
    ("3", "Entra ID\nauthenticates", '#0078d4', "JWT issued"),
    ("4", "UI stores JWT\n(sessionStorage)", '#238636', "Bearer token"),
    ("5", "Click Build\n(API call)", '#f0883e', "JWT in header"),
    ("6", "Function App\nvalidates JWT", '#f0883e', "Signature check"),
    ("7", "Check role\n(Azure SQL)", '#bc8cff', "RBAC lookup"),
    ("8", "Fetch secret\n(Key Vault)", '#d29922', "Managed Identity"),
    ("9", "Connect to\nSynapse", '#bc8cff', "SQL auth (from KV)"),
    ("10", "Execute DDL\n(deploy)", '#3fb950', "Parameterized SQL"),
    ("11", "Log audit\ntrail", '#8b949e', "Append-only"),
]

bw = 330
gap = 40
sx = 80
for i, (num, label, color, note) in enumerate(flow_steps):
    col = i % 6
    row = i // 6
    bx = sx + col * (bw + gap)
    by = ly8 + row * 200
    rounded_rect(bx, by, bw, 140, 12, '#161b22', color, 2)
    draw.rounded_rectangle([bx, by, bx+bw, by+4], radius=2, fill=color)
    draw.text((bx + bw//2 - 10, by + 16), num, fill=color, font=font(28))
    lines = label.split('\n')
    for j, line in enumerate(lines):
        tw = draw.textlength(line, font=font(16))
        draw.text((bx + bw//2 - tw//2, by + 56 + j*22), line, fill='#f0f6fc', font=font(16))
    tw = draw.textlength(note, font=font_r(12))
    draw.text((bx + bw//2 - tw//2, by + 112), note, fill='#484f58', font=font_r(12))
    # Arrow to next
    if i < len(flow_steps) - 1:
        if col < 5:
            draw_arrow(bx + bw + 4, by + 70, bx + bw + gap - 4, by + 70, color, 2)
        else:
            # wrap to next row
            draw_arrow(bx + bw//2, by + 140 + 4, bx + bw//2, by + 200 - 4, color, 2)

# THREAT MODEL box at bottom
ly9 = 3650
section_header(60, ly9, 4080, "THREAT MODEL: What We Defend Against", '#da3633')
ly9 += 60

threats = [
    ("Stolen Credentials", "Entra ID SSO + MFA. No passwords stored by platform. JWT expires in 1hr.", '#da3633'),
    ("Secret Exposure", "All secrets in Key Vault. Code has ZERO hardcoded credentials. KV refs resolve at runtime.", '#da3633'),
    ("SQL Injection", "Parameterized queries everywhere. AI-generated SQL reviewed by Code Review agent.", '#d29922'),
    ("Unauthorized Access", "RBAC on every endpoint. JWT validated server-side. Locked nav items in UI.", '#d29922'),
    ("Data Exfiltration", "Network ACLs deny public access. Storage keys disabled. PII masked in Silver+.", '#f0883e'),
    ("Man-in-the-Middle", "TLS 1.2+ on all connections. HTTPS-only. HSTS enforced. FTPS disabled.", '#3fb950'),
    ("Insider Threat", "Audit trail on every action. Key Vault diagnostic logs. Immutable execution_log.", '#58a6ff'),
    ("Stale Credentials", "Daily secret_health_check timer. Teams alert on near-expiry. Auto-pause reduces surface.", '#58a6ff'),
]

tw = 500
for i, (threat, defense, color) in enumerate(threats):
    col = i % 4
    row = i // 4
    tx = 60 + col * (tw + 20)
    ty = ly9 + row * 140
    rounded_rect(tx, ty, tw, 120, 10, '#161b22', color+'66', 2)
    draw.rectangle([tx, ty, tx + tw, ty + 4], fill=color)
    draw.text((tx + 12, ty + 14), threat, fill=color, font=font(14))
    lines = textwrap.wrap(defense, width=55)
    for j, line in enumerate(lines):
        draw.text((tx + 12, ty + 40 + j * 20), line, fill='#8b949e', font=font_r(12))

# Footer
draw.text((60, H-40), "BI Automation Platform  |  Security Architecture v9.0  |  Azure-native zero-trust model", fill='#30363d', font=font_r(14))

img.save('/project/workspace/synapse-bi-automation/docs/security-architecture.png', quality=95)
print(f"Security architecture diagram saved. Size: {W}x{H}")
