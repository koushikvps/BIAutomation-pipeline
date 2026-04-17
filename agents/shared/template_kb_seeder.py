"""Template KB Seeder: pre-loads the RAG knowledge base with industry patterns.

For greenfield deployments where no client metadata exists yet, this seeds
the knowledge base with:
  - Medallion architecture conventions (Bronze/Silver/Gold)
  - Common industry schema templates (Retail, Finance, Healthcare, SaaS)
  - Standard naming conventions
  - Common business glossary terms
  - Best-practice join patterns
  - SCD patterns (Type 1, Type 2)

The LLM uses these as grounding when no client-specific data is available.
"""
from __future__ import annotations

import logging
from typing import Optional

from .rag_retriever import RAGRetriever, RAGDocument, DocumentType

logger = logging.getLogger(__name__)

MEDALLION_CONVENTIONS = [
    {
        "type": "layer_convention",
        "content": "Convention: Medallion Architecture Layers\n"
                   "Pattern: Bronze (raw/staging) → Silver (cleansed/conformed) → Gold (business-ready)\n"
                   "Bronze schemas: stg, raw, landing, bronze\n"
                   "Silver schemas: clean, cleansed, conformed, silver\n"
                   "Gold schemas: rpt, report, analytics, gold, mart, presentation",
    },
    {
        "type": "naming_convention",
        "content": "Convention: Table Naming\n"
                   "Pattern: {layer_prefix}_{entity_name}\n"
                   "Examples: stg_customers, clean_orders, rpt_daily_revenue\n"
                   "Case: snake_case for all objects\n"
                   "Avoid: spaces, special characters, reserved words",
    },
    {
        "type": "naming_convention",
        "content": "Convention: View Naming\n"
                   "Pattern: vw_{purpose}_{entity}\n"
                   "Examples: vw_rpt_customer_summary, vw_dim_product\n"
                   "Views are used in Gold layer for reporting access",
    },
    {
        "type": "naming_convention",
        "content": "Convention: Stored Procedure Naming\n"
                   "Pattern: usp_{layer}_{action}_{entity}\n"
                   "Examples: usp_stg_load_customers, usp_clean_transform_orders, usp_rpt_refresh_revenue",
    },
    {
        "type": "naming_convention",
        "content": "Convention: Column Naming\n"
                   "Pattern: snake_case, descriptive, no abbreviations\n"
                   "Primary keys: {entity}_id (e.g., customer_id, order_id)\n"
                   "Foreign keys: same name as referenced PK\n"
                   "Dates: {event}_dt or {event}_date (e.g., order_dt, created_date)\n"
                   "Booleans: is_{condition} or has_{condition} (e.g., is_active, has_subscription)\n"
                   "Amounts: {measure}_amount or {measure}_amt (e.g., total_amount)\n"
                   "Counts: {entity}_count or {entity}_cnt",
    },
    {
        "type": "distribution_convention",
        "content": "Convention: Synapse Distribution Strategy\n"
                   "Pattern:\n"
                   "  - HASH(customer_id) for large fact tables with frequent customer joins\n"
                   "  - HASH(order_id) for order detail tables\n"
                   "  - REPLICATE for small dimension tables (< 60K rows)\n"
                   "  - ROUND_ROBIN for staging/landing tables (fast bulk insert)\n"
                   "Rule: Hash on the most common join key; replicate dims under 60K rows",
    },
    {
        "type": "scd_convention",
        "content": "Convention: Slowly Changing Dimensions\n"
                   "SCD Type 1: Overwrite in place. No history. Use for corrections.\n"
                   "  Pattern: MERGE ... WHEN MATCHED THEN UPDATE\n"
                   "SCD Type 2: Full history with effective_from, effective_to, is_current.\n"
                   "  Pattern: MERGE ... WHEN MATCHED AND changed THEN UPDATE effective_to, INSERT new row\n"
                   "  Required columns: effective_from DATE, effective_to DATE, is_current BIT\n"
                   "  Convention: effective_to = '9999-12-31' for current rows",
    },
    {
        "type": "incremental_convention",
        "content": "Convention: Incremental Load Pattern\n"
                   "Pattern: Watermark-based using a monotonically increasing column\n"
                   "  1. Read last watermark from config.pipeline_watermarks\n"
                   "  2. SELECT * FROM source WHERE modified_dt > @last_watermark\n"
                   "  3. MERGE into target\n"
                   "  4. Update watermark on success\n"
                   "Watermark columns: modified_dt, updated_at, last_modified, change_date",
    },
]

INDUSTRY_TEMPLATES = {
    "retail": [
        {"doc_type": "table_schema", "content": "TEMPLATE [stg].[stg_customers]\nColumns:\n  customer_id INT NOT NULL\n  first_name NVARCHAR(100)\n  last_name NVARCHAR(100)\n  email NVARCHAR(200)\n  phone NVARCHAR(20)\n  address_line1 NVARCHAR(200)\n  city NVARCHAR(100)\n  state NVARCHAR(50)\n  postal_code NVARCHAR(20)\n  country NVARCHAR(50)\n  created_date DATE\n  modified_date DATE"},
        {"doc_type": "table_schema", "content": "TEMPLATE [stg].[stg_orders]\nColumns:\n  order_id INT NOT NULL\n  customer_id INT NOT NULL FK→stg_customers\n  order_dt DATE NOT NULL\n  status NVARCHAR(20)\n  shipping_method NVARCHAR(50)\n  total_amount DECIMAL(18,2)\n  tax_amount DECIMAL(18,2)\n  discount_amount DECIMAL(18,2)\n  created_date DATE\n  modified_date DATE"},
        {"doc_type": "table_schema", "content": "TEMPLATE [stg].[stg_order_items]\nColumns:\n  order_item_id INT NOT NULL\n  order_id INT NOT NULL FK→stg_orders\n  product_id INT NOT NULL FK→stg_products\n  quantity INT NOT NULL\n  unit_price DECIMAL(18,2)\n  line_total DECIMAL(18,2)\n  discount_pct DECIMAL(5,2)"},
        {"doc_type": "table_schema", "content": "TEMPLATE [stg].[stg_products]\nColumns:\n  product_id INT NOT NULL\n  product_name NVARCHAR(200)\n  category NVARCHAR(100)\n  subcategory NVARCHAR(100)\n  brand NVARCHAR(100)\n  unit_price DECIMAL(18,2)\n  cost_price DECIMAL(18,2)\n  is_active BIT\n  created_date DATE"},
        {"doc_type": "table_schema", "content": "TEMPLATE [rpt].[rpt_daily_sales]\nColumns:\n  sale_date DATE NOT NULL\n  region NVARCHAR(50)\n  category NVARCHAR(100)\n  total_revenue DECIMAL(18,2)\n  total_orders INT\n  avg_order_value DECIMAL(18,2)\n  total_units_sold INT"},
        {"doc_type": "business_term", "content": "Business Term: Revenue\nDefinition: SUM(line_total) from order items for completed orders\nFormula: SUM(oi.line_total) WHERE o.status = 'completed'\nCategory: Finance"},
        {"doc_type": "business_term", "content": "Business Term: AOV (Average Order Value)\nDefinition: Average revenue per order\nFormula: SUM(total_amount) / COUNT(DISTINCT order_id)\nCategory: Finance"},
        {"doc_type": "business_term", "content": "Business Term: Customer Lifetime Value (CLV)\nDefinition: Total revenue from a customer across all orders\nFormula: SUM(total_amount) GROUP BY customer_id\nCategory: Customer Analytics"},
        {"doc_type": "approved_join", "content": "JOIN: [stg_orders].[customer_id] INNER JOIN [stg_customers].[customer_id]\nDescription: Link orders to customer demographics"},
        {"doc_type": "approved_join", "content": "JOIN: [stg_order_items].[order_id] INNER JOIN [stg_orders].[order_id]\nDescription: Link line items to order header"},
        {"doc_type": "approved_join", "content": "JOIN: [stg_order_items].[product_id] INNER JOIN [stg_products].[product_id]\nDescription: Link line items to product catalog"},
    ],
    "finance": [
        {"doc_type": "table_schema", "content": "TEMPLATE [stg].[stg_accounts]\nColumns:\n  account_id INT NOT NULL\n  account_number NVARCHAR(20)\n  account_type NVARCHAR(50)\n  customer_id INT FK→stg_customers\n  currency_code NVARCHAR(3)\n  opened_date DATE\n  status NVARCHAR(20)\n  balance DECIMAL(18,2)"},
        {"doc_type": "table_schema", "content": "TEMPLATE [stg].[stg_transactions]\nColumns:\n  transaction_id BIGINT NOT NULL\n  account_id INT NOT NULL FK→stg_accounts\n  transaction_dt DATETIME2 NOT NULL\n  transaction_type NVARCHAR(20)\n  amount DECIMAL(18,2)\n  running_balance DECIMAL(18,2)\n  description NVARCHAR(500)\n  category NVARCHAR(100)\n  merchant NVARCHAR(200)"},
        {"doc_type": "table_schema", "content": "TEMPLATE [stg].[stg_exchange_rates]\nColumns:\n  rate_date DATE NOT NULL\n  from_currency NVARCHAR(3)\n  to_currency NVARCHAR(3)\n  exchange_rate DECIMAL(18,8)"},
        {"doc_type": "business_term", "content": "Business Term: Net Interest Income\nDefinition: Interest earned minus interest paid\nFormula: SUM(CASE WHEN type='interest_earned' THEN amount ELSE 0 END) - SUM(CASE WHEN type='interest_paid' THEN amount ELSE 0 END)\nCategory: Banking"},
        {"doc_type": "business_term", "content": "Business Term: NPL Ratio (Non-Performing Loan)\nDefinition: Loans 90+ days past due as percentage of total loans\nFormula: COUNT(loans WHERE days_past_due >= 90) / COUNT(total_loans)\nCategory: Risk"},
    ],
    "healthcare": [
        {"doc_type": "table_schema", "content": "TEMPLATE [stg].[stg_patients]\nColumns:\n  patient_id INT NOT NULL\n  mrn NVARCHAR(20)\n  first_name NVARCHAR(100)\n  last_name NVARCHAR(100)\n  date_of_birth DATE\n  gender NVARCHAR(10)\n  insurance_provider NVARCHAR(100)\n  insurance_plan NVARCHAR(100)"},
        {"doc_type": "table_schema", "content": "TEMPLATE [stg].[stg_encounters]\nColumns:\n  encounter_id INT NOT NULL\n  patient_id INT NOT NULL FK→stg_patients\n  encounter_dt DATETIME2\n  encounter_type NVARCHAR(50)\n  provider_id INT FK→stg_providers\n  department NVARCHAR(100)\n  diagnosis_code NVARCHAR(20)\n  discharge_dt DATETIME2\n  total_charges DECIMAL(18,2)"},
        {"doc_type": "business_term", "content": "Business Term: Length of Stay (LOS)\nDefinition: Days between admission and discharge\nFormula: DATEDIFF(day, encounter_dt, discharge_dt)\nCategory: Operations"},
        {"doc_type": "business_term", "content": "Business Term: Readmission Rate\nDefinition: Patients re-admitted within 30 days of discharge\nFormula: COUNT(readmissions_30d) / COUNT(total_discharges)\nCategory: Quality"},
    ],
    "saas": [
        {"doc_type": "table_schema", "content": "TEMPLATE [stg].[stg_users]\nColumns:\n  user_id INT NOT NULL\n  email NVARCHAR(200)\n  plan_type NVARCHAR(50)\n  signup_date DATE\n  last_login_dt DATETIME2\n  is_active BIT\n  mrr DECIMAL(18,2)\n  trial_end_date DATE"},
        {"doc_type": "table_schema", "content": "TEMPLATE [stg].[stg_events]\nColumns:\n  event_id BIGINT NOT NULL\n  user_id INT NOT NULL FK→stg_users\n  event_name NVARCHAR(100)\n  event_dt DATETIME2\n  properties NVARCHAR(MAX)\n  session_id NVARCHAR(50)"},
        {"doc_type": "table_schema", "content": "TEMPLATE [stg].[stg_subscriptions]\nColumns:\n  subscription_id INT NOT NULL\n  user_id INT NOT NULL FK→stg_users\n  plan_name NVARCHAR(50)\n  start_date DATE\n  end_date DATE\n  mrr DECIMAL(18,2)\n  status NVARCHAR(20)\n  cancelled_date DATE\n  cancel_reason NVARCHAR(200)"},
        {"doc_type": "business_term", "content": "Business Term: MRR (Monthly Recurring Revenue)\nDefinition: Total monthly subscription revenue from active subscriptions\nFormula: SUM(mrr) WHERE status = 'active'\nCategory: Revenue"},
        {"doc_type": "business_term", "content": "Business Term: Churn Rate\nDefinition: Percentage of customers who cancelled in a period\nFormula: COUNT(cancelled_in_period) / COUNT(active_at_period_start)\nCategory: Retention"},
        {"doc_type": "business_term", "content": "Business Term: DAU/MAU Ratio\nDefinition: Daily active users divided by monthly active users (stickiness)\nFormula: COUNT(DISTINCT users_today) / COUNT(DISTINCT users_last_30d)\nCategory: Engagement"},
    ],
}


class TemplateKBSeeder:
    """Seeds the RAG knowledge base with industry templates and standard conventions."""

    def __init__(self, retriever: Optional[RAGRetriever] = None):
        self._retriever = retriever or RAGRetriever()

    def seed_conventions(self) -> int:
        """Load standard medallion and naming conventions."""
        docs = []
        for i, conv in enumerate(MEDALLION_CONVENTIONS):
            docs.append(RAGDocument(
                doc_id=f"template:convention:{i}",
                doc_type=DocumentType.CONVENTION_RULE,
                content=conv["content"],
                metadata={"origin": "template", "type": conv["type"]},
            ))
        self._retriever.index_documents(docs)
        logger.info("Seeded %d convention templates", len(docs))
        return len(docs)

    def seed_industry(self, industry: str) -> int:
        """Load an industry-specific schema template."""
        templates = INDUSTRY_TEMPLATES.get(industry.lower(), [])
        if not templates:
            logger.warning("No templates found for industry: %s", industry)
            return 0

        docs = []
        type_map = {
            "table_schema": DocumentType.TABLE_SCHEMA,
            "business_term": DocumentType.BUSINESS_TERM,
            "approved_join": DocumentType.APPROVED_JOIN,
            "convention_rule": DocumentType.CONVENTION_RULE,
        }
        for i, t in enumerate(templates):
            dt = type_map.get(t["doc_type"], DocumentType.TABLE_SCHEMA)
            docs.append(RAGDocument(
                doc_id=f"template:{industry}:{i}",
                doc_type=dt,
                content=t["content"],
                metadata={"origin": "template", "industry": industry},
            ))
        self._retriever.index_documents(docs)
        logger.info("Seeded %d templates for industry '%s'", len(docs), industry)
        return len(docs)

    def seed_all(self, industries: list[str] | None = None) -> dict:
        """Seed conventions + selected industry templates."""
        stats = {"conventions": self.seed_conventions(), "industries": {}}
        for ind in (industries or []):
            stats["industries"][ind] = self.seed_industry(ind)
        stats["total"] = self._retriever.document_count
        return stats

    @staticmethod
    def available_industries() -> list[str]:
        return list(INDUSTRY_TEMPLATES.keys())
