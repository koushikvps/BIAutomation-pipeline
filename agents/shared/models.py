"""Data models shared across all agents."""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class ExecutionMode(str, Enum):
    GREENFIELD = "greenfield"
    PARTIAL = "partial"
    BROWNFIELD = "brownfield"
    HYBRID = "hybrid"
    ANOMALY = "anomaly"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class ArtifactType(str, Enum):
    ADF_PIPELINE = "adf_pipeline"
    ADF_COPY = "adf_copy"
    EXTERNAL_TABLE = "external_table"
    TABLE = "table"
    STORED_PROCEDURE = "stored_procedure"
    VIEW = "view"


class Layer(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class ValidationStatus(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    WARN = "warn"


class HealerResult(str, Enum):
    FIXED = "fixed"
    ESCALATED = "escalated"
    RETRY_PENDING = "retry_pending"


# --- Story Contract ---

class StoryContract(BaseModel):
    story_id: str
    title: str
    business_objective: str
    source_system: str
    source_tables: list[str]
    dimensions: list[str] = Field(default_factory=list)
    metrics: list[str] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)
    grain: str = ""
    joins: list[str] = Field(default_factory=list)
    acceptance_criteria: list[str] = Field(default_factory=list)
    target_schema: str = "gold"
    target_view_name: Optional[str] = None
    priority: str = "medium"


# --- Build Plan ---

class SourceTarget(BaseModel):
    system: Optional[str] = None
    schema_name: Optional[str] = None  # 'schema' is reserved in pydantic
    table: Optional[str] = None
    container: Optional[str] = None
    path: Optional[str] = None


class BuildStep(BaseModel):
    step: int
    layer: Layer
    action: str  # 'create', 'alter', 'augment'
    artifact_type: ArtifactType
    object_name: str
    source: Optional[SourceTarget] = None
    target: Optional[SourceTarget] = None
    columns: list[dict] = Field(default_factory=list)
    logic_summary: Optional[str] = None
    load_pattern: Optional[str] = None  # 'full', 'incremental'
    watermark_column: Optional[str] = None
    business_rules: list[str] = Field(default_factory=list)
    depends_on: list[int] = Field(default_factory=list)


class ValidationRequirement(BaseModel):
    check_type: str  # 'row_count', 'null_check', 'duplicate_check', 'reconciliation'
    layer: Optional[Layer] = None
    source_layer: Optional[str] = None
    target_layer: Optional[str] = None
    table: Optional[str] = None
    columns: list[str] = Field(default_factory=list)
    metric: Optional[str] = None
    source_query: Optional[str] = None
    target_query: Optional[str] = None


class EngineSettings(BaseModel):
    """Configurable pipeline engine and load pattern settings."""
    pipeline_engine: str = Field(default="adf", description="adf | databricks | synapse_spark")
    load_pattern: str = Field(default="full_load", description="full_load | incremental | merge_scd1 | merge_scd2")
    incremental_column: str = ""
    merge_key_columns: list[str] = Field(default_factory=list)
    scd2_tracked_columns: list[str] = Field(default_factory=list)
    partition_column: str = ""
    databricks_cluster_id: str = ""
    databricks_workspace_url: str = ""
    spark_pool_name: str = ""


class BuildPlan(BaseModel):
    story_id: str
    mode: ExecutionMode
    risk_level: RiskLevel
    execution_order: list[BuildStep]
    validation_requirements: list[ValidationRequirement] = Field(default_factory=list)
    engine_settings: EngineSettings = Field(default_factory=EngineSettings)


# --- Artifact Bundle ---

class GeneratedArtifact(BaseModel):
    step: int
    artifact_type: ArtifactType
    object_name: str
    layer: Layer
    file_name: str
    content: str  # SQL or ADF JSON


class ArtifactBundle(BaseModel):
    story_id: str
    artifacts: list[GeneratedArtifact]


# --- Validation Report ---

class ValidationCheck(BaseModel):
    check_name: str
    check_type: str
    layer: str
    target_object: str
    expected_value: Optional[str] = None
    actual_value: Optional[str] = None
    status: ValidationStatus
    query_executed: Optional[str] = None
    message: Optional[str] = None


class ValidationReport(BaseModel):
    story_id: str
    phase: str  # 'pre_deploy', 'post_deploy'
    overall_status: ValidationStatus
    checks: list[ValidationCheck]
    blocking_failures: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


# --- Healer Action ---

class HealerAction(BaseModel):
    story_id: str
    failure_type: str
    severity: str
    auto_healable: bool
    action_taken: str
    original_error: Optional[str] = None
    fix_applied: Optional[str] = None
    attempt_number: int
    result: HealerResult


# --- Orchestrator State ---

class PipelineState(BaseModel):
    story_id: str
    mode: Optional[ExecutionMode] = None
    status: str = "pending"  # pending, planning, developing, validating, healing, deploying, completed, failed, escalated
    build_plan: Optional[BuildPlan] = None
    artifacts: Optional[ArtifactBundle] = None
    deploy_result: Optional[dict] = None
    validation_report: Optional[ValidationReport] = None
    healer_actions: list[HealerAction] = Field(default_factory=list)
    retry_count: int = 0
    error_log: list[str] = Field(default_factory=list)
