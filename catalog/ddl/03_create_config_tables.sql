-- ============================================================
-- Config DB Tables (Phase 1)
-- Run against: Azure SQL Database (Config DB)
-- These tables persist pipeline state, artifact versions,
-- and execution history beyond Durable Functions lifetime.
-- ============================================================

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'config')
    EXEC('CREATE SCHEMA config');
GO

-- Pipeline Registry: one row per story-to-pipeline mapping
IF OBJECT_ID('config.pipeline_registry', 'U') IS NULL
CREATE TABLE config.pipeline_registry (
    pipeline_id         INT IDENTITY(1,1) PRIMARY KEY,
    story_id            NVARCHAR(50)  NOT NULL,
    work_item_id        INT           NULL,
    title               NVARCHAR(500) NOT NULL,
    source_system       NVARCHAR(100) NULL,
    source_tables       NVARCHAR(MAX) NULL,       -- JSON array
    target_objects      NVARCHAR(MAX) NULL,       -- JSON array of deployed objects
    mode                NVARCHAR(20)  NULL,       -- greenfield/brownfield/partial
    risk_level          NVARCHAR(10)  NULL,
    status              NVARCHAR(30)  NOT NULL DEFAULT 'registered',
    -- registered, planning, developing, validating, deploying, active, failed, retired
    artifact_count      INT           NULL,
    deploy_count        INT           NULL,
    skip_count          INT           NULL,
    fail_count          INT           NULL,
    last_instance_id    NVARCHAR(100) NULL,       -- Durable Functions instance
    last_run_at         DATETIME2     NULL,
    last_duration_sec   INT           NULL,
    created_at          DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    updated_at          DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT UQ_pipeline_story UNIQUE (story_id)
);
GO

-- Execution Log: one row per step per run
IF OBJECT_ID('config.execution_log', 'U') IS NULL
CREATE TABLE config.execution_log (
    log_id              BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_id         INT           NOT NULL,
    instance_id         NVARCHAR(100) NOT NULL,   -- Durable Functions instance
    step_number         INT           NOT NULL,
    step_name           NVARCHAR(100) NOT NULL,
    status              NVARCHAR(20)  NOT NULL,   -- started, completed, failed, skipped
    detail              NVARCHAR(MAX) NULL,
    error_message       NVARCHAR(MAX) NULL,
    started_at          DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    finished_at         DATETIME2     NULL,
    duration_ms         INT           NULL,
    CONSTRAINT FK_exec_pipeline FOREIGN KEY (pipeline_id)
        REFERENCES config.pipeline_registry(pipeline_id)
);
GO

CREATE NONCLUSTERED INDEX IX_exec_pipeline ON config.execution_log(pipeline_id, instance_id);
GO

-- Artifact Versions: every generated SQL/JSON artifact with version tracking
IF OBJECT_ID('config.artifact_versions', 'U') IS NULL
CREATE TABLE config.artifact_versions (
    artifact_id         BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_id         INT           NOT NULL,
    instance_id         NVARCHAR(100) NOT NULL,
    layer               NVARCHAR(20)  NOT NULL,   -- bronze/silver/gold/adf
    object_name         NVARCHAR(255) NOT NULL,
    artifact_type       NVARCHAR(50)  NOT NULL,   -- external_table/table/view/adf_pipeline
    file_path           NVARCHAR(500) NULL,       -- path in repo
    sql_content         NVARCHAR(MAX) NOT NULL,
    version             INT           NOT NULL DEFAULT 1,
    commit_sha          NVARCHAR(40)  NULL,       -- ADO Git commit
    commit_branch       NVARCHAR(200) NULL,
    deploy_status       NVARCHAR(20)  NULL,       -- deployed/failed/skipped
    created_at          DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT FK_artifact_pipeline FOREIGN KEY (pipeline_id)
        REFERENCES config.pipeline_registry(pipeline_id)
);
GO

CREATE NONCLUSTERED INDEX IX_artifact_pipeline ON config.artifact_versions(pipeline_id);
CREATE NONCLUSTERED INDEX IX_artifact_object ON config.artifact_versions(object_name, layer);
GO

-- Semantic Definitions: business terms for consistent metrics
IF OBJECT_ID('config.semantic_definitions', 'U') IS NULL
CREATE TABLE config.semantic_definitions (
    id                  INT IDENTITY(1,1) PRIMARY KEY,
    term                NVARCHAR(200) NOT NULL UNIQUE,
    definition          NVARCHAR(MAX) NOT NULL,
    views               NVARCHAR(MAX) NULL,       -- comma-separated view names
    owner               NVARCHAR(100) NULL,
    created_at          DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    updated_at          DATETIME2     NOT NULL DEFAULT GETUTCDATE()
);
GO

-- Feedback: user-submitted issues and suggestions
IF OBJECT_ID('config.feedback', 'U') IS NULL
CREATE TABLE config.feedback (
    id                  INT IDENTITY(1,1) PRIMARY KEY,
    feedback_text       NVARCHAR(MAX) NOT NULL,
    category            NVARCHAR(100) NULL DEFAULT 'General',
    affected_object     NVARCHAR(255) NULL,
    status              NVARCHAR(30)  NULL DEFAULT 'open',  -- open, reviewed, resolved
    resolution          NVARCHAR(MAX) NULL,
    created_at          DATETIME2     NOT NULL DEFAULT GETUTCDATE()
);
GO

-- Data Source Connectors: multi-source connector registry
IF OBJECT_ID('config.source_connectors', 'U') IS NULL
CREATE TABLE config.source_connectors (
    id                  INT IDENTITY(1,1) PRIMARY KEY,
    name                NVARCHAR(200) NOT NULL UNIQUE,
    connector_type      NVARCHAR(50)  NOT NULL,   -- rest_api, csv_upload, azure_sql, azure_blob, snowflake, sap
    connection_config   NVARCHAR(MAX) NULL,       -- JSON (no secrets — those go to Key Vault)
    key_vault_secret    NVARCHAR(200) NULL,       -- Key Vault secret name for credentials
    schema_hint         NVARCHAR(MAX) NULL,       -- JSON array of expected columns
    status              NVARCHAR(20)  NOT NULL DEFAULT 'active',
    last_tested_at      DATETIME2     NULL,
    created_at          DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    updated_at          DATETIME2     NOT NULL DEFAULT GETUTCDATE()
);
GO

-- Column-level Lineage: tracks column transformations across layers
IF OBJECT_ID('config.column_lineage', 'U') IS NULL
CREATE TABLE config.column_lineage (
    id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
    story_id            NVARCHAR(50)  NOT NULL,
    source_schema       NVARCHAR(100) NOT NULL,
    source_table        NVARCHAR(200) NOT NULL,
    source_column       NVARCHAR(200) NOT NULL,
    target_schema       NVARCHAR(100) NOT NULL,
    target_table        NVARCHAR(200) NOT NULL,
    target_column       NVARCHAR(200) NOT NULL,
    transformation      NVARCHAR(500) NULL,       -- e.g. 'CAST', 'COALESCE', 'SUM', 'direct'
    layer_from          NVARCHAR(20)  NOT NULL,   -- source/bronze/silver
    layer_to            NVARCHAR(20)  NOT NULL,   -- bronze/silver/gold
    created_at          DATETIME2     NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_lineage_story ON config.column_lineage(story_id);
CREATE NONCLUSTERED INDEX IX_lineage_target ON config.column_lineage(target_table, target_column);
GO

PRINT 'Config DB tables created successfully (v2 — includes semantic, feedback, connectors, lineage).';
GO
