-- ============================================================
-- Metadata Catalog Tables
-- Run against: Azure SQL Database (not Synapse serverless)
-- ============================================================

-- Source systems registry
CREATE TABLE [catalog].[source_systems] (
    system_id           INT IDENTITY(1,1) PRIMARY KEY,
    system_name         NVARCHAR(100) NOT NULL,
    server_name         NVARCHAR(255) NOT NULL,
    database_name       NVARCHAR(128) NOT NULL,
    connection_type     NVARCHAR(50) NOT NULL,
    keyvault_secret_ref NVARCHAR(255) NOT NULL,
    is_active           BIT DEFAULT 1,
    created_at          DATETIME2 DEFAULT GETUTCDATE(),
    updated_at          DATETIME2 DEFAULT GETUTCDATE()
);
GO

-- Source table/column metadata (auto-populated by Planner Agent)
CREATE TABLE [catalog].[source_tables] (
    source_table_id     INT IDENTITY(1,1) PRIMARY KEY,
    system_name         NVARCHAR(100) NOT NULL,
    schema_name         NVARCHAR(128) NOT NULL,
    table_name          NVARCHAR(128) NOT NULL,
    column_name         NVARCHAR(128) NOT NULL,
    data_type           NVARCHAR(128) NOT NULL,
    max_length          INT NULL,
    is_nullable         BIT NOT NULL,
    is_primary_key      BIT DEFAULT 0,
    is_foreign_key      BIT DEFAULT 0,
    fk_references       NVARCHAR(500) NULL,
    last_profiled_at    DATETIME2 NULL,
    created_at          DATETIME2 DEFAULT GETUTCDATE()
);
GO

-- Pre-validated join paths (prevents hallucinated joins)
CREATE TABLE [catalog].[approved_joins] (
    join_id             INT IDENTITY(1,1) PRIMARY KEY,
    left_schema         NVARCHAR(128) NOT NULL,
    left_table          NVARCHAR(128) NOT NULL,
    left_column         NVARCHAR(128) NOT NULL,
    right_schema        NVARCHAR(128) NOT NULL,
    right_table         NVARCHAR(128) NOT NULL,
    right_column        NVARCHAR(128) NOT NULL,
    join_type           NVARCHAR(20) NOT NULL,
    cardinality         NVARCHAR(20) NOT NULL,
    is_validated        BIT DEFAULT 0,
    validated_by        NVARCHAR(100) NULL,
    created_at          DATETIME2 DEFAULT GETUTCDATE()
);
GO

-- Business term to physical column mapping
CREATE TABLE [catalog].[business_glossary] (
    glossary_id         INT IDENTITY(1,1) PRIMARY KEY,
    business_term       NVARCHAR(200) NOT NULL,
    domain              NVARCHAR(100) NOT NULL,
    physical_schema     NVARCHAR(128) NOT NULL,
    physical_table      NVARCHAR(128) NOT NULL,
    physical_column     NVARCHAR(128) NOT NULL,
    description         NVARCHAR(1000) NULL,
    data_type           NVARCHAR(128) NULL,
    is_sensitive        BIT DEFAULT 0,
    created_at          DATETIME2 DEFAULT GETUTCDATE()
);
GO

-- Naming convention rules (enforced by Developer Agent)
CREATE TABLE [catalog].[naming_conventions] (
    convention_id       INT IDENTITY(1,1) PRIMARY KEY,
    layer               NVARCHAR(20) NOT NULL,
    object_type         NVARCHAR(50) NOT NULL,
    pattern             NVARCHAR(500) NOT NULL,
    example             NVARCHAR(500) NOT NULL,
    regex_validation    NVARCHAR(500) NULL
);
GO

-- Deployment history (full audit trail)
CREATE TABLE [catalog].[deployment_log] (
    deployment_id       INT IDENTITY(1,1) PRIMARY KEY,
    story_id            NVARCHAR(50) NOT NULL,
    agent_name          NVARCHAR(50) NOT NULL,
    artifact_type       NVARCHAR(50) NOT NULL,
    artifact_name       NVARCHAR(255) NOT NULL,
    artifact_sql        NVARCHAR(MAX) NULL,
    environment         NVARCHAR(20) NOT NULL,
    status              NVARCHAR(20) NOT NULL,
    commit_sha          NVARCHAR(40) NULL,
    build_plan_json     NVARCHAR(MAX) NULL,
    validation_report   NVARCHAR(MAX) NULL,
    error_message       NVARCHAR(MAX) NULL,
    deployed_at         DATETIME2 DEFAULT GETUTCDATE(),
    deployed_by         NVARCHAR(100) DEFAULT 'agent-system'
);
GO

-- Layer existence tracking (used by Mode Switch engine)
CREATE TABLE [catalog].[layer_inventory] (
    inventory_id        INT IDENTITY(1,1) PRIMARY KEY,
    layer               NVARCHAR(20) NOT NULL,
    object_type         NVARCHAR(50) NOT NULL,
    schema_name         NVARCHAR(128) NOT NULL,
    object_name         NVARCHAR(128) NOT NULL,
    source_story_id     NVARCHAR(50) NULL,
    column_list         NVARCHAR(MAX) NULL,
    last_refreshed_at   DATETIME2 NULL,
    is_active           BIT DEFAULT 1,
    created_at          DATETIME2 DEFAULT GETUTCDATE()
);
GO

PRINT 'All catalog tables created successfully.';
