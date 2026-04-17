-- ============================================================
-- Audit & Observability Tables
-- Run against: Azure SQL Database (not Synapse serverless)
-- ============================================================

-- Agent execution log (every agent call is logged)
CREATE TABLE [audit].[agent_execution_log] (
    execution_id        INT IDENTITY(1,1) PRIMARY KEY,
    story_id            NVARCHAR(50) NOT NULL,
    agent_name          NVARCHAR(50) NOT NULL,
    action              NVARCHAR(100) NOT NULL,
    status              NVARCHAR(20) NOT NULL,
    input_summary       NVARCHAR(MAX) NULL,
    output_summary      NVARCHAR(MAX) NULL,
    error_message       NVARCHAR(MAX) NULL,
    duration_seconds    INT NULL,
    llm_tokens_used     INT NULL,
    started_at          DATETIME2 DEFAULT GETUTCDATE(),
    completed_at        DATETIME2 NULL
);
GO

-- Validation results (every check, every run)
CREATE TABLE [audit].[validation_results] (
    result_id           INT IDENTITY(1,1) PRIMARY KEY,
    story_id            NVARCHAR(50) NOT NULL,
    validation_phase    NVARCHAR(20) NOT NULL,
    check_name          NVARCHAR(200) NOT NULL,
    check_type          NVARCHAR(50) NOT NULL,
    layer               NVARCHAR(20) NOT NULL,
    target_object       NVARCHAR(255) NOT NULL,
    expected_value      NVARCHAR(500) NULL,
    actual_value        NVARCHAR(500) NULL,
    status              NVARCHAR(20) NOT NULL,
    query_executed      NVARCHAR(MAX) NULL,
    executed_at         DATETIME2 DEFAULT GETUTCDATE()
);
GO

-- Healer actions log
CREATE TABLE [audit].[healer_actions] (
    action_id           INT IDENTITY(1,1) PRIMARY KEY,
    story_id            NVARCHAR(50) NOT NULL,
    failure_type        NVARCHAR(100) NOT NULL,
    severity            NVARCHAR(20) NOT NULL,
    auto_healable       BIT NOT NULL,
    action_taken        NVARCHAR(500) NOT NULL,
    original_error      NVARCHAR(MAX) NULL,
    fix_applied         NVARCHAR(MAX) NULL,
    attempt_number      INT NOT NULL,
    result              NVARCHAR(20) NOT NULL,
    created_at          DATETIME2 DEFAULT GETUTCDATE()
);
GO

PRINT 'All audit tables created successfully.';
