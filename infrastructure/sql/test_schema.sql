-- Test Automation Schema
-- Run on Config DB (same as source DB)

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'test_runs')
CREATE TABLE dbo.test_runs (
    id INT IDENTITY(1,1) PRIMARY KEY,
    instance_id NVARCHAR(100) NOT NULL,
    story_id NVARCHAR(50) NOT NULL,
    work_item_id INT NULL,
    title NVARCHAR(500),
    app_url NVARCHAR(500),
    status NVARCHAR(50) DEFAULT 'pending',
    total_tests INT DEFAULT 0,
    passed INT DEFAULT 0,
    failed INT DEFAULT 0,
    errors INT DEFAULT 0,
    ado_plan_id INT NULL,
    ado_suite_id INT NULL,
    elapsed_seconds INT DEFAULT 0,
    auto_triggered BIT DEFAULT 0,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    completed_at DATETIME2 NULL
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'test_cases')
CREATE TABLE dbo.test_cases (
    id INT IDENTITY(1,1) PRIMARY KEY,
    run_id INT REFERENCES dbo.test_runs(id),
    test_case_id NVARCHAR(20) NOT NULL,
    ado_test_case_id INT NULL,
    title NVARCHAR(500),
    category NVARCHAR(50),
    priority NVARCHAR(20),
    status NVARCHAR(50) DEFAULT 'pending',
    duration_ms INT DEFAULT 0,
    error_message NVARCHAR(MAX),
    screenshot_url NVARCHAR(500),
    created_at DATETIME2 DEFAULT GETUTCDATE()
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'test_bugs')
CREATE TABLE dbo.test_bugs (
    id INT IDENTITY(1,1) PRIMARY KEY,
    run_id INT REFERENCES dbo.test_runs(id),
    ado_bug_id INT NOT NULL,
    test_case_id NVARCHAR(20),
    title NVARCHAR(500),
    created_at DATETIME2 DEFAULT GETUTCDATE()
);
