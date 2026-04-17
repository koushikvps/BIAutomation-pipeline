-- ============================================================
-- Create schemas for medallion architecture + metadata
-- Run against: Synapse Serverless SQL Pool
-- ============================================================

-- Medallion layers
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA [bronze]');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA [silver]');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA [gold]');
GO

-- Metadata catalog
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'catalog')
    EXEC('CREATE SCHEMA [catalog]');
GO

-- Audit trail
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit')
    EXEC('CREATE SCHEMA [audit]');
GO

PRINT 'All schemas created successfully.';
