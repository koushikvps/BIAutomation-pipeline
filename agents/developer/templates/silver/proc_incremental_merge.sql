-- Template: Silver Stored Procedure (Synapse Dedicated Pool)
-- For dedicated pool, we use CTAS + rename pattern instead of MERGE
-- Placeholders: {domain}, {entity}, {select_columns}, {from_clause}

CREATE OR ALTER PROCEDURE [silver].[usp_load_{domain}_{entity}]
    @batch_id NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('silver.{domain}_{entity}_staging', 'U') IS NOT NULL
        DROP TABLE [silver].[{domain}_{entity}_staging];

    CREATE TABLE [silver].[{domain}_{entity}_staging]
    WITH (DISTRIBUTION = ROUND_ROBIN, CLUSTERED COLUMNSTORE INDEX)
    AS
    SELECT
        {select_columns},
        GETUTCDATE() AS _ingested_at,
        '{source_system}' AS _source_system,
        @batch_id AS _batch_id
    FROM [bronze].[{primary_source_table}]
    {join_clauses}
    WHERE {where_clause};

    IF OBJECT_ID('silver.{domain}_{entity}', 'U') IS NOT NULL
        RENAME OBJECT::silver.{domain}_{entity} TO {domain}_{entity}_old;

    RENAME OBJECT::silver.{domain}_{entity}_staging TO {domain}_{entity};

    IF OBJECT_ID('silver.{domain}_{entity}_old', 'U') IS NOT NULL
        DROP TABLE [silver].[{domain}_{entity}_old];
END;
