-- Template: Silver Table (Synapse Dedicated Pool - CTAS pattern)
-- Placeholders: {domain}, {entity}, {select_columns}, {from_clause}, {join_clauses}, {where_clause}

CREATE TABLE [silver].[{domain}_{entity}]
WITH (
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT
    {select_columns},
    GETUTCDATE() AS _ingested_at,
    '{source_system}' AS _source_system
FROM [bronze].[{primary_source_table}]
{join_clauses}
WHERE {where_clause};
