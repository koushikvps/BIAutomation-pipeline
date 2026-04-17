-- Template: Gold Aggregated View
-- Placeholders: {domain}, {descriptive_name}, {select_columns}, {from_clause}, {join_clauses}, {where_clause}, {group_by_clause}

CREATE OR ALTER VIEW [gold].[vw_{domain}_{descriptive_name}]
AS
/*
    Business Purpose: {business_objective}
    Source Story: {story_id}
    Generated: {timestamp}
    Grain: {grain}
*/
SELECT
    {select_columns}
FROM {from_clause}
{join_clauses}
WHERE {where_clause}
GROUP BY {group_by_clause};
