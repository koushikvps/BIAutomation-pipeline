"""SQL and notebook templates for merge, incremental, and engine-specific loads."""

from __future__ import annotations

from textwrap import dedent


def generate_merge_scd1(
    target_table: str,
    source_table: str,
    key_columns: list[str],
    update_columns: list[str],
) -> str:
    join_clause = " AND ".join(
        f"tgt.[{col}] = src.[{col}]" for col in key_columns
    )
    update_set = ",\n        ".join(
        f"tgt.[{col}] = src.[{col}]" for col in update_columns
    )
    all_columns = key_columns + update_columns
    insert_cols = ", ".join(f"[{col}]" for col in all_columns)
    insert_vals = ", ".join(f"src.[{col}]" for col in all_columns)

    return dedent(f"""\
        MERGE {target_table} AS tgt
        USING {source_table} AS src
            ON {join_clause}
        WHEN MATCHED THEN
            UPDATE SET
                {update_set}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals});
    """)


def generate_merge_scd2(
    target_table: str,
    source_table: str,
    key_columns: list[str],
    tracked_columns: list[str],
    effective_date_col: str = "effective_from",
    end_date_col: str = "effective_to",
) -> str:
    join_clause = " AND ".join(
        f"tgt.[{col}] = src.[{col}]" for col in key_columns
    )
    change_detect = " OR ".join(
        f"tgt.[{col}] <> src.[{col}]" for col in tracked_columns
    )
    all_columns = key_columns + tracked_columns
    insert_cols = ", ".join(f"[{col}]" for col in all_columns)
    insert_cols += f", [{effective_date_col}], [{end_date_col}], [is_current]"
    insert_vals = ", ".join(f"src.[{col}]" for col in all_columns)
    insert_vals += ", GETUTCDATE(), NULL, 1"

    return dedent(f"""\
        -- Step 1: Expire changed rows
        UPDATE tgt
        SET tgt.[{end_date_col}] = GETUTCDATE(),
            tgt.[is_current] = 0
        FROM {target_table} AS tgt
        INNER JOIN {source_table} AS src
            ON {join_clause}
        WHERE tgt.[is_current] = 1
          AND ({change_detect});

        -- Step 2: Insert new/changed rows
        INSERT INTO {target_table} ({insert_cols})
        SELECT {insert_vals}
        FROM {source_table} AS src
        LEFT JOIN {target_table} AS tgt
            ON {join_clause}
           AND tgt.[is_current] = 1
        WHERE tgt.[{key_columns[0]}] IS NULL
           OR ({change_detect});
    """)


def generate_incremental_load(
    target_table: str,
    source_query: str,
    incremental_column: str,
    watermark_value: str,
) -> str:
    return dedent(f"""\
        INSERT INTO {target_table}
        SELECT *
        FROM ({source_query}) AS src
        WHERE src.[{incremental_column}] > '{watermark_value}';
    """)


def generate_databricks_notebook(
    tables: list[str],
    load_pattern: str,
    source_config: dict,
) -> str:
    jdbc_url = source_config.get("jdbc_url", "")
    db_user = source_config.get("user", "")
    db_password_secret = source_config.get("password_secret", "")

    table_reads = ""
    for table in tables:
        safe_name = table.replace(".", "_")
        table_reads += dedent(f"""\
            df_{safe_name} = (
                spark.read
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", "{table}")
                .option("user", db_user)
                .option("password", db_password)
                .load()
            )
        """)
        if load_pattern == "full_load":
            table_reads += dedent(f"""\
                df_{safe_name}.write.mode("overwrite").format("delta").saveAsTable("{safe_name}")
            """)
        else:
            table_reads += dedent(f"""\
                df_{safe_name}.write.mode("append").format("delta").saveAsTable("{safe_name}")
            """)
        table_reads += "\n"

    return dedent(f"""\
        # Databricks notebook - auto-generated
        # Load pattern: {load_pattern}

        jdbc_url = "{jdbc_url}"
        db_user = "{db_user}"
        db_password = dbutils.secrets.get(scope="keyvault", key="{db_password_secret}")

    """) + table_reads


def generate_spark_sql(
    tables: list[str],
    load_pattern: str,
) -> str:
    statements: list[str] = []
    for table in tables:
        safe_name = table.replace(".", "_")
        if load_pattern == "full_load":
            statements.append(dedent(f"""\
                -- Full load: {table}
                IF OBJECT_ID('{safe_name}', 'U') IS NOT NULL
                    TRUNCATE TABLE {safe_name};

                INSERT INTO {safe_name}
                SELECT * FROM {table};
            """))
        else:
            statements.append(dedent(f"""\
                -- Incremental load: {table}
                INSERT INTO {safe_name}
                SELECT * FROM {table}
                WHERE _load_timestamp > (
                    SELECT COALESCE(MAX(_load_timestamp), '1900-01-01')
                    FROM {safe_name}
                );
            """))

    header = dedent(f"""\
        -- Synapse Spark SQL - auto-generated
        -- Load pattern: {load_pattern}

    """)
    return header + "\n".join(statements)
