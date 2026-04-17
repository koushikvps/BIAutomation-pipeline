"""Pipeline engine configuration and load pattern definitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class PipelineEngine(str, Enum):
    ADF = "adf"
    DATABRICKS = "databricks"
    SYNAPSE_SPARK = "synapse_spark"


class LoadPattern(str, Enum):
    FULL_LOAD = "full_load"
    INCREMENTAL = "incremental"
    MERGE_SCD1 = "merge_scd1"
    MERGE_SCD2 = "merge_scd2"


@dataclass
class EngineConfig:
    engine: PipelineEngine = PipelineEngine.ADF
    load_pattern: LoadPattern = LoadPattern.FULL_LOAD
    incremental_column: str = ""
    merge_key_columns: list[str] = field(default_factory=list)
    scd2_tracked_columns: list[str] = field(default_factory=list)
    partition_column: str = ""
    databricks_cluster_id: str = ""
    databricks_workspace_url: str = ""
    spark_pool_name: str = ""

    @classmethod
    def from_dict(cls, data: dict) -> EngineConfig:
        engine = data.get("engine", "adf")
        if isinstance(engine, str):
            engine = PipelineEngine(engine)
        load_pattern = data.get("load_pattern", "full_load")
        if isinstance(load_pattern, str):
            load_pattern = LoadPattern(load_pattern)
        return cls(
            engine=engine,
            load_pattern=load_pattern,
            incremental_column=data.get("incremental_column", ""),
            merge_key_columns=data.get("merge_key_columns", []),
            scd2_tracked_columns=data.get("scd2_tracked_columns", []),
            partition_column=data.get("partition_column", ""),
            databricks_cluster_id=data.get("databricks_cluster_id", ""),
            databricks_workspace_url=data.get("databricks_workspace_url", ""),
            spark_pool_name=data.get("spark_pool_name", ""),
        )

    def to_dict(self) -> dict:
        return {
            "engine": self.engine.value,
            "load_pattern": self.load_pattern.value,
            "incremental_column": self.incremental_column,
            "merge_key_columns": self.merge_key_columns,
            "scd2_tracked_columns": self.scd2_tracked_columns,
            "partition_column": self.partition_column,
            "databricks_cluster_id": self.databricks_cluster_id,
            "databricks_workspace_url": self.databricks_workspace_url,
            "spark_pool_name": self.spark_pool_name,
        }
