-- Template: Bronze External Table (Parquet on ADLS Gen2)
-- Placeholders: {schema}, {table_name}, {column_definitions}, {adls_path}

IF NOT EXISTS (SELECT 1 FROM sys.external_file_formats WHERE name = 'ParquetFileFormat')
    CREATE EXTERNAL FILE FORMAT [ParquetFileFormat]
    WITH (FORMAT_TYPE = PARQUET, DATA_COMPRESSION = 'org.apache.parquet.format.CompressionCodecName.SNAPPY');

IF NOT EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = 'BronzeDataSource')
    CREATE EXTERNAL DATA SOURCE [BronzeDataSource]
    WITH (LOCATION = 'abfss://bronze@${storage_account}.dfs.core.windows.net');

CREATE EXTERNAL TABLE [bronze].[{source_schema}_{table_name}] (
    {column_definitions}
)
WITH (
    LOCATION = '{source_schema}/{table_name}/',
    DATA_SOURCE = [BronzeDataSource],
    FILE_FORMAT = [ParquetFileFormat]
);
