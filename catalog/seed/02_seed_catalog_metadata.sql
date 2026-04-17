-- ============================================================
-- Seed Metadata Catalog
-- Run against: Azure SQL Database (not Synapse serverless)
-- ============================================================

-- Register source system
-- NOTE: Update server_name and database_name to match your deployment
INSERT INTO [catalog].[source_systems] (system_name, server_name, database_name, connection_type, keyvault_secret_ref)
VALUES ('SalesDB', '$(SqlServerFqdn)', '$(SqlDbName)', 'AzureSqlDb', 'source-db-connection-string');
GO

-- sales.Customer
INSERT INTO [catalog].[source_tables] (system_name, schema_name, table_name, column_name, data_type, max_length, is_nullable) VALUES
('SalesDB', 'sales', 'Customer', 'CustomerId', 'int', NULL, 0),
('SalesDB', 'sales', 'Customer', 'CustomerName', 'nvarchar', 200, 0),
('SalesDB', 'sales', 'Customer', 'Region', 'nvarchar', 100, 0),
('SalesDB', 'sales', 'Customer', 'Country', 'nvarchar', 100, 0),
('SalesDB', 'sales', 'Customer', 'Segment', 'nvarchar', 50, 0),
('SalesDB', 'sales', 'Customer', 'CreatedDate', 'date', NULL, 0),
('SalesDB', 'sales', 'Customer', 'ModifiedDate', 'datetime2', NULL, 0);
GO

-- sales.Product
INSERT INTO [catalog].[source_tables] (system_name, schema_name, table_name, column_name, data_type, max_length, is_nullable) VALUES
('SalesDB', 'sales', 'Product', 'ProductId', 'int', NULL, 0),
('SalesDB', 'sales', 'Product', 'ProductName', 'nvarchar', 200, 0),
('SalesDB', 'sales', 'Product', 'Category', 'nvarchar', 100, 0),
('SalesDB', 'sales', 'Product', 'SubCategory', 'nvarchar', 100, 0),
('SalesDB', 'sales', 'Product', 'UnitPrice', 'decimal', NULL, 0),
('SalesDB', 'sales', 'Product', 'IsActive', 'bit', NULL, 0),
('SalesDB', 'sales', 'Product', 'CreatedDate', 'date', NULL, 0),
('SalesDB', 'sales', 'Product', 'ModifiedDate', 'datetime2', NULL, 0);
GO

-- sales.OrderHeader
INSERT INTO [catalog].[source_tables] (system_name, schema_name, table_name, column_name, data_type, max_length, is_nullable, is_foreign_key, fk_references) VALUES
('SalesDB', 'sales', 'OrderHeader', 'OrderId', 'int', NULL, 0, 0, NULL),
('SalesDB', 'sales', 'OrderHeader', 'CustomerId', 'int', NULL, 0, 1, 'sales.Customer.CustomerId'),
('SalesDB', 'sales', 'OrderHeader', 'OrderDate', 'date', NULL, 0, 0, NULL),
('SalesDB', 'sales', 'OrderHeader', 'ShipDate', 'date', NULL, 1, 0, NULL),
('SalesDB', 'sales', 'OrderHeader', 'Status', 'nvarchar', 20, 0, 0, NULL),
('SalesDB', 'sales', 'OrderHeader', 'ShipRegion', 'nvarchar', 100, 1, 0, NULL),
('SalesDB', 'sales', 'OrderHeader', 'TotalAmount', 'decimal', NULL, 0, 0, NULL),
('SalesDB', 'sales', 'OrderHeader', 'CreatedDate', 'datetime2', NULL, 0, 0, NULL),
('SalesDB', 'sales', 'OrderHeader', 'ModifiedDate', 'datetime2', NULL, 0, 0, NULL);
GO

-- sales.OrderDetail
INSERT INTO [catalog].[source_tables] (system_name, schema_name, table_name, column_name, data_type, max_length, is_nullable, is_foreign_key, fk_references) VALUES
('SalesDB', 'sales', 'OrderDetail', 'OrderDetailId', 'int', NULL, 0, 0, NULL),
('SalesDB', 'sales', 'OrderDetail', 'OrderId', 'int', NULL, 0, 1, 'sales.OrderHeader.OrderId'),
('SalesDB', 'sales', 'OrderDetail', 'ProductId', 'int', NULL, 0, 1, 'sales.Product.ProductId'),
('SalesDB', 'sales', 'OrderDetail', 'Quantity', 'int', NULL, 0, 0, NULL),
('SalesDB', 'sales', 'OrderDetail', 'UnitPrice', 'decimal', NULL, 0, 0, NULL),
('SalesDB', 'sales', 'OrderDetail', 'Discount', 'decimal', NULL, 0, 0, NULL),
('SalesDB', 'sales', 'OrderDetail', 'LineTotal', 'decimal', NULL, 0, 0, NULL),
('SalesDB', 'sales', 'OrderDetail', 'CreatedDate', 'datetime2', NULL, 0, 0, NULL);
GO

-- Approved joins
INSERT INTO [catalog].[approved_joins] (left_schema, left_table, left_column, right_schema, right_table, right_column, join_type, cardinality, is_validated, validated_by) VALUES
('sales', 'OrderHeader', 'CustomerId', 'sales', 'Customer', 'CustomerId', 'INNER', '1:N', 1, 'system-seed'),
('sales', 'OrderHeader', 'OrderId', 'sales', 'OrderDetail', 'OrderId', 'INNER', '1:N', 1, 'system-seed'),
('sales', 'OrderDetail', 'ProductId', 'sales', 'Product', 'ProductId', 'INNER', '1:N', 1, 'system-seed');
GO

-- Business glossary
INSERT INTO [catalog].[business_glossary] (business_term, domain, physical_schema, physical_table, physical_column, description, data_type, is_sensitive) VALUES
('Revenue', 'Sales', 'sales', 'OrderDetail', 'LineTotal', 'Calculated line total: Quantity * UnitPrice * (1 - Discount)', 'decimal', 0),
('Order Amount', 'Sales', 'sales', 'OrderHeader', 'TotalAmount', 'Sum of all line totals for the order', 'decimal', 0),
('Customer Region', 'Sales', 'sales', 'Customer', 'Region', 'Geographic region of the customer', 'nvarchar', 0),
('Product Category', 'Sales', 'sales', 'Product', 'Category', 'Top-level product category', 'nvarchar', 0),
('Order Date', 'Sales', 'sales', 'OrderHeader', 'OrderDate', 'Date the order was placed', 'date', 0),
('Customer Name', 'Sales', 'sales', 'Customer', 'CustomerName', 'Full legal name of the customer', 'nvarchar', 1),
('Customer Segment', 'Sales', 'sales', 'Customer', 'Segment', 'Business segment: Enterprise, SMB, Consumer', 'nvarchar', 0);
GO

-- Naming conventions
INSERT INTO [catalog].[naming_conventions] (layer, object_type, pattern, example, regex_validation) VALUES
('bronze', 'external_table', '[bronze].[{source_schema}_{table_name}]', '[bronze].[sales_order_header]', '^bronze\.\w+$'),
('silver', 'table', '[silver].[{domain}_{entity}]', '[silver].[sales_order_combined]', '^silver\.\w+$'),
('silver', 'procedure', '[silver].[usp_load_{domain}_{entity}]', '[silver].[usp_load_sales_order_combined]', '^silver\.usp_load_\w+$'),
('gold', 'view', '[gold].[vw_{domain}_{descriptive_name}]', '[gold].[vw_sales_daily_summary]', '^gold\.vw_\w+$');
GO

PRINT 'Catalog metadata seeded successfully.';
