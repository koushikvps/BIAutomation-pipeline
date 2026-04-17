-- ============================================================
-- Drop all deployed objects for greenfield re-test
-- Run against: Synapse Dedicated Pool (bipool)
-- ============================================================

-- Gold layer
DROP VIEW IF EXISTS [gold].[vw_customer_order_summary];
GO
DROP VIEW IF EXISTS [gold].[vw_sales_daily_summary];
GO

-- Silver layer
DROP VIEW IF EXISTS [silver].[vw_sales_customers];
GO
DROP VIEW IF EXISTS [silver].[vw_sales_orders];
GO
DROP VIEW IF EXISTS [silver].[vw_sales_order_items];
GO
DROP VIEW IF EXISTS [silver].[vw_sales_products];
GO
DROP VIEW IF EXISTS [silver].[sales_daily_summary];
GO

-- Bronze layer (external tables)
IF OBJECT_ID('[bronze].[ext_sales_customers]') IS NOT NULL DROP EXTERNAL TABLE [bronze].[ext_sales_customers];
GO
IF OBJECT_ID('[bronze].[ext_sales_orders]') IS NOT NULL DROP EXTERNAL TABLE [bronze].[ext_sales_orders];
GO
IF OBJECT_ID('[bronze].[ext_sales_order_items]') IS NOT NULL DROP EXTERNAL TABLE [bronze].[ext_sales_order_items];
GO
IF OBJECT_ID('[bronze].[ext_sales_products]') IS NOT NULL DROP EXTERNAL TABLE [bronze].[ext_sales_products];
GO
IF OBJECT_ID('[bronze].[sales_OrderHeader]') IS NOT NULL DROP EXTERNAL TABLE [bronze].[sales_OrderHeader];
GO
IF OBJECT_ID('[bronze].[sales_OrderDetail]') IS NOT NULL DROP EXTERNAL TABLE [bronze].[sales_OrderDetail];
GO
IF OBJECT_ID('[bronze].[sales_Customer]') IS NOT NULL DROP EXTERNAL TABLE [bronze].[sales_Customer];
GO
IF OBJECT_ID('[bronze].[sales_Product]') IS NOT NULL DROP EXTERNAL TABLE [bronze].[sales_Product];
GO

PRINT 'All objects dropped. Ready for greenfield deployment.';
GO
