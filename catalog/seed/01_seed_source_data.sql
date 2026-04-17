-- ============================================================
-- Seed Sample Source Data (runs against simulated Azure SQL DB)
-- Sample sales data for development and testing
-- ============================================================

-- Create source schema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'sales')
    EXEC('CREATE SCHEMA [sales]');
GO

-- Customers dimension
CREATE TABLE [sales].[Customer] (
    CustomerId      INT PRIMARY KEY,
    CustomerName    NVARCHAR(200) NOT NULL,
    Region          NVARCHAR(100) NOT NULL,
    Country         NVARCHAR(100) NOT NULL,
    Segment         NVARCHAR(50) NOT NULL,       -- 'Enterprise', 'SMB', 'Consumer'
    CreatedDate     DATE NOT NULL,
    ModifiedDate    DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

-- Products dimension
CREATE TABLE [sales].[Product] (
    ProductId       INT PRIMARY KEY,
    ProductName     NVARCHAR(200) NOT NULL,
    Category        NVARCHAR(100) NOT NULL,
    SubCategory     NVARCHAR(100) NOT NULL,
    UnitPrice       DECIMAL(18,2) NOT NULL,
    IsActive        BIT DEFAULT 1,
    CreatedDate     DATE NOT NULL,
    ModifiedDate    DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

-- Order headers (fact)
CREATE TABLE [sales].[OrderHeader] (
    OrderId         INT PRIMARY KEY,
    CustomerId      INT NOT NULL REFERENCES [sales].[Customer](CustomerId),
    OrderDate       DATE NOT NULL,
    ShipDate        DATE NULL,
    Status          NVARCHAR(20) NOT NULL,       -- 'Pending', 'Shipped', 'Delivered', 'Cancelled'
    ShipRegion      NVARCHAR(100) NULL,
    TotalAmount     DECIMAL(18,2) NOT NULL,
    CreatedDate     DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    ModifiedDate    DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

-- Order details (fact)
CREATE TABLE [sales].[OrderDetail] (
    OrderDetailId   INT PRIMARY KEY,
    OrderId         INT NOT NULL REFERENCES [sales].[OrderHeader](OrderId),
    ProductId       INT NOT NULL REFERENCES [sales].[Product](ProductId),
    Quantity        INT NOT NULL,
    UnitPrice       DECIMAL(18,2) NOT NULL,
    Discount        DECIMAL(5,2) DEFAULT 0,
    LineTotal       AS (Quantity * UnitPrice * (1 - Discount)) PERSISTED,
    CreatedDate     DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

-- ============================================================
-- INSERT SAMPLE DATA
-- ============================================================

-- Customers (20 records)
INSERT INTO [sales].[Customer] (CustomerId, CustomerName, Region, Country, Segment, CreatedDate) VALUES
(1, 'Contoso Ltd', 'North America', 'USA', 'Enterprise', '2023-01-15'),
(2, 'Fabrikam Inc', 'North America', 'USA', 'Enterprise', '2023-02-20'),
(3, 'Northwind Traders', 'Europe', 'UK', 'SMB', '2023-03-10'),
(4, 'Adventure Works', 'North America', 'Canada', 'Enterprise', '2023-01-05'),
(5, 'Alpine Ski House', 'Europe', 'Switzerland', 'SMB', '2023-04-18'),
(6, 'Bellows College', 'North America', 'USA', 'Consumer', '2023-05-22'),
(7, 'Best For You Organics', 'Asia Pacific', 'Australia', 'SMB', '2023-06-01'),
(8, 'Consolidated Messenger', 'Europe', 'Germany', 'Enterprise', '2023-02-14'),
(9, 'Datum Corporation', 'North America', 'USA', 'Enterprise', '2023-07-30'),
(10, 'Fourth Coffee', 'Asia Pacific', 'Japan', 'Consumer', '2023-03-25'),
(11, 'Graphic Design Institute', 'Europe', 'France', 'SMB', '2023-08-12'),
(12, 'Humongous Insurance', 'North America', 'USA', 'Enterprise', '2023-01-20'),
(13, 'Lamna Healthcare', 'Asia Pacific', 'India', 'Enterprise', '2023-09-05'),
(14, 'Lucerne Publishing', 'Europe', 'UK', 'SMB', '2023-04-30'),
(15, 'Margie Travel', 'North America', 'Mexico', 'Consumer', '2023-10-15'),
(16, 'Munson Pickles', 'North America', 'USA', 'SMB', '2023-05-08'),
(17, 'Proseware Inc', 'Europe', 'Netherlands', 'Enterprise', '2023-11-20'),
(18, 'School of Fine Art', 'Asia Pacific', 'Singapore', 'Consumer', '2023-06-14'),
(19, 'Southridge Video', 'North America', 'USA', 'SMB', '2023-12-01'),
(20, 'Trey Research', 'Europe', 'Germany', 'Enterprise', '2023-07-22');
GO

-- Products (15 records)
INSERT INTO [sales].[Product] (ProductId, ProductName, Category, SubCategory, UnitPrice, CreatedDate) VALUES
(1, 'Surface Laptop 5', 'Electronics', 'Laptops', 1299.99, '2023-01-01'),
(2, 'Surface Pro 9', 'Electronics', 'Tablets', 999.99, '2023-01-01'),
(3, 'Xbox Series X', 'Electronics', 'Gaming', 499.99, '2023-01-01'),
(4, 'Microsoft 365 Business', 'Software', 'Productivity', 12.50, '2023-01-01'),
(5, 'Azure Reserved Instance', 'Cloud', 'Infrastructure', 150.00, '2023-01-01'),
(6, 'Dynamics 365 Sales', 'Software', 'CRM', 65.00, '2023-01-01'),
(7, 'Power BI Pro', 'Software', 'Analytics', 9.99, '2023-01-01'),
(8, 'Surface Hub 2S', 'Electronics', 'Collaboration', 8999.99, '2023-01-01'),
(9, 'HoloLens 2', 'Electronics', 'Mixed Reality', 3500.00, '2023-01-01'),
(10, 'Azure Synapse', 'Cloud', 'Analytics', 250.00, '2023-01-01'),
(11, 'GitHub Enterprise', 'Software', 'DevOps', 21.00, '2023-01-01'),
(12, 'Visual Studio Enterprise', 'Software', 'DevOps', 250.00, '2023-01-01'),
(13, 'Surface Earbuds', 'Electronics', 'Accessories', 199.99, '2023-01-01'),
(14, 'Azure OpenAI Service', 'Cloud', 'AI', 0.03, '2023-06-01'),
(15, 'Copilot for M365', 'Software', 'AI', 30.00, '2023-11-01');
GO

-- Order Headers (100 records spanning 12 months)
DECLARE @i INT = 1;
WHILE @i <= 100
BEGIN
    INSERT INTO [sales].[OrderHeader] (OrderId, CustomerId, OrderDate, ShipDate, Status, ShipRegion, TotalAmount, CreatedDate, ModifiedDate)
    VALUES (
        @i,
        ((@i - 1) % 20) + 1,
        DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 365, '2026-03-27'),
        DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 360, '2026-03-27'),
        CASE @i % 4 WHEN 0 THEN 'Pending' WHEN 1 THEN 'Shipped' WHEN 2 THEN 'Delivered' ELSE 'Cancelled' END,
        CASE ((@i - 1) % 20) / 5 WHEN 0 THEN 'North America' WHEN 1 THEN 'Europe' WHEN 2 THEN 'Asia Pacific' ELSE 'North America' END,
        0, -- Will update after details
        GETUTCDATE(),
        GETUTCDATE()
    );
    SET @i = @i + 1;
END;
GO

-- Order Details (300 records, ~3 per order)
DECLARE @j INT = 1;
DECLARE @orderId INT;
DECLARE @lineNum INT;
WHILE @j <= 300
BEGIN
    SET @orderId = ((@j - 1) / 3) + 1;
    SET @lineNum = ((@j - 1) % 3) + 1;
    INSERT INTO [sales].[OrderDetail] (OrderDetailId, OrderId, ProductId, Quantity, UnitPrice, Discount)
    VALUES (
        @j,
        @orderId,
        ((@j - 1) % 15) + 1,
        (ABS(CHECKSUM(NEWID())) % 10) + 1,
        (SELECT UnitPrice FROM [sales].[Product] WHERE ProductId = ((@j - 1) % 15) + 1),
        CASE WHEN @j % 5 = 0 THEN 0.10 ELSE 0.00 END
    );
    SET @j = @j + 1;
END;
GO

-- Update OrderHeader totals
UPDATE oh
SET TotalAmount = sub.Total
FROM [sales].[OrderHeader] oh
INNER JOIN (
    SELECT OrderId, SUM(LineTotal) AS Total
    FROM [sales].[OrderDetail]
    GROUP BY OrderId
) sub ON oh.OrderId = sub.OrderId;
GO

PRINT 'Sample source data seeded successfully.';
PRINT 'Tables: sales.Customer (20), sales.Product (15), sales.OrderHeader (100), sales.OrderDetail (300)';
