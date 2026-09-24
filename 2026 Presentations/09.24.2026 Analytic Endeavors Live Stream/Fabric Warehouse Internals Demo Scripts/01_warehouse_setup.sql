-- ============================================================
-- RUN THIS IN YOUR FABRIC WAREHOUSE
-- Creates demo tables and loads sample data
-- ============================================================

-- Customer dimension
CREATE TABLE dbo.Customer (
    CustomerID INT,
    CustomerName VARCHAR(100),
    Region VARCHAR(50),
    Segment VARCHAR(50),
    City VARCHAR(100),
    State VARCHAR(50),
    JoinDate DATE
);

-- Seed a numbers table using stacked CTEs
-- This generates ~500 customers
INSERT INTO dbo.Customer (CustomerID, CustomerName, Region, Segment, City, State, JoinDate)
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS CustomerID,
    CONCAT('Customer-', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) AS CustomerName,
    CASE ABS(CHECKSUM(NEWID())) % 5
        WHEN 0 THEN 'Northeast'
        WHEN 1 THEN 'Southeast'
        WHEN 2 THEN 'Midwest'
        WHEN 3 THEN 'West'
        WHEN 4 THEN 'Southwest'
    END AS Region,
    CASE ABS(CHECKSUM(NEWID())) % 3
        WHEN 0 THEN 'Enterprise'
        WHEN 1 THEN 'SMB'
        WHEN 2 THEN 'Consumer'
    END AS Segment,
    CASE ABS(CHECKSUM(NEWID())) % 8
        WHEN 0 THEN 'New York'
        WHEN 1 THEN 'Atlanta'
        WHEN 2 THEN 'Chicago'
        WHEN 3 THEN 'Denver'
        WHEN 4 THEN 'Dallas'
        WHEN 5 THEN 'Seattle'
        WHEN 6 THEN 'Boston'
        WHEN 7 THEN 'Phoenix'
    END AS City,
    CASE ABS(CHECKSUM(NEWID())) % 8
        WHEN 0 THEN 'NY'
        WHEN 1 THEN 'GA'
        WHEN 2 THEN 'IL'
        WHEN 3 THEN 'CO'
        WHEN 4 THEN 'TX'
        WHEN 5 THEN 'WA'
        WHEN 6 THEN 'MA'
        WHEN 7 THEN 'AZ'
    END AS State,
    DATEADD(DAY, -(ABS(CHECKSUM(NEWID())) % 1825), '2025-01-01') AS JoinDate
FROM
    (SELECT 1 AS n UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
     UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) a
    CROSS JOIN
    (SELECT 1 AS n UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
     UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) b
    CROSS JOIN
    (SELECT 1 AS n UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) c;


-- SalesOrder fact table - ~2 million rows
CREATE TABLE dbo.SalesOrder (
    OrderID INT,
    CustomerID INT,
    OrderDate DATE,
    OrderYear INT,
    ProductCategory VARCHAR(50),
    ProductName VARCHAR(100),
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    Amount DECIMAL(12,2),
    Region VARCHAR(50)
);

-- Load in batches to avoid timeout
-- Batch 1: ~500K rows
INSERT INTO dbo.SalesOrder (OrderID, CustomerID, OrderDate, OrderYear, ProductCategory, ProductName, Quantity, UnitPrice, Amount, Region)
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS OrderID,
    ABS(CHECKSUM(NEWID())) % 500 + 1 AS CustomerID,
    DATEADD(DAY, -(ABS(CHECKSUM(NEWID())) % 1095), '2026-08-20') AS OrderDate,
    YEAR(DATEADD(DAY, -(ABS(CHECKSUM(NEWID())) % 1095), '2026-08-20')) AS OrderYear,
    CASE ABS(CHECKSUM(NEWID())) % 6
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        WHEN 2 THEN 'Home & Garden'
        WHEN 3 THEN 'Sports'
        WHEN 4 THEN 'Books'
        WHEN 5 THEN 'Food & Beverage'
    END AS ProductCategory,
    CONCAT('Product-', ABS(CHECKSUM(NEWID())) % 200 + 1) AS ProductName,
    ABS(CHECKSUM(NEWID())) % 20 + 1 AS Quantity,
    CAST(ABS(CHECKSUM(NEWID())) % 500 + 5 AS DECIMAL(10,2)) AS UnitPrice,
    CAST((ABS(CHECKSUM(NEWID())) % 20 + 1) * (ABS(CHECKSUM(NEWID())) % 500 + 5) AS DECIMAL(12,2)) AS Amount,
    CASE ABS(CHECKSUM(NEWID())) % 5
        WHEN 0 THEN 'Northeast'
        WHEN 1 THEN 'Southeast'
        WHEN 2 THEN 'Midwest'
        WHEN 3 THEN 'West'
        WHEN 4 THEN 'Southwest'
    END AS Region
FROM
    (SELECT 1 AS n UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
     UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) a
    CROSS JOIN
    (SELECT 1 AS n UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
     UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) b
    CROSS JOIN
    (SELECT 1 AS n UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
     UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) c
    CROSS JOIN
    (SELECT 1 AS n UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
     UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) d
    CROSS JOIN
    (SELECT 1 AS n UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) e;

-- Batch 2: duplicate to get to ~2M rows
INSERT INTO dbo.SalesOrder (OrderID, CustomerID, OrderDate, OrderYear, ProductCategory, ProductName, Quantity, UnitPrice, Amount, Region)
SELECT
    OrderID + 500000,
    CustomerID,
    DATEADD(DAY, -(ABS(CHECKSUM(NEWID())) % 365), OrderDate) AS OrderDate,
    YEAR(DATEADD(DAY, -(ABS(CHECKSUM(NEWID())) % 365), OrderDate)) AS OrderYear,
    ProductCategory,
    ProductName,
    ABS(CHECKSUM(NEWID())) % 20 + 1 AS Quantity,
    UnitPrice,
    CAST((ABS(CHECKSUM(NEWID())) % 20 + 1) * UnitPrice AS DECIMAL(12,2)) AS Amount,
    Region
FROM dbo.SalesOrder;

-- Batch 3: get to ~4M
INSERT INTO dbo.SalesOrder (OrderID, CustomerID, OrderDate, OrderYear, ProductCategory, ProductName, Quantity, UnitPrice, Amount, Region)
SELECT
    OrderID + 1000000,
    CustomerID,
    DATEADD(DAY, -(ABS(CHECKSUM(NEWID())) % 730), OrderDate),
    YEAR(DATEADD(DAY, -(ABS(CHECKSUM(NEWID())) % 730), OrderDate)),
    ProductCategory,
    ProductName,
    ABS(CHECKSUM(NEWID())) % 20 + 1,
    UnitPrice,
    CAST((ABS(CHECKSUM(NEWID())) % 20 + 1) * UnitPrice AS DECIMAL(12,2)),
    Region
FROM dbo.SalesOrder
WHERE OrderID <= 1000000;


-- Staging table for the row-by-row insert demo (start empty)
CREATE TABLE dbo.StagingTable (
    ID INT,
    Value VARCHAR(100),
    CreatedDate DATETIME2(6)
);


-- Verify counts
SELECT 'Customer' AS TableName, COUNT(*) AS [RowCount] FROM dbo.Customer
UNION ALL
SELECT 'SalesOrder', COUNT(*) FROM dbo.SalesOrder
UNION ALL
SELECT 'StagingTable', COUNT(*) FROM dbo.StagingTable;
