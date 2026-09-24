-- ============================================================
-- RUN THIS IN YOUR FABRIC SQL DATABASE
-- Creates the same tables with identical data
-- Uses SQL Server syntax (rowstore, indexes)
-- ============================================================

CREATE TABLE dbo.Customer (
    CustomerID INT PRIMARY KEY,
    CustomerName NVARCHAR(100),
    Region NVARCHAR(50),
    Segment NVARCHAR(50),
    City NVARCHAR(100),
    State NVARCHAR(50),
    JoinDate DATE
);

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


CREATE TABLE dbo.SalesOrder (
    OrderID INT PRIMARY KEY,
    CustomerID INT REFERENCES dbo.Customer(CustomerID),
    OrderDate DATE,
    OrderYear INT,
    ProductCategory NVARCHAR(50),
    ProductName NVARCHAR(100),
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    Amount DECIMAL(12,2),
    Region NVARCHAR(50)
);

-- Index for the point lookup demo
CREATE NONCLUSTERED INDEX IX_SalesOrder_CustomerID ON dbo.SalesOrder(CustomerID);
CREATE NONCLUSTERED INDEX IX_SalesOrder_OrderDate ON dbo.SalesOrder(OrderDate) INCLUDE (Region, Amount, CustomerID);

-- Load batch 1: ~500K rows
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

-- Batch 2
INSERT INTO dbo.SalesOrder (OrderID, CustomerID, OrderDate, OrderYear, ProductCategory, ProductName, Quantity, UnitPrice, Amount, Region)
SELECT
    OrderID + 500000,
    CustomerID,
    DATEADD(DAY, -(ABS(CHECKSUM(NEWID())) % 365), OrderDate),
    YEAR(DATEADD(DAY, -(ABS(CHECKSUM(NEWID())) % 365), OrderDate)),
    ProductCategory,
    ProductName,
    ABS(CHECKSUM(NEWID())) % 20 + 1,
    UnitPrice,
    CAST((ABS(CHECKSUM(NEWID())) % 20 + 1) * UnitPrice AS DECIMAL(12,2)),
    Region
FROM dbo.SalesOrder;

-- Batch 3
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


CREATE TABLE dbo.StagingTable (
    ID INT PRIMARY KEY,
    Value NVARCHAR(100),
    CreatedDate DATETIME2
);


SELECT 'Customer' AS TableName, COUNT(*) AS [RowCount] FROM dbo.Customer
UNION ALL
SELECT 'SalesOrder', COUNT(*) FROM dbo.SalesOrder
UNION ALL
SELECT 'StagingTable', COUNT(*) FROM dbo.StagingTable;
