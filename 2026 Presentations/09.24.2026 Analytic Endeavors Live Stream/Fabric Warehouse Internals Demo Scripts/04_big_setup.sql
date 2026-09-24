-- ============================================================
-- BIG DATA SETUP for DEMO 4: Where the Warehouse Wins
-- Run this SAME script in BOTH your Fabric Warehouse and Fabric SQL DB (only the CREATE TABLE block differs - pick the right one below)

-- Why this works when the old setup didn't:
--   * Old setup actually produced ~160K rows (40K x 2 x 2), not 4M.
--     That's too small: the Warehouse pays 1-3s of distributed overhead and SQL DB serves 160K rows from memory instantly.
--   * Deterministic data (no NEWID) so both engines hold IDENTICAL rows and return IDENTICAL results. "Same data, same query" is literally true.
--   * WIDE rows (address + notes text). Rowstore reads every byte of every row; the Warehouse only reads the 2-3 columns the query touches.
--   * Rows land in OrderDate order, so the Warehouse can skip whole files / row groups on a date filter. SQL DB has no index on OrderDate.


--TAKES A LONG TIME TO RUN (warehouse 4.5 minutes, sql db 10.5 minutes)
-- ============================================================


DECLARE @TotalRows BIGINT = 30000000;   -- 30M. Scale up if the gap isn't dramatic enough.
DECLARE @BatchSize BIGINT = 5000000;    -- 5M per batch keeps each transaction manageable


-- ------------------------------------------------------------
-- CREATE TABLES: run ONE of these two blocks
-- ------------------------------------------------------------

-- >>> WAREHOUSE version
/*
CREATE TABLE dbo.CustomerBig (
    CustomerID INT,
    CustomerName VARCHAR(100),
    Segment VARCHAR(50),
    State VARCHAR(50),
    JoinYear INT
);
CREATE TABLE dbo.SalesOrderBig (
    OrderID BIGINT,
    CustomerID INT,
    OrderDate DATE,
    OrderYear INT,
    ProductID INT,
    ProductCategory VARCHAR(50),
    Region VARCHAR(50),
    Channel VARCHAR(20),
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    Amount DECIMAL(12,2),
    ShippingAddress VARCHAR(200),
    OrderNotes VARCHAR(400)
);
--*/

-->>> SQL DB version (rowstore, clustered PK - no analytic covering index, no columnstore)
--/*
CREATE TABLE dbo.CustomerBig (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(100),
    Segment VARCHAR(50),
    State VARCHAR(50),
    JoinYear INT
);
CREATE TABLE dbo.SalesOrderBig (
    OrderID BIGINT PRIMARY KEY,
    CustomerID INT,
    OrderDate DATE,
    OrderYear INT,
    ProductID INT,
    ProductCategory VARCHAR(50),
    Region VARCHAR(50),
    Channel VARCHAR(20),
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    Amount DECIMAL(12,2),
    ShippingAddress VARCHAR(200),
    OrderNotes VARCHAR(400)
);
--*/


-- ------------------------------------------------------------
-- CUSTOMERS: 50,000 rows
-- ------------------------------------------------------------
-- Helper: digits 0-9, cross joined to generate row numbers (works in both engines)
CREATE TABLE dbo.Digits (n INT);
INSERT INTO dbo.Digits (n) VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

INSERT INTO dbo.CustomerBig (CustomerID, CustomerName, Segment, State, JoinYear)
SELECT
    n,
    CONCAT('Customer-', n),
    CASE n % 3 WHEN 0 THEN 'Enterprise' WHEN 1 THEN 'SMB' ELSE 'Consumer' END,
    CASE (n * 7) % 8
        WHEN 0 THEN 'NY' WHEN 1 THEN 'GA' WHEN 2 THEN 'IL' WHEN 3 THEN 'CO'
        WHEN 4 THEN 'TX' WHEN 5 THEN 'WA' WHEN 6 THEN 'MA' ELSE 'AZ'
    END,
    2018 + (n % 7)
FROM (
    SELECT TOP (50000) CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS INT) AS n
    FROM dbo.Digits a CROSS JOIN dbo.Digits b CROSS JOIN dbo.Digits c CROSS JOIN dbo.Digits e CROSS JOIN dbo.Digits f
) nums;


-- ------------------------------------------------------------
-- SALES ORDERS: @TotalRows rows in @BatchSize batches
-- OrderDate climbs steadily with OrderID across 2024-01-01 .. 2026-12-30
-- ------------------------------------------------------------
DECLARE @Offset BIGINT = 0;
WHILE @Offset < @TotalRows
BEGIN
    INSERT INTO dbo.SalesOrderBig (OrderID, CustomerID, OrderDate, OrderYear, ProductID, ProductCategory,
                                   Region, Channel, Quantity, UnitPrice, Amount, ShippingAddress, OrderNotes)
    SELECT
        n,
        CustomerID,
        OrderDate,
        YEAR(OrderDate),
        CAST((n * 104729) % 2000 + 1 AS INT),
        CASE n % 6
            WHEN 0 THEN 'Electronics' WHEN 1 THEN 'Clothing' WHEN 2 THEN 'Home & Garden'
            WHEN 3 THEN 'Sports' WHEN 4 THEN 'Books' ELSE 'Food & Beverage'
        END,
        CASE (n / 3) % 5
            WHEN 0 THEN 'Northeast' WHEN 1 THEN 'Southeast' WHEN 2 THEN 'Midwest'
            WHEN 3 THEN 'West' ELSE 'Southwest'
        END,
        CASE (n / 7) % 4 WHEN 0 THEN 'Online' WHEN 1 THEN 'Retail' WHEN 2 THEN 'Partner' ELSE 'Phone' END,
        Quantity,
        UnitPrice,
        CAST(Quantity * UnitPrice AS DECIMAL(12,2)),
        CONCAT(n % 9999 + 1, ' Main Street, Suite ', n % 500 + 1, ', Springfield, ', CustomerID % 90000 + 10000),
        CONCAT('Order ', n, ' for customer ', CustomerID,
               ': standard handling, leave at front desk if no answer, signature not required, ',
               'gift wrap declined, loyalty points applied at checkout.')
    FROM (
        SELECT
            n,
            CAST((n * 7919) % 50000 + 1 AS INT) AS CustomerID,
            DATEADD(DAY, CAST((n - 1) * 1095 / @TotalRows AS INT), CAST('2024-01-01' AS DATE)) AS OrderDate,
            CAST((n * 31) % 20 + 1 AS INT) AS Quantity,
            CAST((n * 17) % 500 + 5 AS DECIMAL(10,2)) AS UnitPrice
        FROM (
            -- 10^8 candidate rows, TOP keeps just this batch
            SELECT TOP (@BatchSize) @Offset + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
            FROM dbo.Digits a CROSS JOIN dbo.Digits b CROSS JOIN dbo.Digits c CROSS JOIN dbo.Digits e
                 CROSS JOIN dbo.Digits f CROSS JOIN dbo.Digits g CROSS JOIN dbo.Digits h CROSS JOIN dbo.Digits i
        ) nums
    ) src;

    SET @Offset = @Offset + @BatchSize;
END;


-- ------------------------------------------------------------
-- VERIFY: run in both engines - every number should match exactly
-- ------------------------------------------------------------
SELECT 'CustomerBig' AS TableName, COUNT_BIG(*) AS [RowCount], NULL AS TotalAmount FROM dbo.CustomerBig
UNION ALL
SELECT 'SalesOrderBig', COUNT_BIG(*), SUM(Amount) FROM dbo.SalesOrderBig;
