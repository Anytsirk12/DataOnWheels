-- ============================================================
-- DEMO 3: Warehouse vs SQL DB — Right Engine, Right Workload
-- Run each section in BOTH engines, compare elapsed times
-- ============================================================


-- ============================================================
-- DEMO 3A: Temp Table Churn
-- SQL DB: near-instant (rowstore temp tables)
-- Warehouse: slow (each temp table = parquet files + manifests)
-- ============================================================

DECLARE @i INT = 1;
WHILE @i <= 20
BEGIN
    SELECT *
    INTO #stage
    FROM dbo.SalesOrder
    WHERE OrderYear = 2024 + (@i % 3);

    SELECT CustomerID, SUM(Amount) AS Total
    INTO #agg
    FROM #stage
    GROUP BY CustomerID;

    DROP TABLE #stage;
    DROP TABLE #agg;
    SET @i = @i + 1;
END;
-- Check elapsed time in the Messages tab


--SUPER FAST IN WH
--Run in different session so we can easily compare the two in the execution logs
DECLARE @i INT = 1;
WHILE @i <= 20
BEGIN
    WITH stage AS (
    SELECT *
    --INTO #stage
    FROM dbo.SalesOrder
    WHERE OrderYear = 2024 + (@i % 3)
    )
    SELECT CustomerID, SUM(Amount) AS Total
    INTO #agg
    FROM stage 
    GROUP BY CustomerID;

    --DROP TABLE #stage;
    DROP TABLE #agg;
    SET @i = @i + 1;
END;
-- Check elapsed time in the Messages tab


-- ============================================================
-- DEMO 3B: Point Lookups
-- SQL DB: sub-ms per lookup (clustered index seek)
-- Warehouse: seconds per lookup (row group metadata scan)
-- ============================================================

DECLARE @id INT = 1;
DECLARE @result NVARCHAR(100);
WHILE @id <= 50
BEGIN
    SELECT @result = CustomerName
    FROM dbo.Customer
    WHERE CustomerID = @id;

    SET @id = @id + 1;
END;
-- Check elapsed time in the Messages tab


-- ============================================================
-- DEMO 3C: Row-by-Row Inserts
-- SQL DB: fast (standard rowstore inserts)
-- Warehouse: slow (each INSERT = new parquet + manifest)
--
-- IMPORTANT: Truncate StagingTable between runs!
-- ============================================================

-- Clean up from previous run
TRUNCATE TABLE dbo.StagingTable;

DECLARE @j INT = 1;
WHILE @j <= 100
BEGIN
    INSERT INTO dbo.StagingTable (ID, Value, CreatedDate)
    VALUES (@j, CONCAT('Record-', @j), GETDATE());

    SET @j = @j + 1;
END;
-- Check elapsed time in the Messages tab


-- ============================================================
-- DEMO 3D: Large Analytical Aggregate (THE FLIP)
-- Warehouse: fast (columnar + column pruning + predicate pushdown)
-- SQL DB: slower (full rowstore scan, reads every column)
-- ============================================================

SELECT
    Region,
    YEAR(OrderDate) AS OrderYear,
    SUM(Amount) AS TotalSales,
    COUNT(DISTINCT CustomerID) AS UniqueCustomers,
    AVG(Quantity) AS AvgQuantity
FROM dbo.SalesOrder
WHERE OrderDate >= '2024-01-01'
GROUP BY Region, YEAR(OrderDate)
ORDER BY TotalSales DESC;
-- Check elapsed time in the Messages tab


