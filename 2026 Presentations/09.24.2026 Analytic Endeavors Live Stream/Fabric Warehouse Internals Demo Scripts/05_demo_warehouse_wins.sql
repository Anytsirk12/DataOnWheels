-- ============================================================
-- DEMO 4: Where the Warehouse Wins
-- Requires 04_big_setup.sql loaded in BOTH engines.
-- Run each query in BOTH engines, compare elapsed times.
--
-- Fair-fight rules:
--   * Run each query twice per engine and compare the SECOND run
--     (the SQL DB buffer pool is warm, the Warehouse cold start is gone).
--   * If result set caching is on for your Warehouse, turn it off for the
--     demo, or the second run is a cache hit rather than the engine doing the work.
--   * SQL DB gets its PK only: no columnstore, no covering index for these.
--     (Talking point: add a columnstore index to SQL DB and the gap closes,
--      but now you're managing it. The Warehouse does this by default.)
--
-- Warehouse timings: Query Activity or queryinsights.exec_requests_history
-- SQL DB timings:    SET STATISTICS TIME ON, or the Messages tab
-- ============================================================


-- ============================================================
-- DEMO 4A: Column Pruning (slide: Column Pruning)
-- Touches 3 of 13 columns across every row.
-- SQL DB reads every page of the wide rowstore (addresses, notes and all).
-- Warehouse reads only ProductCategory, OrderYear, Amount.
-- ============================================================

-- SET STATISTICS TIME ON;   -- SQL DB only
SELECT
    ProductCategory,
    OrderYear,
    SUM(Amount)  AS TotalSales,
    COUNT_BIG(*) AS Orders
FROM dbo.SalesOrderBig
GROUP BY ProductCategory, OrderYear
ORDER BY ProductCategory, OrderYear;


-- ============================================================
-- DEMO 4B: The Elimination Chain (slide: The Elimination Chain)
-- One quarter out of three years. Data was loaded in OrderDate order,
-- so row group min/max stats let the Warehouse skip most files.
-- SQL DB has no index on OrderDate: full clustered index scan.
-- ============================================================

-- SET STATISTICS TIME ON;   -- SQL DB only
SELECT
    Region,
    Channel,
    SUM(Amount)                AS TotalSales,
    COUNT(DISTINCT CustomerID) AS UniqueCustomers
FROM dbo.SalesOrderBig
WHERE OrderDate >= '2026-10-01' AND OrderDate < '2027-01-01'
GROUP BY Region, Channel
ORDER BY TotalSales DESC;


-- ============================================================
-- DEMO 4C: Big Join + Distinct Count (slide: What Runs FASTER)
-- Full fact scan joined to the customer dimension, high-cardinality
-- DISTINCT. The Warehouse spreads the cells across compute nodes;
-- SQL DB does it all on one box.
-- ============================================================

-- SET STATISTICS TIME ON;   -- SQL DB only
SELECT
    c.Segment,
    c.State,
    so.OrderYear,
    SUM(so.Amount)                            AS TotalSales,
    COUNT(DISTINCT so.CustomerID)             AS UniqueCustomers,
    COUNT(DISTINCT so.ProductID)              AS UniqueProducts,
    AVG(CAST(so.Quantity AS DECIMAL(10,2)))   AS AvgQuantity
FROM dbo.SalesOrderBig so
JOIN dbo.CustomerBig c
    ON c.CustomerID = so.CustomerID
GROUP BY c.Segment, c.State, so.OrderYear
ORDER BY TotalSales DESC;


-- ============================================================
-- WHY IS RUN 1 SLOWER IN THE WAREHOUSE? (cold vs warm)
-- Warehouse, run after doing each query twice:
--   run 1 = data pulled from OneLake (remote storage MB is high)
--   run 2 = served from the local cache (memory / disk MB instead)
-- ============================================================

-- SET STATISTICS TIME ON;   -- SQL DB only
SELECT TOP 10
    start_time,
    total_elapsed_time_ms,
    data_scanned_remote_storage_mb,
    data_scanned_disk_mb,
    data_scanned_memory_mb,
    allocated_cpu_time_ms,
    LEFT(command, 80) AS command
FROM queryinsights.exec_requests_history
WHERE command LIKE '%SalesOrderBig%'
ORDER BY start_time DESC;

-- SQL DB equivalent: turn this on, then rerun a query.
-- "physical reads" near 0 on the FIRST run = SQL DB was already warm from the load, so the first-run comparison isn't cold vs. cold.
-- SET STATISTICS IO ON;


-- ============================================================
-- PROOF: same data, same answer
-- Run in both engines. The numbers match exactly because the data is
-- deterministic (no NEWID).
-- ============================================================

-- SET STATISTICS TIME ON;   -- SQL DB only
SELECT COUNT_BIG(*) AS [RowCount], SUM(Amount) AS TotalAmount, COUNT(DISTINCT CustomerID) AS Customers
FROM dbo.SalesOrderBig;
