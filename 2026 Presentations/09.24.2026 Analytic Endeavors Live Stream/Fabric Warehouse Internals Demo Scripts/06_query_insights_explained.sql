-- ============================================================
-- QUERY INSIGHTS: What Actually Happened to My Query?
-- Run in the WAREHOUSE (queryinsights doesn't exist in SQL DB).
-- queryinsights views are populated after the query completes, so give it a few seconds after a run before checking.
-- ============================================================

USE wh_behind_the_curtain

-- ------------------------------------------------------------
-- 1. Recent demo queries, with the timing split out
-- ------------------------------------------------------------
SELECT TOP 20

    -- WHICH QUERY --

    query_hash,                     -- Fingerprint of the query text. Same hash = same query, so you can line up run 1 vs run 2.
    LEFT(command, 100) AS command,  -- The T-SQL text (truncated here). Quotes appear doubled ('').
    statement_type,                 -- SELECT / INSERT / ASSIGN (a variable SET) etc.
    distributed_statement_id,       -- Unique ID for this one execution. Use it to find the same run in Query Activity or other queryinsights views.

    -- WHERE THE TIME WENT --
    total_elapsed_time_ms,          -- Wall clock from SUBMIT to END. This is what the user feels.
    DATEDIFF(MILLISECOND, submit_time, start_time) AS pre_execution_ms,
                                    -- (computed) Submitted but not yet running: 
                                    --   Compiling the distributed plan, creating auto-statistics on first use, getting compute. 
                                    --  This is the "cold start" tax, and it's most of the gap vs. SQL DB on a first run.
    DATEDIFF(MILLISECOND, start_time, end_time)    AS execution_ms,
                                    -- (computed) Time actually spent running on compute.
    allocated_cpu_time_ms,          -- CPU time summed across ALL compute used. When this is  bigger than execution_ms, the work ran in parallel.

    -- WHERE THE DATA CAME FROM --
    data_scanned_remote_storage_mb, -- Read from OneLake (remote object storage). High = cold read.
    data_scanned_disk_mb,           -- Read from the compute node's local SSD cache.
    data_scanned_memory_mb,         -- Read from memory cache. Fastest.
                                    -- Together these show column pruning at work: 
                                    --   A scan of 30M wide rows reading single-digit MB means only the few compressed columns in the query were touched. 
                                    --   The same query in SQL DB reads the whole rowstore (GBs).
    result_cache_hit,               -- Result set caching:
                                    --   0 = not used (caching off or query not eligible)
                                    --   1 = cache MISS: ran for real, result saved for next time
                                    --   2 = cache HIT: answer came from the cache, the engine did no scan work. Don't compare a 2 against SQL DB.
    CASE result_cache_hit
        WHEN 2 THEN 'Cache hit - no engine work'
        WHEN 1 THEN 'Ran for real, result cached'
        ELSE 'Ran for real'
    END AS cache_story,             -- (computed) Same thing, readable on screen.

    -- HOW IT RAN --------------------------------------------------
    is_distributed,                 -- 1 = the DQP split the work into tasks across compute nodes.
                                    --   0 = handled without distribution (e.g. SET @var).
    row_count,                      -- Rows returned (or affected, for writes).
    status,                         -- Succeeded / Failed / Canceled.
    error_code,                     -- 0 when it worked. Non-zero: look this code up.
    sql_pool_name,                  -- Which workload pool ran it. Here reads show 'SELECT';
                                    
    is_accelerated,                 -- Not well documented sadly
    is_using_external_api,          -- Not well documented sadly

    -- WHO / FROM WHERE --------------------------------------------
    login_name,                     -- Who ran it.
    program_name,                   -- Client tool (SSMS, Fabric portal, Power BI, a pipeline...).
    session_id,                     -- Same session_id = same query window / connection session.
    label,                          -- Set with OPTION (LABEL = '...') to tag demo queries.
                                    --   (Warehouse only; SQL DB would reject the hint.)

    submit_time,
    start_time,
    end_time

FROM queryinsights.exec_requests_history
WHERE statement_type = 'SELECT'
  AND command LIKE '%SalesOrderBig%'       -- only the demo queries
  AND command NOT LIKE '%queryinsights%'   -- not this query itself
ORDER BY submit_time DESC;


-- ------------------------------------------------------------
-- 2. Run 1 vs Run 2, side by side, per query
--    The story on one row: cold run pays the pre-execution tax and reads from OneLake; the next run doesn't.
-- ------------------------------------------------------------
WITH runs AS (
    SELECT
        query_hash,
        LEFT(command, 60) AS command,
        total_elapsed_time_ms,
        DATEDIFF(MILLISECOND, submit_time, start_time) AS pre_execution_ms,
        DATEDIFF(MILLISECOND, start_time, end_time)    AS execution_ms,
        data_scanned_remote_storage_mb,
        data_scanned_disk_mb + data_scanned_memory_mb  AS data_scanned_cache_mb,
        result_cache_hit,
        ROW_NUMBER() OVER (PARTITION BY query_hash ORDER BY submit_time) AS run_number
    FROM queryinsights.exec_requests_history
    WHERE statement_type = 'SELECT'
      AND command LIKE '%SalesOrderBig%'
      AND command NOT LIKE '%queryinsights%'
)
SELECT
    r1.command,
    r1.total_elapsed_time_ms          AS run1_total_ms,
    r1.pre_execution_ms               AS run1_pre_exec_ms,
    r1.execution_ms                   AS run1_exec_ms,
    r1.data_scanned_remote_storage_mb AS run1_onelake_mb,
    r2.total_elapsed_time_ms          AS run2_total_ms,
    r2.pre_execution_ms               AS run2_pre_exec_ms,
    r2.execution_ms                   AS run2_exec_ms,
    r2.data_scanned_remote_storage_mb AS run2_onelake_mb,
    r2.data_scanned_cache_mb          AS run2_cache_mb,
    r2.result_cache_hit               AS run2_result_cache_hit   -- 2 = not a fair comparison
FROM runs r1
LEFT JOIN runs r2
    ON r2.query_hash = r1.query_hash AND r2.run_number = 2
WHERE r1.run_number = 1
ORDER BY r1.command;


-- ------------------------------------------------------------
-- 3. For an engine-vs-engine comparison, turn result set caching OFF so run 2 measures a warm engine, not a cached answer.
--    Turn it back on afterward to show the cache as its own "wow".
-- ------------------------------------------------------------
-- ALTER DATABASE [wh_behind_the_curtain] SET RESULT_SET_CACHING OFF;
-- ALTER DATABASE [wh_behind_the_curtain] SET RESULT_SET_CACHING ON;


