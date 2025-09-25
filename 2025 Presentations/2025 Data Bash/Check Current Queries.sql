
--MUST BE AN ADMIN ON THE WORKSPACE
-- Grabs from at a server level




SELECT [req].[session_id],
    [req].[start_time],
    --[req].[cpu_time] AS [cpu_time_ms],
	s.login_name,
	s.host_name,
	s.program_name,
	s.nt_domain,
	d.name AS 'database_name',
	req.status,
	req.blocking_session_id,
	req.wait_time,
	req.total_elapsed_time,
	req.total_elapsed_time / 1000.00 AS elapsed_seconds,
	req.total_elapsed_time / 60000.00 AS elapsed_minutes,
	req.reads,
	req.writes,
	req.logical_reads,
    OBJECT_NAME([ST].[objectid], [ST].[dbid]) AS [ObjectName],
    SUBSTRING(
        REPLACE(
            REPLACE(
                SUBSTRING(
                    [ST].[text], ([req].[statement_start_offset] / 2) + 1,
                    ((CASE [req].[statement_end_offset]
                            WHEN -1 THEN DATALENGTH([ST].[text])
                            ELSE [req].[statement_end_offset]
                        END - [req].[statement_start_offset]
                        ) / 2
                    ) + 1
                ), CHAR(10), ' '
            ), CHAR(13), ' '
        ), 1, 512
    ) AS [statement_text]
FROM
    [sys].[dm_exec_requests] AS [req]
	JOIN sys.dm_exec_sessions AS s ON s.session_id = req.session_id
	JOIN sys.databases d on d.database_id = req.database_id
    CROSS APPLY [sys].dm_exec_sql_text([req].[sql_handle]) AS [ST]
WHERE 
	s.program_name <> 'QueryInsights'
ORDER BY
    [req].[cpu_time] DESC;
GO