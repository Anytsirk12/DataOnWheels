


SELECT * FROM sys.dm_exec_requests WHERE status = 'running';

SELECT * FROM queryinsights.exec_requests_history
ORDER BY start_time DESC;

SELECT * FROM queryinsights.exec_requests_history
WHERE distributed_statement_id = '44E4DA27-E36D-4CD7-8689-6DD4AD80565A';


SELECT * FROM queryinsights.sql_pool_insights

