--Slow queries--

USE lh_middle_earth_crops

--  SELECT * FROM queryinsights.exec_requests_history WHERE status <> 'Succeeded'


/*
--about 20 seconds, 5 million rows

SELECT f.*, p.ProductName
FROM fact_sales f
JOIN dim_product p ON 1 = 1;

--*/

/*
--about 40+ minutes, 121+ million rows

SELECT f.*, p.ProductName, c.FirstName
FROM fact_sales f
JOIN dim_product p ON 1 = 1
JOIN dim_customer c ON 1=1

--*/
