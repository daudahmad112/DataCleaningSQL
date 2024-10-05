CREATE DATABASE BIKE;
USE BIKE;

TRUNCATE TABLE bike0;

LOAD DATA INFILE 'D:/MySQL Learning/Data Analyst SQL/bike dashboard/bike_share_yr_0.csv' 
INTO TABLE bike0
FIELDS TERMINATED BY ','
IGNORE 1 Lines;

SELECT * FROM bike0;

ALTER TABLE bike0
MODIFY dteday DATE;

TRUNCATE TABLE bike1;

LOAD DATA INFILE 'D:/MySQL Learning/Data Analyst SQL/bike dashboard/bike_share_yr_1.csv' 
INTO TABLE bike1
FIELDS TERMINATED BY ','
IGNORE 1 Lines;

SELECT * FROM bike1;

ALTER TABLE bike1
MODIFY dteday DATE;

SELECT * FROM bike0;
SELECT * FROM bike1;
SELECT * FROM cost;


-- Ahead is a common table expression CTE

WITH cte AS (
	SELECT * FROM bike0
	UNION
	SELECT * FROM bike1)
SELECT * FROM cte
JOIN cost c 
ON cte.yr = c.yr;

SELECT * FROM cte;


CREATE TABLE cte(
	SELECT * FROM bike0
	UNION
	SELECT * FROM bike1);

SELECT * FROM cte;

SELECT 
    *
FROM
    cte
        JOIN
    cost c ON cte.yr = c.yr;

SELECT 
    c.dteday,
    c.season,
    c.yr,
    c.weekday,
    c.hr,
    c.rider_type,
    c.riders,
    b.price,
    b.COGS,
    ROUND(c.riders * b.price, 2) AS revenue,
    ROUND((c.riders * b.price)-b.COGS, 2) AS profit
FROM cte c
LEFT JOIN cost b 
ON c.yr = b.yr;


    







