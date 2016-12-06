/*****************************
 * Vertica Analytic Database
 *
 * Example SQL for User Defined Analytic Functions
 *
 * Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP 
 */


-- Step 1: Create LIBRARY 
\set libfile '\''`pwd`'/build/AnalyticFunctions.so\'';
CREATE LIBRARY AnalyticFunctions AS :libfile;

-- Step 2: Create Functions
CREATE ANALYTIC FUNCTION an_rank AS LANGUAGE 'C++'
NAME 'RankFactory' LIBRARY AnalyticFunctions;

CREATE ANALYTIC FUNCTION an_lag AS LANGUAGE 'C++'
NAME 'LagFactory' LIBRARY AnalyticFunctions;

CREATE ANALYTIC FUNCTION an_lead AS LANGUAGE 'C++'
NAME 'LeadFactory' LIBRARY AnalyticFunctions;


-- Step 3: Use Functions

-- Example table used in Rank, Lag, and Lead
CREATE TABLE T (x INTEGER, y INTEGER, z INTEGER);
COPY T FROM STDIN DELIMITER ',';
1,10,9
1,10,8
1,11,7
2,7,5
2,7,4
2,9,6
3,8,3
3,8,2
3,9,1
\.


/***** Example 1: Rank *****/

-- Invoke UDF, no partitioning.
SELECT x, y, z, an_rank() over (ORDER BY z) AS an_rank
FROM T;

-- Invoke UDF, with partitioning and ordering.
SELECT x, y, z, an_rank() over (PARTITION BY x, y ORDER BY z) AS an_rank
FROM T;

/***** Example 2: Lag *****/
SELECT z, an_lag(z, 2) over (ORDER BY z) AS an_lag
FROM T;

/***** Example 3: Lead *****/
SELECT z, an_lead(z, 2) over (ORDER BY z) AS an_lead
FROM T;

/***** clean up *****/
DROP TABLE T;
DROP LIBRARY AnalyticFunctions CASCADE;
