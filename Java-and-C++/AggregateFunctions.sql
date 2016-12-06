/*****************************
 * Vertica Analytic Database
 *
 * Example SQL for User Defined Aggregate Functions
 *
 * Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP 
 */


-- Step 1: Create LIBRARY 
\set libfile '\''`pwd`'/build/AggregateFunctions.so\'';
CREATE LIBRARY AggregateFunctions AS :libfile;

-- Step 2: Create Functions
CREATE AGGREGATE FUNCTION agg_average AS LANGUAGE 'C++'
NAME 'AverageFactory' LIBRARY AggregateFunctions;

CREATE AGGREGATE FUNCTION agg_longest_string AS LANGUAGE 'C++'
NAME 'LongestStringFactory' LIBRARY AggregateFunctions; 

CREATE AGGREGATE FUNCTION agg_count AS LANGUAGE 'C++'
NAME 'CountOccurencesFactory' LIBRARY AggregateFunctions;


-- Step 3: Use Functions

-- Example table used by Average and Longest string aggregate UD functions.
CREATE TABLE T (x INTEGER, y NUMERIC(5,2), z VARCHAR(10));
COPY T FROM STDIN DELIMITER ',';
1,1.5,'A'
1,3.5,'A'
2,2.0,'B'
2,3.0,'A'
2,2.6,'B'
2,1.4,'A'
3,0.5,'C'
3,3.5,'C'
3,1.5,'B'
3,7.5,'B'
4,5.5,'BC'
4,7.5,'AB'
\.

/***** Example 1: Average *****/

-- Invoke UDF
SELECT x, z, agg_average(y) as average
FROM T
GROUP BY x, z
ORDER BY x, z;


/***** Example 2: LongestString *****/

-- Invoke UDF
SELECT x, agg_longest_string(z)
FROM t
GROUP BY x
ORDER BY x;


/******* Example 3: Count Occurences of a num *******/

SELECT agg_count(x using parameters n=2) as num_occurences
FROM T;

SELECT z, agg_count(x using parameters n=2) as num_occurences
FROM T GROUP BY z;

-- Step 4: Cleanup
DROP TABLE T;
DROP LIBRARY AggregateFunctions CASCADE;
