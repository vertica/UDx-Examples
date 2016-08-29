/*****************************
 * Vertica Analytic Database
 *
 * Example SQL for User Defined R Functions
 *
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP 
 */

-- Step 1: Create LIBRARY 
\set libfile '\''`pwd`'/RFunctions/RFunctions.R\''
CREATE LIBRARY rlib AS :libfile LANGUAGE 'R';

-- Step 2: Create Function Factories
CREATE FUNCTION Rmul
AS LANGUAGE 'R' NAME 'mulFactory' LIBRARY rlib;

CREATE TRANSFORM FUNCTION R_Kmeans
AS LANGUAGE 'R' NAME 'kmeansCluFactory' LIBRARY rlib;

-- Step 3: Use Functions

/*** Example 1: Multiplication ***/
CREATE TABLE T(a FLOAT, b FLOAT);
COPY T FROM STDIN DELIMITER ',';
1,2
0,3
3,1
4,2
3,5
\.

-- Invoke the UDF
SELECT a, b, Rmul(a, b) FROM T;

-- Cleanup
DROP TABLE T;


/*** Example 2: K-means Clustering ***/
CREATE TABLE point_data(x FLOAT, y FLOAT);
COPY point_data FROM STDIN DELIMITER ',';
1.2,3.1
2.3,2.7
1.8,2.3
3.3,5.4
3.6,4.3
3.0,4.5
2.5,3.5
\.


-- Invoke the UDF to perform 2-means clustering on the input.
SELECT R_Kmeans(x, y) OVER() FROM point_data;

DROP TABLE point_data;


-- Step 4: Cleanup
DROP LIBRARY rLib CASCADE;
