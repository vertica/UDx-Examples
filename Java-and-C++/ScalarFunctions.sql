/*****************************
 * Vertica Analytic Database
 *
 * Example SQL for User Defined Scalar Functions
 *
 * Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP 
 */


-- Step 1: Create LIBRARY 
\set libfile '\''`pwd`'/build/ScalarFunctions.so\'';
CREATE LIBRARY ScalarFunctions AS :libfile;

-- Step 2: Create Functions
CREATE FUNCTION add2ints AS 
LANGUAGE 'C++' NAME 'Add2IntsFactory' LIBRARY ScalarFunctions;

CREATE FUNCTION addanyints AS 
LANGUAGE 'C++' NAME 'AddAnyIntsFactory' LIBRARY ScalarFunctions;

CREATE FUNCTION removespace AS 
LANGUAGE 'C++' NAME 'RemoveSpaceFactory' LIBRARY ScalarFunctions;

CREATE FUNCTION dfswriteread AS 
LANGUAGE 'C++' NAME 'DFSWriteReadFactory' LIBRARY ScalarFunctions NOT FENCED;

CREATE FUNCTION textconvert AS
LANGUAGE 'C++' NAME 'TextConvertFactory' LIBRARY ScalarFunctions;

CREATE FUNCTION sessionparamgetset AS
LANGUAGE 'C++' NAME 'SessionParamGetSetFactory' LIBRARY ScalarFunctions;

CREATE FUNCTION removesymbol AS
LANGUAGE 'C++' NAME 'RemoveSymbolFactory' LIBRARY ScalarFunctions;

-- Step 3: Use Functions

/***** Add2Ints *****/
CREATE TABLE T (c1 INTEGER, c2 INTEGER, c3 INTEGER, c4 INTEGER);
COPY T FROM STDIN DELIMITER ',';
1,2,3,4
2,2,5,6
3,2,9,3
1,4,5,3
5,2,8,4
\.
-- Invoke UDF
SELECT c1, c2, add2ints(c1, c2) FROM T;

SELECT set_optimizer_directives('EnableDistributeExprEval=false');
-- Invoke DFS UDF
SELECT c1, c2, dfswriteread(c1, c2 USING PARAMETERS file_path='/exscalarfunctions/dfswriteread') FROM T;

SELECT COUNT(*) FROM vs_dfs_file_distributions as distro WHERE distro.distribution_oid =
(SELECT c.distribution FROM vs_dfs_file as c INNER JOIN vs_dfs_file as p
ON c.parent = p.oid WHERE c.name ='dfswriteread' AND p.name = 'exscalarfunctions');

SELECT c.name, c.size, c.isfile, c.isdirectory FROM vs_dfs_file as c INNER JOIN vs_dfs_file as p
ON c.parent = p.oid WHERE c.name = 'dfswriteread' AND p.name = 'exscalarfunctions';

SELECT DISTINCT b.block_size, b.block_id FROM vs_dfs_file_block AS b WHERE b.dfs_file =
(SELECT c.oid FROM vs_dfs_file AS c INNER JOIN vs_dfs_file as p
ON c.parent = p.oid WHERE c.name = 'dfswriteread' AND p.name = 'exscalarfunctions');

-- Delete the file
SELECT dfs_delete('/exscalarfunctions/dfswriteread',false);

SELECT set_optimizer_directives('EnableDistributeExprEval=true');

/***** AddAnyInts *****/

SELECT c1, c2, addanyints(c1, c2) FROM T;
SELECT c1, c2, c3, addanyints(c1, c2, c3) FROM T;
SELECT c1, c2, c3, c4, addanyints(c1, c2, c3, c4) FROM T;

DROP TABLE T;
/***** RemoveSpace *****/
CREATE TABLE T (word VARCHAR(200));
COPY T FROM STDIN;
SingleWord
This is an input text file
So possibly is equated with possible 
This is the final data line
Well maybe not!
The quick brown fox jumped over the lazy dog
\.
-- Invoke UDF
SELECT word, removespace(word) FROM T;
DROP TABLE T;

/***** TextConvert *****/
select textconvert(E'\210\211' USING PARAMETERS from_encoding='EBCDICUS', to_encoding='UTF-8');
select textconvert('c4850a' USING PARAMETERS from_encoding='UTF-16', to_encoding='UTF-8');
select substring(textconvert('㑣㔸愰' USING PARAMETERS from_encoding='UTF-8', to_encoding='UTF-16'),3,5 using characters);
select substring(textconvert('acb' USING PARAMETERS from_encoding='UTF-8', to_encoding='UTF-16'),3,1 using characters);

/***** Set session parameters ****/
select length(SessionParamGetSet(1,'ABC'));
select length(SessionParamGetSet(1,'ABC'));
select length(SessionParamGetSet(20000,'ABC'));
select length(SessionParamGetSet(20000,'ABC'));
select length(SessionParamGetSet(200000,'ABC'));
select length(SessionParamGetSet(200000,'ABC'));
select length(SessionParamGetSet(2000000,'ABC'));
select length(SessionParamGetSet(2000000,'ABC'));
select length(SessionParamGetSet(10000000,'ABC'));
select length(SessionParamGetSet(10000000,'ABC'));

/***** RemoveSymbol *****/
-- Check what parameters our function takes
select * from user_function_parameters where function_name = 'removesymbol';

-- Check what parameters are mandatory
select * from user_function_parameters where function_name = 'removesymbol' and is_required;

-- Invoke UDF
SELECT 'abcxyzxdefx' orig_string, removesymbol('abcxyzxdefx' USING PARAMETERS symbol='x');
SELECT 'abcxyzxdefx' orig_string, removesymbol('abcxyzxdefx' USING PARAMETERS symbol='x', n=2);
SELECT 'abcxyzxdefx' orig_string, removesymbol('abcxyzxdefx' USING PARAMETERS symbol='x', n=10);

-- Step 4: Cleanup
DROP LIBRARY ScalarFunctions CASCADE;
