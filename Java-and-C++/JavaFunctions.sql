/*****************************
 * Vertica Analytic Database
 *
 * Example SQL for User Defined Scalar Functions
 *
 * Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP 
 */


-- Step 1: Create LIBRARY 
\set libSfile '\''`pwd`'/build/JavaScalarLib.jar\''
\set libTfile '\''`pwd`'/build/JavaTransformLib.jar\''

CREATE LIBRARY JavaScalarFunctions AS :libSfile LANGUAGE 'JAVA';
CREATE LIBRARY JavaTransformFunctions AS :libTfile LANGUAGE 'JAVA';
-- Step 2: Create Functions
CREATE FUNCTION add2ints AS LANGUAGE 'Java' NAME 'com.vertica.JavaLibs.Add2intsInfo' LIBRARY JavaScalarFunctions;
CREATE FUNCTION addanyints AS LANGUAGE 'Java' NAME 'com.vertica.JavaLibs.AddAnyIntsInfo' LIBRARY JavaScalarFunctions;

CREATE TRANSFORM FUNCTION topk AS LANGUAGE 'Java' NAME 'com.vertica.JavaLibs.TopKFactory' LIBRARY JavaTransformFunctions;
CREATE TRANSFORM FUNCTION polytopk  AS LANGUAGE 'Java' NAME 'com.vertica.JavaLibs.PolyTopKFactory' LIBRARY JavaTransformFunctions;
CREATE TRANSFORM FUNCTION topkwithparams AS LANGUAGE 'Java' NAME 'com.vertica.JavaLibs.TopKPerPartitionWithParamsFactory' LIBRARY JavaTransformFunctions;

CREATE TRANSFORM FUNCTION InvertedIndex	AS LANGUAGE 'JAVA' NAME 'com.vertica.JavaLibs.InvertedIndexFactory' LIBRARY JavaTransformFunctions;

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

CREATE TABLE foo( x VARCHAR(5), y VARCHAR(5));
COPY foo FROM STDIN DELIMITER ',';
a,b
a,c
a,d
b,a
b,c
b,d
c,a
c,f
c,h
\.

-- Invoke UDF
SELECT c1, c2, add2ints(c1, c2) FROM T;
SELECT c1, c2, addanyints(c1, c2) FROM T;
SELECT c1, c2,c3, addanyints(c1, c2, c3) FROM T;

SELECT topk(2,c1,c2) over(order by c1) FROM T;
SELECT topkwithparams(c1, c2 using parameters k=2) OVER(ORDER BY c1) FROM T;
SELECT topkwithparams(c1, c2 using parameters k=1) OVER(ORDER BY c1,c2) FROM T;

SELECT polytopk(2,c1, c2 ) OVER(ORDER BY c1) FROM T;
SELECT polytopk(2,x,y) OVER(ORDER BY x,y) FROM foo;
SELECT polytopk(2,c2 ) OVER(ORDER BY c2) FROM T;

-- /* Example Inverted Index */
CREATE TABLE docs(doc_id INTEGER PRIMARY KEY, text VARCHAR(200));
INSERT INTO docs VALUES(100, 'Vertica Analytic database');
INSERT INTO docs VALUES(200, 'Analytic database functions with User Defined Functions UD functions for short');
INSERT INTO docs VALUES(300, 'Vertica in database analytic functions combined with UD functions include many UD things');
INSERT INTO docs VALUES(400, 'UDx Framework include UD scalar functions analytic functions UD single and multiphase transforms UD aggregates UD Loads');
COMMIT;

-- Persist inverted index posting lists.
CREATE TABLE docs_ii(
  term encoding RLE,
  doc_id encoding DELTAVAL,
  term_freq,
  corp_freq)
AS(
  SELECT InvertedIndex(doc_id, text) OVER(PARTITION BY doc_id)
  FROM docs)
SEGMENTED BY HASH(term) ALL NODES;

-- Invoke the UDT to search keywords.
SELECT * FROM docs
WHERE doc_id IN (
  SELECT doc_id FROM docs_ii
  WHERE term = 'vertica'
  AND doc_id IS NOT NULL);

-- Conjunction of keywords: "vertica AND functions".
SELECT * FROM docs
WHERE doc_id IN (
  SELECT k1.doc_id
  FROM (
    SELECT doc_id FROM docs_ii
    WHERE term = 'vertica'
    AND doc_id IS NOT NULL) k1 INNER JOIN
  (SELECT doc_id FROM docs_ii
   WHERE term = 'functions'
   AND doc_id IS NOT NULL) k2
  ON k1.doc_id = k2.doc_id);

-- Disjunction of keywords: "ud OR udx".
SELECT * FROM docs
WHERE doc_id IN (
  SELECT doc_id FROM docs_ii
  WHERE term = 'ud'
  AND doc_id IS NOT NULL
  UNION
  SELECT doc_id FROM docs_ii
  WHERE term = 'udx'
  AND doc_id IS NOT NULL);

-- Rank documents by (simplified) "tf*idf" scores.
-- Limit the result to the two documents with highest scores.
SELECT docs.*, doc_scores.score
FROM docs INNER JOIN (
  SELECT ii.doc_id, (ii.term_freq * term_idf.idf) score
  FROM docs_ii ii INNER JOIN (
    SELECT term, (LN((SELECT COUNT(doc_id) FROM docs) / corp_freq)) AS idf
  FROM docs_ii
  WHERE term = 'ud'
  AND doc_id IS NULL
  AND term_freq IS NULL) term_idf
  ON ii.term = term_idf.term
  WHERE ii.term = 'ud'
  AND ii.doc_id IS NOT NULL) doc_scores
ON docs.doc_id = doc_scores.doc_id
ORDER BY doc_scores.score DESC
LIMIT 2;

DROP TABLE docs cascade;
DROP TABLE docs_ii cascade;

--------------

DROP TABLE T;
DROP TABLE foo;
DROP LIBRARY JavaScalarFunctions CASCADE;
DROP LIBRARY JavaTransformFunctions CASCADE;
