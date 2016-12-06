/*****************************
 * Vertica Analytic Database
 *
 * Example SQL for User Defined Transform Functions
 *
 * Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP 
 */

-- Step 1: Create LIBRARY 
\set libfile '\''`pwd`'/build/TransformFunctions.so\'';
CREATE LIBRARY TransformFunctions AS :libfile;

-- Step 2: Create Token Factory
create transform function tokenize 
as language 'C++' name 'TokenFactory' library TransformFunctions;

-- Step 3: Use Functions

/*** Example 1: TokenFactory ***/
CREATE TABLE T (url varchar(30), description varchar(2000));
INSERT INTO T VALUES ('www.amazon.com','Online retail merchant and provider of cloud services');
INSERT INTO T VALUES ('www.hp.com','Leading provider of computer hardware and imaging solutions');
INSERT INTO T VALUES ('www.vertica.com','World''s fastest analytic database');
COMMIT;
-- Invoke the UDT
SELECT url, tokenize(description) OVER (partition by url) FROM T;
DROP TABLE T;

/*** Example 2: TokenFactory ***/
CREATE TABLE T (word VARCHAR(200));
COPY T FROM STDIN;
SingleWord
This is an input text file
So possibly is equated with possible 
This is the final data line
Well maybe not!
The quick brown fox jumped over the lazy dog
\.

-- Invoke the UDT, no partitioning
SELECT tokenize(word) OVER () FROM T;
DROP TABLE T;



/*** Example 3: Apache Log Parser ***/
create transform function ApacheParser 
as language 'C++' name 'ApacheParserFactory' library TransformFunctions;

CREATE TABLE raw_logs (data VARCHAR(4000));
COPY raw_logs FROM STDIN;
217.226.190.13 - - [16/Oct/2003:02:59:28 -0400] "GET /scripts/nsiislog.dll" 404 307
65.124.172.131 - - [16/Oct/2003:03:50:51 -0400] "GET /scripts/nsiislog.dll" 404 307
65.194.193.201 - - [16/Oct/2003:09:17:42 -0400] "GET / HTTP/1.0" 200 14
66.92.74.252 - - [16/Oct/2003:09:43:49 -0400] "GET / HTTP/1.1" 200 14
66.92.74.252 - - [16/Oct/2003:09:43:49 -0400] "GET /favicon.ico HTTP/1.1" 404 298
65.221.182.2 - - [01/Nov/2003:22:39:51 -0500] "GET /main.css HTTP/1.1" 200 373
65.221.182.2 - - [01/Nov/2003:22:39:52 -0500] "GET /about.html HTTP/1.1" 200 532
65.221.182.2 - - [01/Nov/2003:22:39:55 -0500] "GET /web.mit.edu HTTP/1.1" 404 298
66.249.67.20 - - [02/May/2011:03:28:35 -0700] "GET /robots.txt HTTP/1.1" 404 335 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
66.249.67.20 - - [02/May/2011:03:28:35 -0700] "GET /classes/commit/fft-factoring.pdf HTTP/1.1" 200 69534 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
123.108.250.82 - - [02/May/2011:19:59:17 -0700] "GET /classes/commit/pldi03-aal.pdf HTTP/1.1" 200 346761 "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)"
\.

-- Invoke the UDT to parse each log line into 
CREATE TABLE parsed_logs AS
SELECT
-- The following expression swizzles apache timestamp column 
-- into a format vertica can parse into a timestamptz
(substr(ts_str, 4, 3) || ' ' || 
 substr(ts_str, 1, 2) || ', '|| 
 substr(ts_str, 8, 4) || ' ' || 
 substr(ts_str, 13) || ' ')::timestamptz as ts,
* -- all other fields
FROM  (SELECT ApacheParser(data) OVER (PARTITION BY substr(data,1,20))  -- partition by prefix
       FROM raw_logs) AS sq;

-- Show the parsed log lines
SELECT * FROM parsed_logs;

DROP TABLE raw_logs;
DROP TABLE parsed_logs;


/*** Example 4: Top-k per Partition ***/
CREATE TRANSFORM FUNCTION TopK
AS LANGUAGE 'C++' NAME 'TopKFactory' library TransformFunctions;

CREATE TABLE T(a INTEGER, b INTEGER);
COPY T FROM STDIN DELIMITER ',';
1,4
2,3
3,2
4,1
\.

-- Invoke the UDT to get top-2 rows.
SELECT TopK(2, a, b) OVER(ORDER BY b)
FROM T;


/*** Example 5: Top-k per Partition using parameters ***/
CREATE TRANSFORM FUNCTION TopKParams
AS LANGUAGE 'C++' NAME 'TopKParamsFactory' library TransformFunctions;

SELECT TopKParams(a, b using parameters k=2) OVER(ORDER BY b)
FROM T;

/*** Example 6: Polymorphic Top-k per Partition using parameters ***/
CREATE TRANSFORM FUNCTION PolyTopK
AS LANGUAGE 'C++' NAME 'PolyTopKPerPartitionFactory' library TransformFunctions;

CREATE TABLE foo( x VARCHAR(5), y VARCHAR(5));
COPY foo FROM STDIN DELIMITER ',';
a,b
a.c
a,d
b,a
b,c
b,d
c,a
c,f
c,h
\.

SELECT PolyTopK(a, b using parameters k=2) OVER(ORDER BY b)
FROM T;

SELECT PolyTopK(a using parameters k=2) OVER(ORDER BY b)
FROM T;

SELECT PolyTopK(x,y using parameters k=2) OVER(partition by x)
FROM foo;

DROP TABLE T;
DROP TABLE foo;

/*** Example 7: Inverted Index ***/
CREATE TRANSFORM FUNCTION InvertedIndex
AS LANGUAGE 'C++' NAME 'InvertedIndexFactory' LIBRARY TransformFunctions FENCED;

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

-- Step 4: Cleanup
DROP LIBRARY TransformFunctions CASCADE;
