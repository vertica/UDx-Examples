/*****************************
 * Vertica Analytic Database
 *
 * Example SQL for User Defined Parser Functions
 *
 * Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP 
 */


-- Step 1: Create LIBRARY
\set basicintegerparser_libfile '\''`pwd`'/build/BasicIntegerParser.so\'';
CREATE LIBRARY BasicIntegerParserLib AS :basicintegerparser_libfile;

\set basicintegerparser_continuous_libfile '\''`pwd`'/build/BasicIntegerParser_continuous.so\'';
CREATE LIBRARY BasicIntegerParserLib_Continuous AS :basicintegerparser_continuous_libfile;

\set ExampleDelimitedParser_libfile '\''`pwd`'/build/ExampleDelimitedParser.so\'';
CREATE LIBRARY ExampleDelimitedParserLib AS :ExampleDelimitedParser_libfile;

\set libcsv_libfile '\''`pwd`'/build/Rfc4180CsvParser.so\'';
CREATE LIBRARY Rfc4180CsvParserLib AS :libcsv_libfile;

\set csv_libfile '\''`pwd`'/build/TraditionalCsvParser.so\'';
CREATE LIBRARY TraditionalCsvParserLib AS :csv_libfile;

-- Step 2: Create Functions
CREATE PARSER BasicIntegerParser AS 
LANGUAGE 'C++' NAME 'BasicIntegerParserFactory' LIBRARY BasicIntegerParserLib;

CREATE PARSER BasicIntegerParser_Continuous AS 
LANGUAGE 'C++' NAME 'BasicIntegerParserFactory' LIBRARY BasicIntegerParserLib_Continuous;

CREATE PARSER ExampleDelimitedParser AS 
LANGUAGE 'C++' NAME 'DelimitedParserFrameworkExampleFactory' LIBRARY ExampleDelimitedParserLib;

CREATE PARSER LibCSVParser AS 
LANGUAGE 'C++' NAME 'LibCSVParserFactory' LIBRARY Rfc4180CsvParserLib;

CREATE PARSER CSVParser AS 
LANGUAGE 'C++' NAME 'CsvParserFactory' LIBRARY TraditionalCsvParserLib;

-- Step 3: Use Functions
create table t (i integer);

copy t from stdin with parser BasicIntegerParser();
0
1
2
3
4
5
6
7
8
9
\.
select * from t order by i;
truncate table t;

copy t from stdin with parser BasicIntegerParser_Continuous();
0
1
2
3
4
5
6
7
8
9
\.
select * from t order by i;
truncate table t;

copy t from stdin with parser ExampleDelimitedParser();
0
1
2
3
4
5
6
7
8
9
\.
select * from t order by i;
truncate table t;

copy t from stdin with parser LibCSVParser();
0
1
2
3
4
5
6
7
8
9
\.
select * from t order by i;
truncate table t;

copy t from stdin with parser CsvParser();
0
1
2
3
4
5
6
7
8
9
\.
select * from t order by i;
truncate table t;

-- Can even use as an external table
\! seq 1 100000 > /tmp/vertica_udparser_external_table_example.txt
\set tmpfile '''/tmp/vertica_udparser_external_table_example.txt'''
CREATE EXTERNAL TABLE ext_t(i int) as COPY FROM :tmpfile PARSER ExampleDelimitedParser();
SELECT * FROM ext_t LIMIT 1;

-- Step 4: Cleanup
DROP TABLE t;
DROP TABLE ext_t;
\! rm -f /tmp/vertica_udparser_external_table_example.txt

DROP LIBRARY BasicIntegerParserLib CASCADE;
DROP LIBRARY BasicIntegerParserLib_Continuous CASCADE;
DROP LIBRARY ExampleDelimitedParserLib CASCADE;
DROP LIBRARY Rfc4180CsvParserLib CASCADE;
DROP LIBRARY TraditionalCsvParserLib CASCADE;




