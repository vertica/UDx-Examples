/*****************************
 * Vertica Analytic Database
 *
 * Example SQL for User Defined Source Load Functions in Java
 *
 * Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP 
 */


-- Step 1: Create LIBRARY
\! mkdir -p /tmp/vertica_udsource_example/
\set libfile '\''`pwd`'/build/JavaUDlLib.jar\''

CREATE LIBRARY JavaLib AS :libfile LANGUAGE 'JAVA';

-- Step 2: Create Functions

create source File as LANGUAGE 'JAVA' name 'com.vertica.JavaLibs.FileSourceFactory' library JavaLib;

create parser BasicIntegerParser as LANGUAGE 'java'  name 'com.vertica.JavaLibs.BasicIntegerParserFactory' library JavaLib;

create parser BasicIntegerParserContinuous as LANGUAGE 'JAVA' NAME 'com.vertica.JavaLibs.BasicIntegerParserFactoryContinuous' library JavaLib;

create filter OneTwoDecoder as LANGUAGE 'JAVA'  name 'com.vertica.JavaLibs.OneTwoDecoderFactory' library JavaLib;


-- Step 3: Use Functions

create table t (i integer);
\! python -c 'for i in xrange(10): print i' > /tmp/vertica_udsource_example/data.txt

copy t source File(file='/tmp/vertica_udsource_example/data.txt');
select * from t order by i;

truncate table t;
\! rm -r /tmp/vertica_udsource_example/

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

COPY t from stdin with filter OneTwoDecoder();
1
1
1
1
1
1
1
1
1
\.

select * from t;
truncate table t;

-- continuous UD parser

\! mkdir -p /tmp/continuousudparser/
--\set datafile '/tmp/continuousudparser/big.dat' 

-- Make a big data file
\! python -c 'for i in xrange(10000000): print i' > '/tmp/continuousudparser/big.dat'

copy t from '/tmp/continuousudparser/big.dat' with parser BasicIntegerParserContinuous();

select count(*), count(distinct i), avg(i) from t;

--cleanup
Drop table t;
Drop library JavaLib cascade;
\! rm -rf /tmp/continuousudparser
