/*****************************
 * Vertica Analytic Database
 *
 * Example SQL for User Defined Filter Functions
 *
 * Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP 
 */


-- Step 1: Create LIBRARY
\set searchandreplace_libfile '\''`pwd`'/build/SearchAndReplaceFilter.so\'';
CREATE LIBRARY SearchAndReplaceLib AS :searchandreplace_libfile;

\set iconverter_libfile '\''`pwd`'/build/IconverterLib.so\'';
CREATE LIBRARY IconverterLib AS :iconverter_libfile;

\set gzip_libfile '\''`pwd`'/build/GZipLib.so\'';
CREATE LIBRARY GZipLib AS :gzip_libfile;

\set bzip_libfile '\''`pwd`'/build/BZipLib.so\'';
CREATE LIBRARY BZipLib AS :bzip_libfile;

-- Step 2: Create Functions
CREATE FILTER SearchAndReplace AS 
LANGUAGE 'C++' NAME 'SearchAndReplaceFilterFactory' LIBRARY SearchAndReplaceLib;

CREATE FILTER Iconverter AS 
LANGUAGE 'C++' NAME 'IconverterFactory' LIBRARY IconverterLib;

CREATE FILTER GZip AS 
LANGUAGE 'C++' NAME 'GZipUnpackerFactory' LIBRARY GZipLib;

CREATE FILTER BZip AS 
LANGUAGE 'C++' NAME 'BZipUnpackerFactory' LIBRARY BZipLib;

-- Step 3: Use Functions
create table t (i integer);

\! mkdir -p /tmp/vertica_udsource_example/
\! python -c 'for i in xrange(1000000): print i' > /tmp/vertica_udsource_example/data.txt
\! iconv -f utf8 -t utf16 < /tmp/vertica_udsource_example/data.txt > /tmp/vertica_udsource_example/data_utf16.txt
\! gzip < /tmp/vertica_udsource_example/data.txt > /tmp/vertica_udsource_example/data.txt.gz
\! bzip2 < /tmp/vertica_udsource_example/data.txt > /tmp/vertica_udsource_example/data.txt.bz2

\! cp /tmp/vertica_udsource_example/data.txt.gz /tmp/vertica_udsource_example/data.txt.concat.gz
\! echo "-1" | gzip >> /tmp/vertica_udsource_example/data.txt.concat.gz
\! cp /tmp/vertica_udsource_example/data.txt.bz2 /tmp/vertica_udsource_example/data.txt.concat.bz2
\! echo "-1" | bzip2 >> /tmp/vertica_udsource_example/data.txt.concat.bz2


copy t from '/tmp/vertica_udsource_example/data.txt' with filter SearchAndReplace(pattern='0', replace_with='10');
select * from t order by i limit 10;
select count(*) from t;
truncate table t;

copy t from '/tmp/vertica_udsource_example/data_utf16.txt' with filter Iconverter(from_encoding='UTF-16');
select * from t order by i limit 10;
select count(*) from t;
truncate table t;

copy t from '/tmp/vertica_udsource_example/data.txt.gz' with filter GZip();
select * from t order by i limit 10;
select count(*) from t;
truncate table t;

copy t from '/tmp/vertica_udsource_example/data.txt.bz2' with filter BZip();
select * from t order by i limit 10;
select count(*) from t;
truncate table t;

copy t from '/tmp/vertica_udsource_example/data.txt.concat.gz' with filter GZip();
select * from t order by i limit 10;
select count(*) from t;
truncate table t;

copy t from '/tmp/vertica_udsource_example/data.txt.concat.bz2' with filter BZip();
select * from t order by i limit 10;
select count(*) from t;
truncate table t;

-- Step 4: Cleanup
DROP TABLE t;

DROP LIBRARY SearchAndReplaceLib CASCADE;
DROP LIBRARY IconverterLib CASCADE;
DROP LIBRARY GZipLib CASCADE;
DROP LIBRARY BZipLib CASCADE;

\! rm -r /tmp/vertica_udsource_example/



