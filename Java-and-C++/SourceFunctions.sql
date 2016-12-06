/*****************************
 * Vertica Analytic Database
 *
 * Example SQL for User Defined Source Functions
 *
 * Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP 
 */


-- Step 1: Create LIBRARY
\set file_libfile '\''`pwd`'/build/filelib.so\'';
CREATE LIBRARY filelib AS :file_libfile;

\set curl_libfile '\''`pwd`'/build/cURLLib.so\'';
CREATE LIBRARY curllib AS :curl_libfile;

\set multicurl_libfile '\''`pwd`'/build/MultiFileCurlSource.so\'';
CREATE LIBRARY multicurllib AS :multicurl_libfile;

-- Step 2: Create Functions
CREATE SOURCE file AS 
LANGUAGE 'C++' NAME 'FileSourceFactory' LIBRARY filelib;

CREATE SOURCE curl AS 
LANGUAGE 'C++' NAME 'CurlSourceFactory' LIBRARY curllib;

CREATE SOURCE multicurl AS 
LANGUAGE 'C++' NAME 'cURLSourceFactory' LIBRARY multicurllib;

-- Step 3: Use Functions
create table t (i integer);


\! mkdir -p /tmp/vertica_udsource_example/
\set port `echo $((10+${iniport:=7990}))`
\! python -c 'for i in xrange(10): print i' > /tmp/vertica_udsource_example/data.txt
\set url '''http://localhost:':port'/data.txt'''
\! echo "http://localhost:$((10+${iniport:=7990}))/data.txt" > /tmp/vertica_udsource_example/index.txt
\! echo "http://localhost:$((10+${iniport:=7990}))/data.txt" >> /tmp/vertica_udsource_example/index.txt
\! cd /tmp/vertica_udsource_example/ ; python -m SimpleHTTPServer `echo $((10+${iniport:=7990}))` >/dev/null 2>&1 & echo $! > /tmp/vertica_udsource_example/SimpleHTTPServer.pid
-- Give SimpleHTTPServer a few seconds to come up
\! sleep 3

copy t source file(file='/tmp/vertica_udsource_example/data.txt');
select * from t order by i;
truncate table t;

copy t source curl(url=:url);
select * from t order by i;
truncate table t;
\set url2 '''http://localhost:':port'/index.txt'''
copy t source multicurl(url=:url2);
select * from t order by i;
truncate table t;

-- Step 4: Cleanup
DROP TABLE t;

DROP LIBRARY filelib CASCADE;
DROP LIBRARY curllib CASCADE;
DROP LIBRARY multicurllib CASCADE;

\! kill `cat /tmp/vertica_udsource_example/SimpleHTTPServer.pid`
\! rm -r /tmp/vertica_udsource_example/



