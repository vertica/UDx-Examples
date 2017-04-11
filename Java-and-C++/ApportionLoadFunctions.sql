/*****************************
 * Vertica Analytic Database
 *
 * Example SQL for User Defined Source Functions
 *
 * Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP 
 */


-- Step 1: Create LIBRARY
\set libfile '\''`pwd`'/build/FilePortionSource.so\''
CREATE LIBRARY FilePortionSourceLib as :libfile;

\set libparser '\''`pwd`'/build/DelimFilePortionParser.so\''
CREATE LIBRARY DelimFilePortionParserLib as :libparser;

\set native_libfile '\''`pwd`'/build/NativeIntegerParser.so\'';
CREATE LIBRARY NativeIntegerParserLib AS :native_libfile;

-- Step 2: Create Functions
CREATE SOURCE FilePortionSource AS 
LANGUAGE 'C++' NAME 'FilePortionSourceFactory' LIBRARY FilePortionSourceLib; 

CREATE PARSER DelimFilePortionParser AS 
LANGUAGE 'C++' NAME 'DelimFilePortionParserFactory' LIBRARY DelimFilePortionParserLib; 

CREATE PARSER NativeIntParser AS
LANGUAGE 'C++' NAME 'NativeIntegerParserFactory' LIBRARY NativeIntegerParserLib;



-- Step 3: Use Functions
\set data '''/tmp/apls_delim.dat'''
\! python -c "s = ''.join(\"%s|%s|%s~\"%(str(x).zfill(3), 'a'*4997, 'b'*4997) for x in xrange (20)); print s" > /tmp/apls_delim.dat

create table t (n varchar(3), a varchar(4997), b varchar(4997));

-- sdk example delim parser with sdk file portion source, use initiator node by default unless 'nodes' argument is specified
copy t with source FilePortionSource(file=:data) parser DelimFilePortionParser(delimiter = '|', record_terminator = '~');
truncate table t;

-- built-in delim parser with sdk file portion source
copy t with source FilePortionSource(file=:data) delimiter '|' record terminator '~'; 
truncate table t;

-- customize portions from offset list, a few portions have no rows
copy t with source FilePortionSource(file=:data, offsets='0,1234,5678,91011,121314') parser DelimFilePortionParser(delimiter = '|', record_terminator = '~');
truncate table t;



-- NativeIntegerParser: uses apportioned load both with and without a chunker
-- generate data for NativeIntegerParser
\! /bin/echo -e "import fileinput\nimport sys\nfor line in fileinput.input(): sys.stdout.write(str(bytearray(reversed(bytearray.fromhex(unicode('{0:016x}'.format(int(line))))))))" > /tmp/nativeints.py
\! seq 1 $((1024 * 1024)) | python /tmp/nativeints.py > /tmp/nativeints.dat
create table tt (i int);
copy tt from '/tmp/nativeints.dat' parser NativeIntParser();



-- Step 4: Cleanup
drop table tt;
drop table t;
\! rm /tmp/apls_delim*.dat

--Cleanup Libraries
DROP LIBRARY FilePortionSourceLib CASCADE;
DROP LIBRARY DelimFilePortionParserLib CASCADE;
DROP LIBRARY NativeIntegerParserLib CASCADE;
