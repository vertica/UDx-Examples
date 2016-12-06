****************************
* HP Vertica Analytics Platform
*
* User Defined Extensions Examples
*
* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP 
****************************

This directory contains example code for use with Vertica User Defined Extensions (UDx),
including:
1. UDx in C++: Scalar Functions (UDF), User Defined Transforms (UDT), User Defined Analytics (UDAn),
User Defined Aggregates (UDAgg), and User Defined Load (UDL).

2. UDx in R (Statistical Computing): Scalar Functions and Transform Functions.

3. UDx in Java: Scalar Functions (UDF) and User Defined Transforms (UDT)

Note: HP Vertica R language package should be installed before running the R example functions below.
Please refer to the "Installing R for Vertica" section in the documentation for R package installation instructions.
Note: The functions included herein are code examples, intended for developers.
Vertica supports the Vertica SDK API, but does not support code compiled by other developers against the API.

*******************************
** In Database
*******************************

***
* User Defined Functions in C++
***
Scalar Functions:
  make
  vsql -f ScalarFunctions.sql # includes DDL and examples

Transform Functions:
  make
  vsql -f TransformFunctions.sql # includes DDL and examples

Analytic Functions:
  make
  vsql -f AnalyticFunctions.sql # includes DDL and examples

Aggregate Functions:
  make
  vsql -f AggregateFunctions.sql # includes DDL and examples
  
Source Functions:
  make
  vsql -f SourceFunctions.sql # includes DDL and examples

Filter Functions:
  make
  vsql -f FilterFunctions.sql # includes DDL and examples

Parser Functions:
  make
  vsql -f ParserFunctions.sql # includes DDL and examples


***
* User Defined Functions in R
***
  vsql -f RFunctions.sql # includes DDL and examples

***
* User Defined Functions in Java
***

  make 
  vsql -f JavaFunctions.sql  # includes DDL and examples

***
* User Defined Load Functions in Java
***
  
  make 
  vsql -f JavaUDLFunctions.sql  # includes DDL and examples


*******************************
** Dependencies
*******************************

Some of these examples require that third-party libraries be
installed.  If an example needs a library that is missing, that
library will not be built, and an appropriate error message will be
printed.  Other examples will not be affected.

Most examples do not have any external third-party dependencies; the
following should only be installed if required by a particular example
that you are trying to build.

Most of these dependencies are either installed by default or are
available through the official package repositories of Linux
distributions supported by Vertica.  If installing from a package
repository rather than directly from the websites below, it is
necessary to install the -dev or -devel version of the package.

These dependencies include:
- Boost (<http://www.boost.org/>) -- headers only
- libcurl (<http://curl.haxx.se/libcurl/>) -- library and headers
- libiconv (<http://www.gnu.org/software/libiconv/>) -- library and headers
- zlib (<http://www.zlib.net/>) -- library and headers
- bzip (<http://www.bzip.org/>) (known as "libbz2" on some systems) -- library and headers

Some users may wish to install Boost to a nonstandard location, as
it's a large library.  The examples makefile will search the path
listed in the BOOST_INCLUDE environment variable for the headers.
