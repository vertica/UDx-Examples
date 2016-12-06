-- This file is a part of the test url module of the Python SDK

-- Create the User Defined Scalar Function (UDSF)
CREATE OR REPLACE LIBRARY pylib AS 'validate_url.py' LANGUAGE 'Python';
CREATE OR REPLACE FUNCTION validate_url AS LANGUAGE 'Python' NAME 'validate_url_factory' LIBRARY pylib fenced;

-- Create the table and load the data
CREATE TABLE webpages (url varchar(300));
COPY webpages FROM STDIN;
http://my.vertica.com/documentation/vertica/
http://www.google.com/
http://www.mass.gov.com/
http://www.espn.com
http://blah.blah.blah.blah
http://www.hpe.com/
\.

-- Return all the rows in the webpages table and then use the UDSF
-- to query the table.
SELECT * FROM webpages;
SELECT url, test_url(url) AS url_status
    FROM webpages;

-- Clean up
DROP TABLE webpages;
