-- This file is a part of the currency convert module of the Python SDK

-- Create the User Defined Scalar Function (UDSF)
CREATE OR REPLACE LIBRARY pylib AS '/home/cstarnes/python_udx/working_examples/currency_conversion/currency_convert.py' LANGUAGE 'Python';
CREATE OR REPLACE FUNCTION currency_convert AS LANGUAGE 'Python' NAME 'currency_convert_factory' LIBRARY pylib fenced;

-- Create the table and load the data
CREATE TABLE items (product varchar(30), currency varchar(3), value money);
COPY items FROM STDIN;
Shoes|EUR|120.03
Soccer Ball|GBP|75.49
Coffee|CAD|17.62
Surfboard|AUD|245.33
Hockey Stick|CAD|99.99
Car|USD|17000.00
Software|INR|700.00
Hamburger|USD|7.50
Fish|GBP|89.28
Cattle|ZAR|4231.89
\.

-- Return all the rows in items_cost table and then use the UDSF
-- to query the table.
SELECT product, currency, value FROM items;
SELECT product, currency_convert(currency, value) AS cost_in_usd
    FROM items;

-- Clean up
DROP TABLE items;

