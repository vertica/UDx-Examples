-- This file is a part of the currency convert module of the Python SDK

-- Create the User Defined Scalar Function (UDSF)
CREATE OR REPLACE LIBRARY pylib AS '/home/cstarnes/python_udx/working_examples/add2ints/add2ints.py' LANGUAGE 'Python';
CREATE OR REPLACE FUNCTION add2ints AS LANGUAGE 'Python' NAME 'add2ints_factory' LIBRARY pylib fenced;

-- Create the table and load the data
CREATE TABLE bunch_of_numbers (product_id int, numbs_1 int, numbs_2 int);
COPY bunch_of_numbers FROM STDIN;
200|10|10
300|1|4
400|6|6
100|30|144
\.

-- Return all the rows in items_cost table and then use the UDSF
-- to query the table.
SELECT numbs_1, numbs_2, add2ints(numbs_1, numbs_2, product_id) AS add2ints_sum
    FROM bunch_of_numbers;

-- Clean up
DROP TABLE bunch_of_numbers;

