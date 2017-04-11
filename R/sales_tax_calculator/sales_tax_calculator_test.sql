-- Sales Tax Calculator SQL Test
DROP TABLE IF EXISTS inventory;
DROP FUNCTION SalesTaxCalculator(float, varchar);
DROP LIBRARY rLib;
-- Create table and load data
CREATE TABLE inventory (item VARCHAR(30), state_abbreviation VARCHAR(2), price FLOAT);
COPY inventory FROM STDIN;
Scarf|AZ|6.88
Software|MA|88.31
Soccer Ball|MS|12.55
Beads|LA|0.99
Baseball|TN|42.42
Cheese|WI|20.77
Coffee Mug|MA|8.99
Shoes|TN|23.99
\.
-- Create the library and the transform function
CREATE LIBRARY rLib AS 'sales_tax_calculator/sales_tax_calculator.R' LANGUAGE 'R';
CREATE FUNCTION SalesTaxCalculator AS LANGUAGE 'R' name 'SalesTaxCalculatorFactory' LIBRARY rLib;
-- Use the function
SELECT item, state_abbreviation, price, SalesTaxCalculator(price, state_abbreviation) AS Cost_With_Sales_Tax FROM inventory;
