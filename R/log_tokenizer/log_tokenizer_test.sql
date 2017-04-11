-- Sales Tax Calculator SQL Test
DROP TABLE IF EXISTS error_logs;
DROP FUNCTION LogTokenizer(varchar);
DROP LIBRARY rLib cascade;
-- Create table and load data
CREATE TABLE error_logs (machine VARCHAR(30), error_log VARCHAR(500));
COPY error_logs FROM STDIN;
node001|ERROR 345 - Broken pipe
node001|WARN - Nearly filled disk
node002|ERROR 111 - Flooded roads
node003|ERROR 222 - Plain old broken
\.
-- Create the library and the transform function
CREATE LIBRARY rLib AS 'log_tokenizer/log_tokenizer.R' LANGUAGE 'R';
CREATE TRANSFORM FUNCTION LogTokenizer AS LANGUAGE 'R' name 'LogTokenizerFactory' LIBRARY rLib;
-- Use the function
SELECT LogTokenizer(error_log USING PARAMETERS spliton=' ') OVER() FROM error_logs;
SELECT machine, LogTokenizer(error_log USING PARAMETERS spliton=' ') OVER(partition by machine) FROM error_logs;
