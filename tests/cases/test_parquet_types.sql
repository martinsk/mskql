-- parquet foreign table with multiple types (int, bigint, float, text, bool)
-- setup:
CREATE FOREIGN TABLE types OPTIONS (FILENAME '@@FIXTURES@@/types.parquet');
-- input:
SELECT * FROM types;
-- expected output:
10|1000000000000|1.5|hello|t
20|2000000000000|2.5|world|f
30|3000000000000|3.5|test|t
-- expected status: 0
