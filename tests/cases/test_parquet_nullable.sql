-- parquet foreign table with NULL values
-- setup:
CREATE FOREIGN TABLE nullable_data OPTIONS (FILENAME '@@FIXTURES@@/nullable.parquet');
-- input:
SELECT * FROM nullable_data;
-- expected output:
1|10|a
2||
3|30|c
4||d
5|50|
-- expected status: 0
