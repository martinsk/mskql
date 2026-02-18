-- parquet foreign table COUNT with nullable column
-- setup:
CREATE FOREIGN TABLE nullable_data OPTIONS (FILENAME '@@FIXTURES@@/nullable.parquet');
-- input:
SELECT COUNT(*), COUNT(value) FROM nullable_data;
-- expected output:
5|3
-- expected status: 0
