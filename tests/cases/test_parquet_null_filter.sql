-- parquet foreign table WHERE IS NOT NULL filter
-- setup:
CREATE FOREIGN TABLE nullable_data OPTIONS (FILENAME '@@FIXTURES@@/nullable.parquet');
-- input:
SELECT id, value FROM nullable_data WHERE value IS NOT NULL;
-- expected output:
1|10
3|30
5|50
-- expected status: 0
