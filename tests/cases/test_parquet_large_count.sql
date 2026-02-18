-- parquet foreign table COUNT on large dataset (1000 rows)
-- setup:
CREATE FOREIGN TABLE large OPTIONS (FILENAME '@@FIXTURES@@/large.parquet');
-- input:
SELECT COUNT(*) FROM large;
-- expected output:
1000
-- expected status: 0
