-- parquet foreign table COUNT aggregate
-- setup:
CREATE FOREIGN TABLE basic OPTIONS (FILENAME '@@FIXTURES@@/basic.parquet');
-- input:
SELECT COUNT(*) FROM basic;
-- expected output:
5
-- expected status: 0
