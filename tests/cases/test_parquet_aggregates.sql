-- parquet foreign table SUM/AVG/MIN/MAX aggregates
-- setup:
CREATE FOREIGN TABLE basic OPTIONS (FILENAME '@@FIXTURES@@/basic.parquet');
-- input:
SELECT SUM(score), MIN(score), MAX(score) FROM basic;
-- expected output:
433|72|95
-- expected status: 0
