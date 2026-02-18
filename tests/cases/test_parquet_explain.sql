-- parquet foreign table EXPLAIN shows Foreign Scan
-- setup:
CREATE FOREIGN TABLE basic OPTIONS (FILENAME '@@FIXTURES@@/basic.parquet');
-- input:
EXPLAIN SELECT * FROM basic;
-- expected output:
Foreign Scan on basic
-- expected status: 0
