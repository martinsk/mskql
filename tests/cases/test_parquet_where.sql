-- parquet foreign table WHERE filter
-- setup:
CREATE FOREIGN TABLE basic OPTIONS (FILENAME '@@FIXTURES@@/basic.parquet');
-- input:
SELECT id, name FROM basic WHERE score > 90;
-- expected output:
1|alice
4|diana
-- expected status: 0
