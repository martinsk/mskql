-- parquet foreign table ORDER BY with LIMIT
-- setup:
CREATE FOREIGN TABLE basic OPTIONS (FILENAME '@@FIXTURES@@/basic.parquet');
-- input:
SELECT * FROM basic ORDER BY score DESC LIMIT 3;
-- expected output:
1|alice|95
4|diana|91
5|eve|88
-- expected status: 0
