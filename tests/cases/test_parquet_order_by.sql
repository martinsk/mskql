-- parquet foreign table ORDER BY
-- setup:
CREATE FOREIGN TABLE basic OPTIONS (FILENAME '@@FIXTURES@@/basic.parquet');
-- input:
SELECT * FROM basic ORDER BY name;
-- expected output:
1|alice|95
2|bob|87
3|charlie|72
4|diana|91
5|eve|88
-- expected status: 0
