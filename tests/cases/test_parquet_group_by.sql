-- parquet foreign table GROUP BY on large dataset
-- setup:
CREATE FOREIGN TABLE large OPTIONS (FILENAME '@@FIXTURES@@/large.parquet');
-- input:
SELECT category, COUNT(*) FROM large GROUP BY category ORDER BY category;
-- expected output:
A|338
B|327
C|335
-- expected status: 0
