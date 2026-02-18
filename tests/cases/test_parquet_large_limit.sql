-- parquet foreign table ORDER BY with LIMIT on large dataset
-- setup:
CREATE FOREIGN TABLE large OPTIONS (FILENAME '@@FIXTURES@@/large.parquet');
-- input:
SELECT id, category FROM large ORDER BY id LIMIT 5;
-- expected output:
1|C
2|A
3|A
4|C
5|B
-- expected status: 0
