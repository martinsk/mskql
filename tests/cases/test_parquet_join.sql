-- parquet foreign table JOIN between two parquet tables
-- setup:
CREATE FOREIGN TABLE jleft OPTIONS (FILENAME '@@FIXTURES@@/join_left.parquet');
CREATE FOREIGN TABLE jright OPTIONS (FILENAME '@@FIXTURES@@/join_right.parquet');
-- input:
SELECT jleft.id, jleft.val, jright.extra FROM jleft JOIN jright ON jleft.id = jright.id ORDER BY jleft.id;
-- expected output:
2|b|x
3|c|y
4|d|z
-- expected status: 0
