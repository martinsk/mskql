-- SELECT from empty table should return no rows
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
-- input:
SELECT * FROM t1;
-- expected output:
-- expected status: 0
