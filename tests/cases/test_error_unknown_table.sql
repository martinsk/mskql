-- SELECT from nonexistent table should report table name
-- setup:
CREATE TABLE t1 (id INT);
-- input:
SELECT * FROM nonexistent;
-- expected output:
ERROR:  table 'nonexistent' not found
-- expected status: 1
