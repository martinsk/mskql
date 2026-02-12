-- DISTINCT ON with multiple rows per group
-- setup:
CREATE TABLE t1 (dept TEXT, name TEXT, salary INT);
INSERT INTO t1 VALUES ('eng', 'alice', 100), ('eng', 'bob', 120), ('sales', 'carol', 90), ('sales', 'dave', 110);
-- input:
SELECT DISTINCT ON (dept) dept, name, salary FROM t1 ORDER BY dept, salary DESC;
-- expected output:
eng|bob|120
sales|dave|110
-- expected status: 0
