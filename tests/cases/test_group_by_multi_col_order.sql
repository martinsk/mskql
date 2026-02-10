-- GROUP BY multiple columns with ORDER BY
-- setup:
CREATE TABLE t1 (dept TEXT, role TEXT, salary INT);
INSERT INTO t1 (dept, role, salary) VALUES ('eng', 'dev', 100), ('eng', 'dev', 200), ('eng', 'mgr', 300), ('sales', 'rep', 50);
-- input:
SELECT dept, role, SUM(salary) FROM t1 GROUP BY dept, role ORDER BY dept, role;
-- expected output:
eng|dev|300
eng|mgr|300
sales|rep|50
-- expected status: 0
