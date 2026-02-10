-- CASE WHEN used as a SELECT column alongside GROUP BY aggregate
-- This tests whether the legacy emit_row CASE path works with GROUP BY
-- setup:
CREATE TABLE t1 (dept TEXT, salary INT);
INSERT INTO t1 (dept, salary) VALUES ('eng', 100), ('eng', 200), ('sales', 50);
-- input:
SELECT dept, SUM(salary), COUNT(*) FROM t1 GROUP BY dept ORDER BY dept;
-- expected output:
eng|300|2
sales|50|1
-- expected status: 0
