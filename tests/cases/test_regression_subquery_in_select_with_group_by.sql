-- REGRESSION: Subquery in SELECT list with GROUP BY fails with 'expected column or aggregate function'
-- setup:
CREATE TABLE t (grp TEXT, val INT);
INSERT INTO t VALUES ('A', 10), ('A', 20), ('B', 30);
-- input:
SELECT grp, SUM(val), (SELECT COUNT(*) FROM t) AS total_rows FROM t GROUP BY grp ORDER BY grp;
-- expected output:
A|30|3
B|30|3
-- expected status: 0
