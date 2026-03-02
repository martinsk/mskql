-- Bug: correlated subquery in WHERE returns wrong rows
-- Query: SELECT id FROM t WHERE sal > (SELECT AVG(sal) FROM t WHERE dept = o.dept)
-- Should return rows where salary exceeds their department average.
-- dept 1: avg(1000,2000)=1500 -> id 2 qualifies (sal=2000)
-- dept 2: avg(3000,4000)=3500 -> id 4 qualifies (sal=4000)
-- mskql returns ids 3 and 4 instead of 2 and 4
-- setup:
CREATE TABLE t_corr (id INT, dept INT, sal INT);
INSERT INTO t_corr VALUES (1, 1, 1000), (2, 1, 2000), (3, 2, 3000), (4, 2, 4000);
-- input:
SELECT id FROM t_corr o WHERE sal > (SELECT AVG(sal) FROM t_corr WHERE dept = o.dept) ORDER BY id;
-- expected output:
2
4
-- expected status: 0
