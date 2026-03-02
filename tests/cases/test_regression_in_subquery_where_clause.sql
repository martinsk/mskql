-- Bug: value IN (SELECT ...) in WHERE clause returns wrong result (no rows returned)
-- SELECT id FROM t WHERE dept IN (SELECT dept FROM t GROUP BY dept HAVING COUNT(*) > 1)
-- should return rows where the dept appears more than once
-- mskql returns empty result instead of the matching rows
-- setup:
CREATE TABLE t_isw (id INT, dept INT);
INSERT INTO t_isw VALUES (1,1),(2,2),(3,1),(4,3);
-- input:
SELECT id FROM t_isw WHERE dept IN (SELECT dept FROM t_isw GROUP BY dept HAVING COUNT(*) > 1) ORDER BY id;
-- expected output:
1
3
-- expected status: 0
