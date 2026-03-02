-- Bug: GROUP BY ... HAVING COUNT(*) > 1 returns empty result even when groups exist
-- SELECT dept FROM t GROUP BY dept HAVING COUNT(*) > 1 should return dept values
-- where that dept appears more than once in the table
-- mskql returns empty result
-- setup:
CREATE TABLE t_ghc (id INT, dept INT);
INSERT INTO t_ghc VALUES (1,1),(2,2),(3,1),(4,3);
-- input:
SELECT dept FROM t_ghc GROUP BY dept HAVING COUNT(*) > 1;
-- expected output:
1
-- expected status: 0
