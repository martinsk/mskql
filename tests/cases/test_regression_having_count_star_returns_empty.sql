-- Bug: GROUP BY ... HAVING COUNT(*) > N returns empty result even when groups qualify
-- SELECT id FROM t GROUP BY id HAVING COUNT(*) > 1 should return id=1 (appears twice)
-- mskql returns empty result
-- setup:
CREATE TABLE t_hcstar (id INT, v INT);
INSERT INTO t_hcstar VALUES (1,1),(1,2),(2,3);
-- input:
SELECT id FROM t_hcstar GROUP BY id HAVING COUNT(*) > 1;
-- expected output:
1
-- expected status: 0
