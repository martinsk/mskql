-- Bug: GROUP BY ... HAVING MIN(col) < N returns empty result even when groups qualify
-- SELECT g FROM t GROUP BY g HAVING MIN(v) < 3 should return 'b' (MIN=1)
-- mskql returns empty result
-- setup:
CREATE TABLE t_hmin (g TEXT, v INT);
INSERT INTO t_hmin VALUES ('a',5),('a',15),('b',1),('b',2);
-- input:
SELECT g FROM t_hmin GROUP BY g HAVING MIN(v) < 3 ORDER BY g;
-- expected output:
b
-- expected status: 0
