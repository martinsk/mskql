-- Bug: GROUP BY ... HAVING AVG(col) > N returns empty result even when groups qualify
-- SELECT g FROM t GROUP BY g HAVING AVG(v) > 10 should return 'a' (AVG=15)
-- mskql returns empty result
-- setup:
CREATE TABLE t_havg (g TEXT, v INT);
INSERT INTO t_havg VALUES ('a',10),('a',20),('b',5),('b',3);
-- input:
SELECT g FROM t_havg GROUP BY g HAVING AVG(v) > 10 ORDER BY g;
-- expected output:
a
-- expected status: 0
