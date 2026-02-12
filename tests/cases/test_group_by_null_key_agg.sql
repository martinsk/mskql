-- GROUP BY with NULL key - NULLs should be grouped together
-- setup:
CREATE TABLE t1 (grp INT, val INT);
INSERT INTO t1 VALUES (1, 10), (NULL, 20), (1, 30), (NULL, 40), (2, 50);
-- input:
SELECT grp, SUM(val) FROM t1 GROUP BY grp ORDER BY grp;
-- expected output:
1|40
2|50
|60
-- expected status: 0
