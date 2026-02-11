-- Window AVG where all partition values are NULL should return NULL, not 0
-- setup:
CREATE TABLE t1 (id INT, grp TEXT, val INT);
INSERT INTO t1 (id, grp, val) VALUES (1, 'a', NULL), (2, 'a', NULL);
-- input:
SELECT id, AVG(val) OVER (PARTITION BY grp) FROM t1 ORDER BY id;
-- expected output:
1|
2|
-- expected status: 0
