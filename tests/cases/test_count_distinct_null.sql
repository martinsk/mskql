-- COUNT(DISTINCT col) should not count NULLs
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 10), (2, NULL), (3, 10), (4, 20), (5, NULL);
-- input:
SELECT COUNT(DISTINCT val) FROM t1;
-- expected output:
2
-- expected status: 0
