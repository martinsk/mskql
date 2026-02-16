-- Test COUNT(DISTINCT col) without GROUP BY through plan executor
-- setup:
CREATE TABLE t_cd (id INT, val INT);
INSERT INTO t_cd VALUES (1, 10);
INSERT INTO t_cd VALUES (2, 20);
INSERT INTO t_cd VALUES (3, 10);
INSERT INTO t_cd VALUES (4, NULL);
INSERT INTO t_cd VALUES (5, 20);
INSERT INTO t_cd VALUES (6, 30);
-- input:
SELECT COUNT(DISTINCT val) FROM t_cd;
-- expected output:
3
