-- Test COUNT(DISTINCT col) with GROUP BY through plan executor
-- setup:
CREATE TABLE t_cdg (category TEXT, val INT);
INSERT INTO t_cdg VALUES ('a', 10);
INSERT INTO t_cdg VALUES ('a', 20);
INSERT INTO t_cdg VALUES ('a', 10);
INSERT INTO t_cdg VALUES ('a', NULL);
INSERT INTO t_cdg VALUES ('b', 30);
INSERT INTO t_cdg VALUES ('b', 30);
INSERT INTO t_cdg VALUES ('b', 40);
-- input:
SELECT category, COUNT(DISTINCT val) FROM t_cdg GROUP BY category ORDER BY category;
-- expected output:
a|2
b|2
