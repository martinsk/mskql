-- adversarial: NATURAL JOIN with no common columns — degrades to CROSS JOIN
-- setup:
CREATE TABLE t_nj1 (a INT);
CREATE TABLE t_nj2 (b INT);
INSERT INTO t_nj1 VALUES (1);
INSERT INTO t_nj2 VALUES (2);
-- input:
SELECT * FROM t_nj1 NATURAL JOIN t_nj2;
-- expected output:
1|2
