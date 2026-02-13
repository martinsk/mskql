-- adversarial: CROSS JOIN with one empty table — should produce zero rows
-- setup:
CREATE TABLE t_cj1 (a INT);
CREATE TABLE t_cj2 (b INT);
INSERT INTO t_cj1 VALUES (1);
INSERT INTO t_cj1 VALUES (2);
-- input:
SELECT * FROM t_cj1 CROSS JOIN t_cj2;
-- expected output:
