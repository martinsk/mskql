-- adversarial: UNION with different number of columns — should error
-- setup:
CREATE TABLE t_uc1 (a INT, b INT);
CREATE TABLE t_uc2 (a INT);
INSERT INTO t_uc1 VALUES (1, 2);
INSERT INTO t_uc2 VALUES (3);
-- input:
SELECT a, b FROM t_uc1 UNION ALL SELECT a FROM t_uc2;
-- expected status: error
