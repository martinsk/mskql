-- adversarial: NUMERIC/FLOAT precision edge cases
-- SUM of NUMERIC values 0.1+0.2 should return 0.3
-- setup:
CREATE TABLE t_np (v NUMERIC);
INSERT INTO t_np VALUES (0.1);
INSERT INTO t_np VALUES (0.2);
-- input:
SELECT SUM(v) FROM t_np;
-- expected output:
0.3
