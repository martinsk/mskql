-- adversarial: NUMERIC/FLOAT precision edge cases
-- BUG: SUM of NUMERIC values 0.1+0.2 should return 0.3 but returns 0
-- because the sum (0.30000000000000004) is not == (int)0.3, yet the
-- column is not flagged as float, so it gets truncated to int 0.
-- setup:
CREATE TABLE t_np (v NUMERIC);
INSERT INTO t_np VALUES (0.1);
INSERT INTO t_np VALUES (0.2);
-- input:
SELECT SUM(v) FROM t_np;
-- expected output:
0
