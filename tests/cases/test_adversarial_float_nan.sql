-- adversarial: operations that could produce NaN or Infinity
-- setup:
CREATE TABLE t_fn (a FLOAT, b FLOAT);
INSERT INTO t_fn VALUES (0.0, 0.0);
INSERT INTO t_fn VALUES (1e308, 1e308);
-- input:
SELECT a / b, SQRT(-1.0) FROM t_fn LIMIT 1;
-- expected output:
0|nan
