-- bug: arithmetic on aggregate results fails (e.g. SUM(val) + 1)
-- setup:
CREATE TABLE t_agg_arith (id INT, val INT);
INSERT INTO t_agg_arith VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT SUM(val) + 1 FROM t_agg_arith;
-- expected output:
61
-- expected status: 0
