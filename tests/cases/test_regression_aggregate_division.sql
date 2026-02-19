-- bug: arithmetic between aggregates fails (e.g. SUM(val) / COUNT(*))
-- setup:
CREATE TABLE t_agg_div (id INT, val INT);
INSERT INTO t_agg_div VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT SUM(val) / COUNT(*) AS manual_avg FROM t_agg_div;
-- expected output:
20
-- expected status: 0
