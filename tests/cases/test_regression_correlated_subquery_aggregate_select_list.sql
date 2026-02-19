-- bug: correlated subquery with aggregate (SUM) in SELECT list returns empty
-- setup:
CREATE TABLE t_corr_agg (id INT, val INT);
INSERT INTO t_corr_agg VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id, val, (SELECT SUM(b.val) FROM t_corr_agg b WHERE b.id <= t_corr_agg.id) AS running_sum FROM t_corr_agg ORDER BY id;
-- expected output:
1|10|10
2|20|30
3|30|60
-- expected status: 0
