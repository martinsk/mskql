-- adversarial: GROUP BY where the grouping column is all NULL
-- setup:
CREATE TABLE t_gbn (category TEXT, value INT);
INSERT INTO t_gbn VALUES (NULL, 10);
INSERT INTO t_gbn VALUES (NULL, 20);
INSERT INTO t_gbn VALUES (NULL, 30);
-- input:
SELECT category, SUM(value) FROM t_gbn GROUP BY category;
-- expected output:
|60
