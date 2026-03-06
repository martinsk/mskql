-- parser: pure aggregate list populates parsed_columns
-- setup:
CREATE TABLE t_pure_agg (val INT, score INT);
INSERT INTO t_pure_agg VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT SUM(val), AVG(score), COUNT(*) FROM t_pure_agg
-- expected output:
6|20|3
