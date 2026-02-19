-- bug: COALESCE(COUNT(*), 0) fails with 'column * does not exist'
-- setup:
CREATE TABLE t_coalesce_count (id INT, val INT);
INSERT INTO t_coalesce_count VALUES (1, 10), (2, 20);
-- input:
SELECT COALESCE(COUNT(*), 0) FROM t_coalesce_count;
-- expected output:
2
-- expected status: 0
