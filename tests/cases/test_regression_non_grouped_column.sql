-- BUG: Non-grouped column in SELECT with GROUP BY should error
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id, val, COUNT(*) FROM t GROUP BY id;
-- expected output:
ERROR:  column "val" must appear in the GROUP BY clause or be used in an aggregate function
-- expected status: 0
