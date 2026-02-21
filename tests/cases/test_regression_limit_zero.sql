-- BUG: LIMIT 0 returns all rows instead of empty set
-- setup:
CREATE TABLE t (id INT);
INSERT INTO t VALUES (1), (2), (3);
-- input:
SELECT * FROM t ORDER BY id LIMIT 0;
-- expected output:
-- expected status: 0
