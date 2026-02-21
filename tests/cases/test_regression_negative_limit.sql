-- BUG: Negative LIMIT should error, not return all rows
-- setup:
CREATE TABLE t (id INT);
INSERT INTO t VALUES (1), (2), (3);
-- input:
SELECT * FROM t ORDER BY id LIMIT -1;
-- expected output:
ERROR:  LIMIT must not be negative
-- expected status: 0
