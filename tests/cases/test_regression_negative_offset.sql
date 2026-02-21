-- BUG: Negative OFFSET should error, not return all rows
-- setup:
CREATE TABLE t (id INT);
INSERT INTO t VALUES (1), (2), (3);
-- input:
SELECT * FROM t ORDER BY id OFFSET -1;
-- expected output:
ERROR:  OFFSET must not be negative
-- expected status: 0
