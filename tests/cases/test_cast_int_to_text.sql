-- CAST integer to text
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 42);
-- input:
SELECT CAST(val AS TEXT) FROM t1;
-- expected output:
42
-- expected status: 0
