-- CAST float to integer (truncates)
-- setup:
CREATE TABLE t1 (id INT, val FLOAT);
INSERT INTO t1 (id, val) VALUES (1, 3.7);
-- input:
SELECT CAST(val AS INT) FROM t1;
-- expected output:
3
-- expected status: 0
