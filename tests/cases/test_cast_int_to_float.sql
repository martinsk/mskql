-- CAST integer to float
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 5);
-- input:
SELECT CAST(val AS FLOAT) FROM t1;
-- expected output:
5
-- expected status: 0
