-- BUG: IS UNKNOWN not supported in WHERE clause
-- setup:
CREATE TABLE t (id INT, flag BOOLEAN);
INSERT INTO t VALUES (1, TRUE), (2, FALSE), (3, NULL);
-- input:
SELECT * FROM t WHERE flag IS UNKNOWN ORDER BY id;
-- expected output:
3|
-- expected status: 0
