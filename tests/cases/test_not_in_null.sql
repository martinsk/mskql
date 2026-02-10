-- NOT IN with NULL in list should return no rows (SQL standard: x NOT IN (1, NULL) is UNKNOWN)
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id FROM t1 WHERE val NOT IN (10, NULL) ORDER BY id;
-- expected output:
-- expected status: 0
