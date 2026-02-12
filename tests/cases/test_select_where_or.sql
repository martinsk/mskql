-- WHERE with OR condition
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30), (4, 40);
-- input:
SELECT id FROM t1 WHERE val = 10 OR val = 30 ORDER BY id;
-- expected output:
1
3
-- expected status: 0
