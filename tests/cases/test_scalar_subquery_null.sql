-- scalar subquery returning NULL
-- setup:
CREATE TABLE t1 (id INT, val INT);
CREATE TABLE t2 (id INT);
INSERT INTO t1 VALUES (1, 10), (2, 20);
-- input:
SELECT id, val FROM t1 WHERE val > (SELECT MAX(id) FROM t2) ORDER BY id;
-- expected output:
-- expected status: 0
