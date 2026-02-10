-- scalar subquery in WHERE with > comparison
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30);
CREATE TABLE t2 (avg_val INT);
INSERT INTO t2 (avg_val) VALUES (15);
-- input:
SELECT id FROM t1 WHERE val > (SELECT avg_val FROM t2) ORDER BY id;
-- expected output:
2
3
-- expected status: 0
