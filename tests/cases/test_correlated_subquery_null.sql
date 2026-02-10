-- correlated subquery in WHERE with NULL outer column should not match
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, NULL), (3, 30);
CREATE TABLE t2 (ref INT);
INSERT INTO t2 (ref) VALUES (10), (30);
-- input:
SELECT id FROM t1 WHERE val IN (SELECT ref FROM t2) ORDER BY id;
-- expected output:
1
3
-- expected status: 0
