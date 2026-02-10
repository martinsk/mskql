-- Multiple scalar subqueries in SELECT list
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT (SELECT MIN(val) FROM t1), (SELECT MAX(val) FROM t1);
-- expected output:
10|30
-- expected status: 0
