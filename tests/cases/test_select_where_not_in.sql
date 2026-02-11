-- WHERE NOT IN with literal list
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id FROM t1 WHERE val NOT IN (10, 30) ORDER BY id;
-- expected output:
2
-- expected status: 0
