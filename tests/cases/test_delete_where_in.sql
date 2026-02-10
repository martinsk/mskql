-- DELETE with WHERE ... IN list
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
DELETE FROM t1 WHERE id IN (1, 3);
SELECT id, val FROM t1;
-- expected output:
DELETE 2
2|20
-- expected status: 0
