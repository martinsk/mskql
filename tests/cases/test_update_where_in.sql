-- UPDATE with WHERE ... IN list
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
UPDATE t1 SET val = 0 WHERE id IN (1, 3);
SELECT id, val FROM t1 ORDER BY id;
-- expected output:
UPDATE 2
1|0
2|20
3|0
-- expected status: 0
