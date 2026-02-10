-- UPDATE SET with self-referencing arithmetic: val = val * 2 + 1
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 5), (2, 10);
-- input:
UPDATE t1 SET val = val * 2 + 1 WHERE id = 1;
SELECT id, val FROM t1 ORDER BY id;
-- expected output:
UPDATE 1
1|11
2|10
-- expected status: 0
