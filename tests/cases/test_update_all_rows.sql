-- UPDATE without WHERE should update all rows
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
UPDATE t1 SET val = 0;
SELECT id, val FROM t1 ORDER BY id;
-- expected output:
UPDATE 3
1|0
2|0
3|0
-- expected status: 0
