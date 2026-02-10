-- UPDATE SET multiple columns with expressions in single statement
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 (id, a, b) VALUES (1, 10, 20), (2, 30, 40);
-- input:
UPDATE t1 SET a = a * 2, b = b + 1 WHERE id = 1;
SELECT id, a, b FROM t1 ORDER BY id;
-- expected output:
UPDATE 1
1|20|21
2|30|40
-- expected status: 0
