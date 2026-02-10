-- UPDATE SET a = b, b = a should swap using pre-update values (SQL standard)
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 (id, a, b) VALUES (1, 10, 20);
-- input:
UPDATE t1 SET a = b, b = a WHERE id = 1;
SELECT id, a, b FROM t1;
-- expected output:
UPDATE 1
1|20|10
-- expected status: 0
