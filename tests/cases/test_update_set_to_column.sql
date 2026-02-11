-- UPDATE SET col1 = col2 (copy value from another column)
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 (id, a, b) VALUES (1, 10, 99), (2, 20, 88);
-- input:
UPDATE t1 SET a = b;
SELECT id, a, b FROM t1 ORDER BY id;
-- expected output:
UPDATE 2
1|99|99
2|88|88
-- expected status: 0
