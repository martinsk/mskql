-- UPDATE SET with expression referencing NULL column should produce NULL
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 (id, a, b) VALUES (1, 10, 5), (2, NULL, 5);
-- input:
UPDATE t1 SET b = a + b;
SELECT id, b FROM t1 ORDER BY id;
-- expected output:
UPDATE 2
1|15
2|
-- expected status: 0
