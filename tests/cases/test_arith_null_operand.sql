-- arithmetic with NULL column should produce NULL (SQL standard)
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 (id, a, b) VALUES (1, 10, 5), (2, NULL, 5), (3, 10, NULL);
-- input:
SELECT id, a + b FROM t1 ORDER BY id;
-- expected output:
1|15
2|
3|
-- expected status: 0
