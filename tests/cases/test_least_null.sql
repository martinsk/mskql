-- LEAST should skip NULLs and return least non-null (SQL standard)
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT, c INT);
INSERT INTO t1 (id, a, b, c) VALUES (1, 10, NULL, 30), (2, NULL, NULL, NULL);
-- input:
SELECT id, LEAST(a, b, c) FROM t1 ORDER BY id;
-- expected output:
1|10
2|
-- expected status: 0
