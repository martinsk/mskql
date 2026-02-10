-- COALESCE with three arguments, first two NULL
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT, c INT);
INSERT INTO t1 (id, a, b, c) VALUES (1, NULL, NULL, 42), (2, NULL, 10, 20), (3, 5, NULL, NULL);
-- input:
SELECT id, COALESCE(a, b, c) FROM t1 ORDER BY id;
-- expected output:
1|42
2|10
3|5
-- expected status: 0
