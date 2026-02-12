-- arithmetic with NULL propagates NULL
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 VALUES (1, 5, NULL), (2, NULL, 3), (3, 4, 2);
-- input:
SELECT id, a + b FROM t1 ORDER BY id;
-- expected output:
1|
2|
3|6
-- expected status: 0
