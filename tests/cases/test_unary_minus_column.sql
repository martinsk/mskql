-- unary minus on column reference
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 5), (2, -3), (3, 0);
-- input:
SELECT id, -val FROM t1 ORDER BY id;
-- expected output:
1|-5
2|3
3|0
-- expected status: 0
