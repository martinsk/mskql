-- unary negation in SELECT expression
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 42), (2, -10);
-- input:
SELECT id, -val FROM t1 ORDER BY id;
-- expected output:
1|-42
2|10
-- expected status: 0
