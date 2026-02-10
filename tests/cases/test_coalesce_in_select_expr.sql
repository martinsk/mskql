-- COALESCE used in a computed SELECT expression with arithmetic
-- setup:
CREATE TABLE t1 (id INT, bonus INT);
INSERT INTO t1 (id, bonus) VALUES (1, 10), (2, NULL), (3, 30);
-- input:
SELECT id, COALESCE(bonus, 0) FROM t1 ORDER BY id;
-- expected output:
1|10
2|0
3|30
-- expected status: 0
