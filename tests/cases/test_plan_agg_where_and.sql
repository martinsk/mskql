-- plan: GROUP BY with compound AND WHERE filter before aggregation
-- setup:
CREATE TABLE t1 (id INT, category TEXT, amount INT, active INT);
INSERT INTO t1 VALUES (1, 'a', 10, 1), (2, 'b', 20, 0), (3, 'a', 30, 1), (4, 'b', 40, 1), (5, 'a', 50, 0);
-- input:
SELECT category, SUM(amount) AS total FROM t1 WHERE amount > 15 AND active = 1 GROUP BY category ORDER BY category;
-- expected output:
a|30
b|40
-- expected status: 0
