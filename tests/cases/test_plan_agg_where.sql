-- plan: GROUP BY with WHERE filter before aggregation
-- setup:
CREATE TABLE t1 (id INT, category TEXT, amount INT);
INSERT INTO t1 VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'b', 40), (5, 'a', 50);
-- input:
SELECT category, SUM(amount) AS total FROM t1 WHERE amount > 15 GROUP BY category ORDER BY category;
-- expected output:
a|80
b|60
-- expected status: 0
