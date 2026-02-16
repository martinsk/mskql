-- plan: GROUP BY with IN-list WHERE filter before aggregation
-- setup:
CREATE TABLE t1 (id INT, category TEXT, amount INT);
INSERT INTO t1 VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'a', 40), (5, 'b', 50);
-- input:
SELECT category, SUM(amount) AS total FROM t1 WHERE category IN ('a', 'b') GROUP BY category ORDER BY category;
-- expected output:
a|50
b|70
-- expected status: 0
