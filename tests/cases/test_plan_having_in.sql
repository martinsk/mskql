-- plan: HAVING with IN-list on aggregate result
-- setup:
CREATE TABLE t1 (id INT, category TEXT, amount INT);
INSERT INTO t1 VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'b', 40), (5, 'c', 100);
-- input:
SELECT category, SUM(amount) AS total FROM t1 GROUP BY category HAVING SUM(amount) IN (40, 100) ORDER BY category;
-- expected output:
a|40
c|100
-- expected status: 0
