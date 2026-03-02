-- plan: HAVING with BETWEEN on aggregate result
-- setup:
CREATE TABLE t1 (id INT, category TEXT, amount INT);
INSERT INTO t1 VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'b', 40), (5, 'c', 100);
-- input:
SELECT category, SUM(amount) AS total FROM t1 GROUP BY category HAVING SUM(amount) BETWEEN 30 AND 70 ORDER BY category;
EXPLAIN SELECT category, SUM(amount) AS total FROM t1 GROUP BY category HAVING SUM(amount) BETWEEN 30 AND 70 ORDER BY category
-- expected output:
a|40
b|60
Sort
  Filter: (sum BETWEEN 30)
    HashAggregate
      Seq Scan on t1
-- expected status: 0
