-- plan: WHERE with IN-list combined with AND comparison
-- setup:
CREATE TABLE t1 (id INT, category TEXT, val INT);
INSERT INTO t1 VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'c', 40), (5, 'b', 50);
-- input:
SELECT id FROM t1 WHERE category IN ('a', 'b') AND val > 15 ORDER BY id;
-- expected output:
2
3
5
-- expected status: 0
