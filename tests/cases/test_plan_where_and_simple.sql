-- plan: WHERE with AND of two simple comparisons
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
-- input:
SELECT id, val FROM t1 WHERE val >= 20 AND val <= 40 ORDER BY id;
EXPLAIN SELECT id, val FROM t1 WHERE val >= 20 AND val <= 40 ORDER BY id
-- expected output:
2|20
3|30
4|40
Project
  Sort
    Filter: (val <= 40)
      Filter: (val >= 20)
        Seq Scan on t1
-- expected status: 0
