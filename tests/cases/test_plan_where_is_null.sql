-- plan: WHERE with IS NULL
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 10), (2, NULL), (3, 30), (4, NULL);
-- input:
SELECT id FROM t1 WHERE val IS NULL ORDER BY id;
EXPLAIN SELECT id FROM t1 WHERE val IS NULL ORDER BY id
-- expected output:
2
4
Project
  Sort (id)
    Filter: (val IS NULL 0)
      Seq Scan on t1
-- expected status: 0
