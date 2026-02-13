-- EXPLAIN: index scan
-- setup:
CREATE TABLE t (id INT PRIMARY KEY, name TEXT);
INSERT INTO t (id, name) VALUES (1, 'a');
-- input:
EXPLAIN SELECT * FROM t WHERE id = 1;
-- expected output:
Filter: (id = 1)
  Seq Scan on t
-- expected status: 0
