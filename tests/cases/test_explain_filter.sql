-- EXPLAIN: filter
-- setup:
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t (id, name) VALUES (1, 'a');
-- input:
EXPLAIN SELECT * FROM t WHERE id > 5;
-- expected output:
Filter: (id > 5)
  Seq Scan on t
-- expected status: 0
