-- EXPLAIN: sort
-- setup:
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t (id, name) VALUES (1, 'a');
-- input:
EXPLAIN SELECT * FROM t ORDER BY id;
-- expected output:
Sort (id)
  Seq Scan on t
-- expected status: 0
