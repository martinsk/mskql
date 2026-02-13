-- EXPLAIN: limit
-- setup:
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t (id, name) VALUES (1, 'a');
-- input:
EXPLAIN SELECT * FROM t LIMIT 10;
-- expected output:
Limit (10)
  Seq Scan on t
-- expected status: 0
