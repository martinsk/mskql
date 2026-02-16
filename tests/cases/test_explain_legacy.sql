-- EXPLAIN: plan executor for simple aggregates
-- setup:
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t (id, name) VALUES (1, 'a');
-- input:
EXPLAIN SELECT COUNT(*) FROM t;
-- expected output:
Aggregate
  Seq Scan on t
-- expected status: 0
