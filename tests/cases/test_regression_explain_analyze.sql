-- BUG: EXPLAIN ANALYZE not supported (errors with 'expected SQL keyword')
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 20);
-- input:
EXPLAIN ANALYZE SELECT * FROM t WHERE val > 10;
-- expected output:
Filter: (val > 10)
  Seq Scan on t
-- expected status: 0
