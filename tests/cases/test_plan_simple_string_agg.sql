-- plan executor: STRING_AGG without GROUP BY (simple aggregate)
-- setup:
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
-- input:
SELECT STRING_AGG(name, ',') FROM t;
EXPLAIN SELECT STRING_AGG(name, ',') FROM t
-- expected output:
alice,bob,carol
Aggregate
  Seq Scan on t
