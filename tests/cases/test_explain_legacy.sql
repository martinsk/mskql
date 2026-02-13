-- EXPLAIN: legacy executor fallback for aggregates
-- setup:
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t (id, name) VALUES (1, 'a');
-- input:
EXPLAIN SELECT COUNT(*) FROM t;
-- expected output:
Legacy Row Executor
-- expected status: 0
