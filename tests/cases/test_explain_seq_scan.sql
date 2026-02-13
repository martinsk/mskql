-- EXPLAIN: sequential scan
-- setup:
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t (id, name) VALUES (1, 'a');
-- input:
EXPLAIN SELECT * FROM t;
-- expected output:
Seq Scan on t
-- expected status: 0
