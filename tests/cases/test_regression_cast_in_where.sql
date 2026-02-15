-- regression: type cast in WHERE clause
-- setup:
CREATE TABLE t (id INT, val TEXT);
INSERT INTO t VALUES (1,'10'),(2,'20'),(3,'5');
-- input:
SELECT id FROM t WHERE val::INT > 15 ORDER BY id;
-- expected output:
2
