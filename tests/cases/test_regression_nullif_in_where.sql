-- BUG: NULLIF in WHERE clause fails with 'expected comparison operator in WHERE'
-- setup:
CREATE TABLE t (id INT, val TEXT);
INSERT INTO t VALUES (1, 'hello'), (2, 'world'), (3, NULL);
-- input:
SELECT * FROM t WHERE NULLIF(val, 'hello') IS NOT NULL ORDER BY id;
-- expected output:
2|world
-- expected status: 0
