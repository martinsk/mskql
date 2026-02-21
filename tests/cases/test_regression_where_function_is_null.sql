-- BUG: WHERE clause with function result IS NULL fails with 'expected comparison operator in WHERE'
-- setup:
CREATE TABLE t (id INT, val TEXT);
INSERT INTO t VALUES (1, 'hello'), (2, NULL), (3, 'world');
-- input:
SELECT * FROM t WHERE UPPER(val) IS NULL ORDER BY id;
-- expected output:
2|
-- expected status: 0
