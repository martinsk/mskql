-- BUG: WHERE clause with CAST expression (val::INT) fails with column not found
-- setup:
CREATE TABLE t (id INT, val TEXT);
INSERT INTO t VALUES (1, '100'), (2, '200'), (3, '50');
-- input:
SELECT * FROM t WHERE val::INT > 100 ORDER BY id;
-- expected output:
2|200
-- expected status: 0
