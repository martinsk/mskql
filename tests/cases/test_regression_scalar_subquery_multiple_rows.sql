-- BUG: Scalar subquery returning multiple rows should error, not silently return first row
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT (SELECT val FROM t);
-- expected output:
ERROR:  more than one row returned by a subquery used as an expression
-- expected status: 0
