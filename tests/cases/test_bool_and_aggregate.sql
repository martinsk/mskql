-- BUG: bool_and aggregate function not supported
-- setup:
CREATE TABLE t (id INT, active BOOLEAN);
INSERT INTO t VALUES (1, TRUE), (2, FALSE), (3, TRUE);
-- input:
SELECT bool_and(active) FROM t;
-- expected output:
f
-- expected status: 0
