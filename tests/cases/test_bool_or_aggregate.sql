-- BUG: bool_or aggregate function not supported
-- setup:
CREATE TABLE t (id INT, active BOOLEAN);
INSERT INTO t VALUES (1, TRUE), (2, FALSE), (3, TRUE);
-- input:
SELECT bool_or(active) FROM t;
-- expected output:
t
-- expected status: 0
