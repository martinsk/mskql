-- BUG: CAST expression in INSERT VALUES not supported
-- setup:
CREATE TABLE t (id INT, val FLOAT);
-- input:
INSERT INTO t VALUES (1, '3.14'::FLOAT);
-- expected output:
INSERT 0 1
-- expected status: 0
