-- EXCEPT where all LHS rows are in RHS returns empty
-- setup:
CREATE TABLE t1 (id INT);
CREATE TABLE t2 (id INT);
INSERT INTO t1 VALUES (1), (2);
INSERT INTO t2 VALUES (1), (2), (3);
-- input:
SELECT id FROM t1 EXCEPT SELECT id FROM t2;
-- expected output:
-- expected status: 0
