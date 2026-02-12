-- EXCEPT ALL keeps LHS rows not found in RHS (with duplicates)
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 VALUES (1);
INSERT INTO t1 VALUES (2);
INSERT INTO t1 VALUES (2);
INSERT INTO t1 VALUES (3);
CREATE TABLE t2 (id INT);
INSERT INTO t2 VALUES (2);
INSERT INTO t2 VALUES (3);
-- input:
SELECT id FROM t1 EXCEPT ALL SELECT id FROM t2;
-- expected output:
1
-- expected status: 0
