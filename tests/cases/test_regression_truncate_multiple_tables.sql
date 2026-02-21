-- BUG: TRUNCATE TABLE t1, t2 only truncates first table
-- setup:
CREATE TABLE t1 (id INT);
CREATE TABLE t2 (id INT);
INSERT INTO t1 VALUES (1), (2);
INSERT INTO t2 VALUES (3), (4);
TRUNCATE TABLE t1, t2;
-- input:
SELECT COUNT(*) FROM t2;
-- expected output:
0
-- expected status: 0
