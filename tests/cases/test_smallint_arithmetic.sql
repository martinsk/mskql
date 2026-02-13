-- SMALLINT arithmetic: SELECT a + b
-- setup:
CREATE TABLE t1 (a SMALLINT, b SMALLINT);
INSERT INTO t1 (a, b) VALUES (100, 200);
INSERT INTO t1 (a, b) VALUES (-50, 50);
-- input:
SELECT a + b FROM t1 ORDER BY a;
-- expected output:
0
300
-- expected status: 0
