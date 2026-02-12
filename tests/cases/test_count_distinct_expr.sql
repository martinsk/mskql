-- COUNT(DISTINCT expr) counts distinct non-null expression values
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 VALUES (1, 10, 5);
INSERT INTO t1 VALUES (2, 20, 10);
INSERT INTO t1 VALUES (3, 10, 5);
INSERT INTO t1 VALUES (4, 30, 0);
-- input:
SELECT COUNT(DISTINCT a + b) FROM t1;
-- expected output:
2
-- expected status: 0
