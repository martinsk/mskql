-- NOT EXISTS as expression in SELECT list
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 VALUES (1);
CREATE TABLE t2 (id INT);
-- input:
SELECT NOT EXISTS(SELECT 1 FROM t2) FROM t1;
-- expected output:
t
-- expected status: 0
