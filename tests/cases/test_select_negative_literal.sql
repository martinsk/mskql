-- SELECT with negative literal in expression
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10);
-- input:
SELECT id, val + -5 FROM t1;
-- expected output:
1|5
-- expected status: 0
