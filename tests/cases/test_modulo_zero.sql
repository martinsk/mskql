-- modulo by zero should not crash (returns 0)
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10);
-- input:
SELECT id, val % 0 FROM t1;
-- expected output:
1|0
-- expected status: 0
