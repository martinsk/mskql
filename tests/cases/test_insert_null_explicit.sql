-- INSERT with explicit NULL value
-- setup:
CREATE TABLE t1 (id INT, name TEXT, val INT);
-- input:
INSERT INTO t1 (id, name, val) VALUES (1, NULL, NULL);
SELECT id, name, val FROM t1;
-- expected output:
INSERT 0 1
1||
-- expected status: 0
