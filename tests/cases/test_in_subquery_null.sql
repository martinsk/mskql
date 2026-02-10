-- IN subquery where subquery returns NULLs should not match NULL rows
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, NULL), (3, 30);
CREATE TABLE t2 (x INT);
INSERT INTO t2 (x) VALUES (10), (NULL);
-- input:
SELECT id FROM t1 WHERE val IN (SELECT x FROM t2) ORDER BY id;
-- expected output:
1
-- expected status: 0
