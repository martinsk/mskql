-- UNION should deduplicate NULL rows
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
CREATE TABLE t2 (id INT, val TEXT);
INSERT INTO t1 VALUES (1, NULL);
INSERT INTO t2 VALUES (1, NULL);
-- input:
SELECT id, val FROM t1 UNION SELECT id, val FROM t2;
-- expected output:
1|
-- expected status: 0
