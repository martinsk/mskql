-- UNION should deduplicate rows containing NULLs
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 (id, val) VALUES (1, NULL);
CREATE TABLE t2 (id INT, val TEXT);
INSERT INTO t2 (id, val) VALUES (1, NULL);
-- input:
SELECT id, val FROM t1 UNION SELECT id, val FROM t2;
-- expected output:
1|
-- expected status: 0
