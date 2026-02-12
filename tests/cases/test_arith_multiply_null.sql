-- multiplication with NULL returns NULL
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, NULL), (2, 5);
-- input:
SELECT id, val * 2 FROM t1 ORDER BY id;
-- expected output:
1|
2|10
-- expected status: 0
