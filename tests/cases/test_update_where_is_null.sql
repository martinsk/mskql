-- UPDATE with WHERE col IS NULL
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, NULL), (3, NULL);
-- input:
UPDATE t1 SET val = 0 WHERE val IS NULL;
SELECT id, val FROM t1 ORDER BY id;
-- expected output:
UPDATE 2
1|10
2|0
3|0
-- expected status: 0
