-- WHERE col = NULL should match nothing (SQL standard: use IS NULL instead)
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, NULL), (3, 20);
-- input:
SELECT id FROM t1 WHERE val = NULL;
-- expected output:
-- expected status: 0
