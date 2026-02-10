-- WHERE with OR and NULL: (val = 10 OR val IS NULL) should match NULLs
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, NULL), (3, 20);
-- input:
SELECT id FROM t1 WHERE val = 10 OR val IS NULL ORDER BY id;
-- expected output:
1
2
-- expected status: 0
