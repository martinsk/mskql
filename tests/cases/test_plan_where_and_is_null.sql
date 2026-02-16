-- plan: WHERE with IS NOT NULL combined with AND
-- setup:
CREATE TABLE t1 (id INT, val INT, name TEXT);
INSERT INTO t1 VALUES (1, 10, 'a'), (2, NULL, 'b'), (3, 30, NULL), (4, NULL, NULL);
-- input:
SELECT id FROM t1 WHERE val IS NOT NULL AND name IS NOT NULL ORDER BY id;
-- expected output:
1
-- expected status: 0
