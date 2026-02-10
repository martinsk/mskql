-- WHERE with COALESCE function: WHERE COALESCE(val, 0) > 5
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, NULL), (3, 3);
-- input:
SELECT id FROM t1 WHERE COALESCE(val, 0) > 5 ORDER BY id;
-- expected output:
1
-- expected status: 0
