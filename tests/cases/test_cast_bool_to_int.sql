-- CAST boolean to integer
-- setup:
CREATE TABLE t1 (id INT, flag BOOLEAN);
INSERT INTO t1 VALUES (1, true), (2, false);
-- input:
SELECT id, CAST(flag AS INT) FROM t1 ORDER BY id;
-- expected output:
1|1
2|0
-- expected status: 0
