-- modulo by zero should return error not silently return 0
-- setup:
CREATE TABLE t (id INT, a INT, b INT);
INSERT INTO t VALUES (1, 10, 3), (2, 10, 0);
-- input:
SELECT id, a % b FROM t ORDER BY id;
-- expected output:
ERROR:  division by zero
-- expected status: 0
