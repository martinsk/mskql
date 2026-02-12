-- CAST(expr AS BIGINT) syntax
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 999);
-- input:
SELECT CAST(val AS BIGINT) FROM t1;
-- expected output:
999
-- expected status: 0
