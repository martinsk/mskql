-- BIGINT column should handle large values
-- setup:
CREATE TABLE t1 (id INT, big BIGINT);
INSERT INTO t1 (id, big) VALUES (1, 9999999999);
-- input:
SELECT id, big FROM t1;
-- expected output:
1|9999999999
-- expected status: 0
