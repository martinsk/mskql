-- OFFSET without LIMIT should skip rows and return the rest
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 (id) VALUES (1), (2), (3), (4), (5);
-- input:
SELECT id FROM t1 ORDER BY id OFFSET 3;
-- expected output:
4
5
-- expected status: 0
