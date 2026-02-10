-- WHERE NOT condition
-- setup:
CREATE TABLE t1 (id INT, active INT);
INSERT INTO t1 (id, active) VALUES (1, 1), (2, 0), (3, 1);
-- input:
SELECT id FROM t1 WHERE NOT active = 1 ORDER BY id;
-- expected output:
2
-- expected status: 0
