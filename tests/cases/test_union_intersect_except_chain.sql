-- EXCEPT should remove matching rows
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 (id) VALUES (1), (2), (3), (4);
CREATE TABLE t2 (id INT);
INSERT INTO t2 (id) VALUES (2), (4);
-- input:
SELECT id FROM t1 EXCEPT SELECT id FROM t2 ORDER BY id;
-- expected output:
1
3
-- expected status: 0
