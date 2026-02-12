-- INTERSECT with no common rows returns empty
-- setup:
CREATE TABLE t1 (id INT);
CREATE TABLE t2 (id INT);
INSERT INTO t1 VALUES (1), (2);
INSERT INTO t2 VALUES (3), (4);
-- input:
SELECT id FROM t1 INTERSECT SELECT id FROM t2;
-- expected output:
-- expected status: 0
