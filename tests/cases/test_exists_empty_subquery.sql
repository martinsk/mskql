-- EXISTS with empty subquery returns false
-- setup:
CREATE TABLE t1 (id INT);
CREATE TABLE t2 (id INT);
INSERT INTO t1 VALUES (1), (2);
-- input:
SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2) ORDER BY id;
-- expected output:
-- expected status: 0
