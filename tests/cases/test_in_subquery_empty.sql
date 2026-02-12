-- IN subquery that returns no rows should match nothing
-- setup:
CREATE TABLE t1 (id INT, val INT);
CREATE TABLE t2 (id INT);
INSERT INTO t1 VALUES (1, 10), (2, 20);
-- input:
SELECT id FROM t1 WHERE id IN (SELECT id FROM t2);
-- expected output:
-- expected status: 0
