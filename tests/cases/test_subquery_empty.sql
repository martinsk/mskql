-- IN subquery returning empty result should match no rows
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE t2 (id INT);
-- input:
SELECT name FROM t1 WHERE id IN (SELECT id FROM t2);
-- expected output:
-- expected status: 0
