-- NOT IN subquery that returns NULL should exclude all rows (SQL standard)
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 (id) VALUES (1), (2), (3);
CREATE TABLE t2 (val INT);
INSERT INTO t2 (val) VALUES (1), (NULL);
-- input:
SELECT id FROM t1 WHERE id NOT IN (SELECT val FROM t2) ORDER BY id;
-- expected output:
-- expected status: 0
