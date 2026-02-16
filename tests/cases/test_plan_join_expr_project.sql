-- plan: JOIN with expression projection (UPPER, COALESCE)
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
CREATE TABLE t2 (fk INT, val INT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob');
INSERT INTO t2 VALUES (1, 100), (2, 200);
-- input:
SELECT UPPER(t1.name), t2.val * 2 FROM t1 JOIN t2 ON t1.id = t2.fk ORDER BY t1.name;
-- expected output:
ALICE|200
BOB|400
-- expected status: 0
