-- INSERT INTO ... SELECT with WHERE filter
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
CREATE TABLE t2 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
INSERT INTO t2 SELECT id, name FROM t1 WHERE id > 1;
-- input:
SELECT id, name FROM t2 ORDER BY id;
-- expected output:
2|bob
3|charlie
-- expected status: 0
