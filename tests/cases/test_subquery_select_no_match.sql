-- scalar subquery in SELECT list returning no rows should yield NULL
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
CREATE TABLE t2 (id INT, score INT);
-- input:
SELECT t1.name, (SELECT score FROM t2 WHERE t2.id = t1.id) FROM t1;
-- expected output:
alice|
-- expected status: 0
