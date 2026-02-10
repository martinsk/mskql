-- scalar subquery in SELECT list
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE t2 (id INT, score INT);
INSERT INTO t2 (id, score) VALUES (1, 90), (2, 80);
-- input:
SELECT t1.name, (SELECT score FROM t2 WHERE t2.id = t1.id) FROM t1 ORDER BY t1.id;
-- expected output:
alice|90
bob|80
-- expected status: 0
