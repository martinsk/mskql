-- FULL JOIN where no rows match should return all rows with NULLs on opposite side
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE t2 (id INT, score INT);
INSERT INTO t2 (id, score) VALUES (3, 90), (4, 80);
-- input:
SELECT t1.name, t2.score FROM t1 FULL JOIN t2 ON t1.id = t2.id ORDER BY t1.name;
-- expected output:
alice|
bob|
|90
|80
-- expected status: 0
