-- FULL JOIN with WHERE filter
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE t2 (id INT, score INT);
INSERT INTO t2 (id, score) VALUES (2, 90), (3, 70);
-- input:
SELECT t1.name, t2.score FROM t1 FULL JOIN t2 ON t1.id = t2.id WHERE t2.score IS NOT NULL ORDER BY t2.score;
-- expected output:
|70
bob|90
-- expected status: 0
