-- plan executor: JOIN ... USING(col) single-column
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
CREATE TABLE t2 (id INT, score INT);
INSERT INTO t2 (id, score) VALUES (1, 90), (2, 80);
-- input:
SELECT t1.name, t2.score FROM t1 JOIN t2 USING (id) ORDER BY t1.name;
-- expected output:
alice|90
bob|80
