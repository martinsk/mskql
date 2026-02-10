-- RIGHT JOIN should include unmatched right rows with NULL left columns
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
CREATE TABLE t2 (id INT, score INT);
INSERT INTO t2 (id, score) VALUES (1, 90), (2, 80);
-- input:
SELECT t1.name, t2.score FROM t1 RIGHT JOIN t2 ON t1.id = t2.id ORDER BY t2.score;
-- expected output:
|80
alice|90
-- expected status: 0
