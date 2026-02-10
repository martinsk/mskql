-- LEFT JOIN across three tables: unmatched rows should have NULLs
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
CREATE TABLE t2 (t1_id INT, score INT);
INSERT INTO t2 (t1_id, score) VALUES (1, 90), (3, 70);
CREATE TABLE t3 (t1_id INT, grade TEXT);
INSERT INTO t3 (t1_id, grade) VALUES (1, 'A');
-- input:
SELECT t1.name, t2.score, t3.grade FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id LEFT JOIN t3 ON t1.id = t3.t1_id ORDER BY t1.name;
-- expected output:
alice|90|A
bob||
carol|70|
-- expected status: 0
