-- JOIN USING with a single column (basic test; multi-column USING is not supported)
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE t2 (id INT, score INT);
INSERT INTO t2 (id, score) VALUES (1, 90), (3, 70);
-- input:
SELECT t1.name, t2.score FROM t1 JOIN t2 USING (id);
-- expected output:
alice|90
-- expected status: 0
