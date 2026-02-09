-- LEFT JOIN + WHERE on right column filters out unmatched rows (common SQL pitfall)
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
CREATE TABLE t2 (t1_id INT, score INT);
INSERT INTO t2 (t1_id, score) VALUES (1, 90), (3, 70);
-- input:
SELECT t1.name, t2.score FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id WHERE t2.score > 50 ORDER BY t1.name;
-- expected output:
alice|90
carol|70
-- expected status: 0
