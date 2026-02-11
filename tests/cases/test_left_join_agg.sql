-- LEFT JOIN with aggregate: COUNT should count matched rows only
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
CREATE TABLE t2 (user_id INT, item TEXT);
INSERT INTO t2 (user_id, item) VALUES (1, 'x'), (1, 'y'), (2, 'z');
-- input:
SELECT t1.name, COUNT(t2.item) FROM t1 LEFT JOIN t2 ON t1.id = t2.user_id GROUP BY t1.name ORDER BY t1.name;
-- expected output:
alice|2
bob|1
carol|0
-- expected status: 0
