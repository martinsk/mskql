-- NOT EXISTS with correlated subquery
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
CREATE TABLE t2 (user_id INT);
INSERT INTO t2 (user_id) VALUES (1), (3);
-- input:
SELECT name FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.user_id = t1.id) ORDER BY name;
-- expected output:
bob
-- expected status: 0
