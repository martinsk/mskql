-- NOT EXISTS with empty subquery should return all rows
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE t2 (id INT);
-- input:
SELECT name FROM t1 WHERE NOT EXISTS (SELECT id FROM t2) ORDER BY name;
-- expected output:
alice
bob
-- expected status: 0
