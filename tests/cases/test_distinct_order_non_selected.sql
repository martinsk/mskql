-- DISTINCT with ORDER BY on the selected column
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'bob'), (2, 'alice'), (3, 'bob'), (4, 'carol');
-- input:
SELECT DISTINCT name FROM t1 ORDER BY name;
-- expected output:
alice
bob
carol
-- expected status: 0
