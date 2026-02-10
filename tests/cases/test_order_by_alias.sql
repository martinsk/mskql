-- ORDER BY using a SELECT alias
-- setup:
CREATE TABLE t1 (id INT, first_name TEXT, last_name TEXT);
INSERT INTO t1 (id, first_name, last_name) VALUES (1, 'Bob', 'Smith'), (2, 'Alice', 'Jones');
-- input:
SELECT id, first_name AS name FROM t1 ORDER BY name;
-- expected output:
2|Alice
1|Bob
-- expected status: 0
