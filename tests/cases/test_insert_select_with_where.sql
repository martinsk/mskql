-- INSERT ... SELECT with WHERE clause should only insert matching rows
-- setup:
CREATE TABLE src (id INT, name TEXT, active INT);
INSERT INTO src (id, name, active) VALUES (1, 'alice', 1), (2, 'bob', 0), (3, 'carol', 1);
CREATE TABLE dst (id INT, name TEXT);
-- input:
INSERT INTO dst SELECT id, name FROM src WHERE active = 1;
SELECT id, name FROM dst ORDER BY id;
-- expected output:
INSERT 0 2
1|alice
3|carol
-- expected status: 0
