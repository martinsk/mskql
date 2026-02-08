-- insert select
-- setup:
CREATE TABLE src (id INT, name TEXT);
INSERT INTO src (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE dst (id INT, name TEXT);
-- input:
INSERT INTO dst SELECT * FROM src;
SELECT id, name FROM dst ORDER BY id;
-- expected output:
INSERT 0 2
1|alice
2|bob
-- expected status: 0
