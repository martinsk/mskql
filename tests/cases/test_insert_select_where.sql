-- insert select with where filter
-- setup:
CREATE TABLE src (id INT, name TEXT);
INSERT INTO src (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
CREATE TABLE dst (id INT, name TEXT);
-- input:
INSERT INTO dst SELECT id, name FROM src WHERE id > 1;
SELECT id, name FROM dst ORDER BY id;
-- expected output:
INSERT 0 2
2|bob
3|charlie
-- expected status: 0
