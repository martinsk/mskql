-- INSERT ... SELECT with transformation in the SELECT
-- setup:
CREATE TABLE src (id INT, name TEXT);
INSERT INTO src (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE dst (id INT, name TEXT);
-- input:
INSERT INTO dst SELECT id, UPPER(name) FROM src;
SELECT id, name FROM dst ORDER BY id;
-- expected output:
INSERT 0 2
1|ALICE
2|BOB
-- expected status: 0
