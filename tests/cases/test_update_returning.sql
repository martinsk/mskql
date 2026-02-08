-- update returning
-- setup:
CREATE TABLE updtest (id INT, name TEXT);
INSERT INTO updtest (id, name) VALUES (1, 'alice'), (2, 'bob');
-- input:
UPDATE updtest SET name = 'ALICE' WHERE id = 1 RETURNING *;
-- expected output:
1|ALICE
UPDATE 1
-- expected status: 0
