-- delete returning
-- setup:
CREATE TABLE deltest (id INT, name TEXT);
INSERT INTO deltest (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
-- input:
DELETE FROM deltest WHERE id = 2 RETURNING *;
-- expected output:
2|bob
DELETE 1
-- expected status: 0
