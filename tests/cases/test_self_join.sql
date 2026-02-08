-- self join with aliases
-- setup:
CREATE TABLE nodes (id INT, parent_id INT, name TEXT);
INSERT INTO nodes (id, parent_id, name) VALUES (1, 0, 'root'), (2, 1, 'child1'), (3, 1, 'child2');
-- input:
SELECT a.name, b.name FROM nodes a JOIN nodes b ON a.id = b.parent_id ORDER BY b.name;
-- expected output:
root|child1
root|child2
-- expected status: 0
