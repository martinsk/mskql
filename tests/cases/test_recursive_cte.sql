-- recursive CTE walking a parent-child hierarchy
-- setup:
CREATE TABLE tree (id INT, parent_id INT, name TEXT);
INSERT INTO tree (id, parent_id, name) VALUES (1, 0, 'root'), (2, 1, 'child1'), (3, 1, 'child2'), (4, 2, 'grandchild1');
-- input:
WITH RECURSIVE descendants AS (SELECT id, name FROM tree WHERE id = 1 UNION ALL SELECT tree.id, tree.name FROM tree JOIN descendants ON tree.parent_id = descendants.id) SELECT name FROM descendants ORDER BY id;
-- expected output:
root
child1
child2
grandchild1
-- expected status: 0
