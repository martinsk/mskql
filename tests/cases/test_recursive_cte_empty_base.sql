-- recursive CTE with empty base case should return no rows
-- setup:
CREATE TABLE t1 (id INT, parent_id INT);
-- input:
WITH RECURSIVE tree AS (SELECT id, parent_id FROM t1 WHERE id = 999 UNION ALL SELECT t1.id, t1.parent_id FROM t1 JOIN tree ON t1.parent_id = tree.id) SELECT id FROM tree ORDER BY id;
-- expected output:
-- expected status: 0
