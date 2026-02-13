-- FK DELETE RESTRICT: deleting referenced row errors
-- setup:
CREATE TABLE parents (id INT PRIMARY KEY);
INSERT INTO parents (id) VALUES (1);
INSERT INTO parents (id) VALUES (2);
CREATE TABLE children (id INT, parent_id INT REFERENCES parents(id));
INSERT INTO children (id, parent_id) VALUES (10, 1);
-- input:
DELETE FROM parents WHERE id = 1;
-- expected output:
ERROR:  update or delete on table 'parents' violates foreign key constraint on table 'children'
-- expected status: 1
