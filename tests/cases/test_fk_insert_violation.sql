-- FK INSERT: invalid foreign key reference errors
-- setup:
CREATE TABLE parents (id INT PRIMARY KEY);
INSERT INTO parents (id) VALUES (1);
CREATE TABLE children (id INT, parent_id INT REFERENCES parents(id));
-- input:
INSERT INTO children (id, parent_id) VALUES (10, 999);
-- expected output:
ERROR:  insert or update on table 'children' violates foreign key constraint on column 'parent_id'
-- expected status: 1
