-- FK INSERT: valid foreign key reference succeeds
-- setup:
CREATE TABLE parents (id INT PRIMARY KEY);
INSERT INTO parents (id) VALUES (1);
INSERT INTO parents (id) VALUES (2);
CREATE TABLE children (id INT, parent_id INT REFERENCES parents(id));
-- input:
INSERT INTO children (id, parent_id) VALUES (10, 1);
SELECT id, parent_id FROM children;
-- expected output:
INSERT 0 1
10|1
-- expected status: 0
