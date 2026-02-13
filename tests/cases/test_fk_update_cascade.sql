-- FK UPDATE CASCADE: updating referenced column cascades to children
-- setup:
CREATE TABLE parents (id INT PRIMARY KEY);
INSERT INTO parents (id) VALUES (1);
INSERT INTO parents (id) VALUES (2);
CREATE TABLE children (id INT, parent_id INT REFERENCES parents(id) ON UPDATE CASCADE);
INSERT INTO children (id, parent_id) VALUES (10, 1);
INSERT INTO children (id, parent_id) VALUES (20, 2);
-- input:
UPDATE parents SET id = 100 WHERE id = 1;
SELECT id, parent_id FROM children ORDER BY id;
-- expected output:
UPDATE 1
10|100
20|2
-- expected status: 0
