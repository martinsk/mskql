-- FK DELETE SET DEFAULT: deleting referenced row sets FK column to DEFAULT
-- setup:
CREATE TABLE parents (id INT PRIMARY KEY);
INSERT INTO parents (id) VALUES (0);
INSERT INTO parents (id) VALUES (1);
INSERT INTO parents (id) VALUES (2);
CREATE TABLE children (id INT, parent_id INT DEFAULT 0 REFERENCES parents(id) ON DELETE SET DEFAULT);
INSERT INTO children (id, parent_id) VALUES (10, 1);
INSERT INTO children (id, parent_id) VALUES (20, 2);
-- input:
DELETE FROM parents WHERE id = 1;
SELECT id, parent_id FROM children ORDER BY id;
-- expected output:
DELETE 1
10|0
20|2
-- expected status: 0
