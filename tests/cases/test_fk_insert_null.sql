-- FK INSERT: NULL in FK column is allowed (NULLs bypass FK check)
-- setup:
CREATE TABLE parents (id INT PRIMARY KEY);
INSERT INTO parents (id) VALUES (1);
CREATE TABLE children (id INT, parent_id INT REFERENCES parents(id));
-- input:
INSERT INTO children (id, parent_id) VALUES (10, NULL);
SELECT id, parent_id FROM children;
-- expected output:
INSERT 0 1
10|
-- expected status: 0
