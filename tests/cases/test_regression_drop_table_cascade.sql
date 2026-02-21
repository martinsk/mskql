-- BUG: DROP TABLE ... CASCADE does not drop referencing tables
-- setup:
CREATE TABLE parent (id INT PRIMARY KEY);
CREATE TABLE child (id INT, p_id INT REFERENCES parent(id));
INSERT INTO parent VALUES (1);
INSERT INTO child VALUES (1, 1);
DROP TABLE parent CASCADE;
-- input:
SELECT COUNT(*) FROM child;
-- expected output:
ERROR:  table 'child' not found
-- expected status: 0
