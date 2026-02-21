-- BUG: TRUNCATE TABLE ... CASCADE does not truncate referencing tables
-- setup:
CREATE TABLE parent (id INT PRIMARY KEY);
CREATE TABLE child (id INT, p_id INT REFERENCES parent(id));
INSERT INTO parent VALUES (1), (2);
INSERT INTO child VALUES (1, 1), (2, 2);
TRUNCATE TABLE parent CASCADE;
-- input:
SELECT COUNT(*) FROM child;
-- expected output:
0
-- expected status: 0
