-- insert returning with default column padding
-- setup:
CREATE TABLE items (id INT, name TEXT, status TEXT DEFAULT 'active');
-- input:
INSERT INTO items (id, name) VALUES (1, 'widget') RETURNING *;
-- expected output:
1|widget|active
INSERT 0 1
-- expected status: 0
