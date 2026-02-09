-- CASE WHEN with NULL column value should fall through to ELSE
-- setup:
CREATE TABLE items (id INT, name TEXT);
INSERT INTO items (id, name) VALUES (1, 'alice'), (2, NULL), (3, 'charlie');
-- input:
SELECT id, CASE WHEN name = 'alice' THEN 'found' ELSE 'missing' END FROM items ORDER BY id;
-- expected output:
1|found
2|missing
3|missing
-- expected status: 0
