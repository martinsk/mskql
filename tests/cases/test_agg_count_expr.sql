-- COUNT of an expression (only counts non-NULL results)
-- setup:
CREATE TABLE items (id INT, val INT);
INSERT INTO items (id, val) VALUES (1, 10), (2, NULL), (3, 20), (4, NULL);
-- input:
SELECT COUNT(val + 1) FROM items;
-- expected output:
2
-- expected status: 0
