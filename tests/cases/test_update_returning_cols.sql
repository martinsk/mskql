-- update returning specific columns
-- setup:
CREATE TABLE items (id INT, name TEXT, price INT);
INSERT INTO items (id, name, price) VALUES (1, 'apple', 5), (2, 'banana', 3);
-- input:
UPDATE items SET price = 10 WHERE id = 1 RETURNING id, price;
-- expected output:
1|10
UPDATE 1
-- expected status: 0
