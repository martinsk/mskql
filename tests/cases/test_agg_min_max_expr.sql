-- MIN and MAX of an expression
-- setup:
CREATE TABLE items (id INT, price INT, discount INT);
INSERT INTO items (id, price, discount) VALUES (1, 100, 10), (2, 200, 50), (3, 50, 5);
-- input:
SELECT MIN(price - discount), MAX(price - discount) FROM items;
-- expected output:
45|150
-- expected status: 0
