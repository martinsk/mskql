-- subquery in FROM clause
-- setup:
CREATE TABLE products (id INT, name TEXT, price INT);
INSERT INTO products (id, name, price) VALUES (1, 'apple', 5), (2, 'banana', 3), (3, 'cherry', 8);
-- input:
SELECT name, price FROM (SELECT name, price FROM products WHERE price > 3) AS sub ORDER BY price;
-- expected output:
apple|5
cherry|8
-- expected status: 0
