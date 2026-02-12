-- SUM of expression with WHERE clause
-- setup:
CREATE TABLE orders (id INT, price INT, quantity INT, status TEXT);
INSERT INTO orders (id, price, quantity, status) VALUES (1, 10, 3, 'shipped'), (2, 20, 2, 'pending'), (3, 5, 10, 'shipped');
-- input:
SELECT SUM(price * quantity) FROM orders WHERE status = 'shipped';
-- expected output:
80
-- expected status: 0
