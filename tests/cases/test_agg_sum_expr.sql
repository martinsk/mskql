-- SUM of an expression (price * quantity)
-- setup:
CREATE TABLE orders (id INT, price INT, quantity INT);
INSERT INTO orders (id, price, quantity) VALUES (1, 10, 3), (2, 20, 2), (3, 5, 10);
-- input:
SELECT SUM(price * quantity) FROM orders;
-- expected output:
120
-- expected status: 0
