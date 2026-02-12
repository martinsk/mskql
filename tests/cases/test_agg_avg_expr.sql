-- AVG of an expression (price * quantity)
-- setup:
CREATE TABLE orders (id INT, price FLOAT, quantity INT);
INSERT INTO orders (id, price, quantity) VALUES (1, 10.0, 3), (2, 20.0, 2), (3, 5.0, 10);
-- input:
SELECT AVG(price * quantity) FROM orders;
-- expected output:
40
-- expected status: 0
