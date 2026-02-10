-- three-table JOIN should produce correct results
-- setup:
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE orders (id INT, user_id INT, product_id INT);
INSERT INTO orders (id, user_id, product_id) VALUES (1, 1, 10), (2, 1, 20), (3, 2, 10);
CREATE TABLE products (id INT, pname TEXT);
INSERT INTO products (id, pname) VALUES (10, 'widget'), (20, 'gadget');
-- input:
SELECT users.name, products.pname FROM users JOIN orders ON users.id = orders.user_id JOIN products ON orders.product_id = products.id ORDER BY users.name, products.pname;
-- expected output:
alice|gadget
alice|widget
bob|widget
-- expected status: 0
