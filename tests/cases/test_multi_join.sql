-- multiple joins in one query
-- setup:
CREATE TABLE "users" (id INT, name TEXT);
INSERT INTO "users" (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE "orders" (id INT, user_id INT, product_id INT);
INSERT INTO "orders" (id, user_id, product_id) VALUES (10, 1, 100), (11, 2, 101);
CREATE TABLE "products" (id INT, pname TEXT);
INSERT INTO "products" (id, pname) VALUES (100, 'widget'), (101, 'gadget');
-- input:
SELECT name, pname FROM users JOIN orders ON users.id = orders.user_id JOIN products ON orders.product_id = products.id ORDER BY name;
-- expected output:
alice|widget
bob|gadget
-- expected status: 0
