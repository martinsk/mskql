-- inner join
-- setup:
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE orders (id INT, user_id INT, item TEXT);
INSERT INTO orders (id, user_id, item) VALUES (10, 1, 'book'), (11, 2, 'pen');
-- input:
SELECT users.name, orders.item FROM users INNER JOIN orders ON users.id = orders.user_id ORDER BY orders.item;
-- expected output:
alice|book
bob|pen
-- expected status: 0
