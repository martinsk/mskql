-- NOT EXISTS subquery in SELECT list
-- setup:
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
CREATE TABLE orders (id INT, user_id INT, amount INT);
INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 50), (2, 2, 100);
-- input:
SELECT users.name, NOT EXISTS(SELECT 1 FROM orders WHERE orders.user_id = users.id) FROM users ORDER BY users.id;
-- expected output:
alice|f
bob|f
carol|t
-- expected status: 0
