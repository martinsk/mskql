-- subquery with arithmetic in SELECT list
-- setup:
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE orders (id INT, user_id INT, amount INT);
INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 50), (2, 1, 30), (3, 2, 100);
-- input:
SELECT users.name, (SELECT SUM(amount) FROM orders WHERE orders.user_id = users.id) * 2 FROM users ORDER BY users.id;
-- expected output:
alice|160
bob|200
-- expected status: 0
