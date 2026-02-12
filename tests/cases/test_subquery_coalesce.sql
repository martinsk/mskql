-- COALESCE with correlated subquery
-- setup:
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
CREATE TABLE orders (id INT, user_id INT, amount INT);
INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 50), (2, 1, 30), (3, 2, 100);
-- input:
SELECT u.name, COALESCE((SELECT SUM(amount) FROM orders o WHERE o.user_id = u.id), 0) FROM users u ORDER BY u.id;
-- expected output:
alice|80
bob|100
carol|0
-- expected status: 0
