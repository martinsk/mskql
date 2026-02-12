-- WHERE EXISTS with table alias
-- setup:
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
CREATE TABLE orders (id INT, user_id INT, amount INT);
INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 50), (2, 2, 100);
-- input:
SELECT u.name FROM users u WHERE EXISTS(SELECT 1 FROM orders o WHERE o.user_id = u.id) ORDER BY u.name;
-- expected output:
alice
bob
-- expected status: 0
