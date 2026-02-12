-- CASE WHEN with scalar subquery comparison
-- setup:
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
CREATE TABLE orders (id INT, user_id INT, amount INT);
INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 50), (2, 1, 30), (3, 2, 100);
-- input:
SELECT users.name, CASE WHEN (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) > 1 THEN 'multiple' ELSE 'single or none' END FROM users ORDER BY users.id;
-- expected output:
alice|multiple
bob|single or none
carol|single or none
-- expected status: 0
