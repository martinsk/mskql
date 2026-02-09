-- select with column alias used in order by
-- setup:
CREATE TABLE items (id INT, name TEXT, price INT);
INSERT INTO items (id, name, price) VALUES (3, 'cherry', 8), (1, 'apple', 5), (2, 'banana', 3);
-- input:
SELECT name, price AS cost FROM items ORDER BY cost;
-- expected output:
banana|3
apple|5
cherry|8
-- expected status: 0
