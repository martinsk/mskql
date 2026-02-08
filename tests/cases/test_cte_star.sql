-- CTE with SELECT star
-- setup:
CREATE TABLE items (id INT, name TEXT, price INT);
INSERT INTO items (id, name, price) VALUES (1, 'apple', 5), (2, 'banana', 3), (3, 'cherry', 8);
-- input:
WITH cheap AS (SELECT * FROM items WHERE price < 6) SELECT id, name FROM cheap ORDER BY id;
-- expected output:
1|apple
2|banana
-- expected status: 0
