-- delete returning specific columns
-- setup:
CREATE TABLE items (id INT, name TEXT, price INT);
INSERT INTO items (id, name, price) VALUES (1, 'apple', 5), (2, 'banana', 3), (3, 'cherry', 8);
-- input:
DELETE FROM items WHERE price < 6 RETURNING id, name;
-- expected output:
1|apple
2|banana
DELETE 2
-- expected status: 0
