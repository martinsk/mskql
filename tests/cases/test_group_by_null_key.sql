-- GROUP BY should group NULL keys together into one group
-- setup:
CREATE TABLE items (category TEXT, price INT);
INSERT INTO items (category, price) VALUES ('a', 10), (NULL, 20), ('a', 30), (NULL, 40);
-- input:
SELECT category, SUM(price) FROM items GROUP BY category ORDER BY category;
-- expected output:
a|40
|60
-- expected status: 0
