-- SUM of expression with GROUP BY
-- setup:
CREATE TABLE sales (category TEXT, price INT, quantity INT);
INSERT INTO sales (category, price, quantity) VALUES ('A', 10, 2), ('A', 20, 1), ('B', 5, 10), ('B', 15, 3);
-- input:
SELECT category, SUM(price * quantity) FROM sales GROUP BY category ORDER BY category;
-- expected output:
A|40
B|95
-- expected status: 0
