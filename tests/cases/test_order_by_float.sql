-- order by on float column
-- setup:
CREATE TABLE "t1" (id INT, price FLOAT);
INSERT INTO "t1" (id, price) VALUES (1, 29.99), (2, 9.99), (3, 19.99);
-- input:
SELECT id, price FROM "t1" ORDER BY price;
-- expected output:
2|9.99
3|19.99
1|29.99
-- expected status: 0
