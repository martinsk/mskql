-- arithmetic multiplication in select
-- setup:
CREATE TABLE "t1" (id INT, price FLOAT, qty INT);
INSERT INTO "t1" (id, price, qty) VALUES (1, 9.99, 3), (2, 19.99, 2);
-- input:
SELECT id, price * qty FROM "t1" ORDER BY id;
-- expected output:
1|29.97
2|39.98
-- expected status: 0
