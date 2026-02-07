-- arithmetic expressions in SELECT
-- setup:
CREATE TABLE "t1" (id INT, price INT, qty INT);
INSERT INTO "t1" (id, price, qty) VALUES (1, 10, 3), (2, 20, 5);
-- input:
SELECT id, price * qty FROM "t1" ORDER BY id;
-- expected output:
1|30
2|100
-- expected status: 0
