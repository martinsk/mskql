-- arithmetic with float literal
-- setup:
CREATE TABLE "t1" (id INT, price INT);
INSERT INTO "t1" (id, price) VALUES (1, 100), (2, 200);
-- input:
SELECT id, price + 50 FROM "t1" ORDER BY id;
-- expected output:
1|150
2|250
-- expected status: 0
