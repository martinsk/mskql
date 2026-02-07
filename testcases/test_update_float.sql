-- update with float literal value
-- setup:
CREATE TABLE "t1" (id INT, price FLOAT);
INSERT INTO "t1" (id, price) VALUES (1, 9.99), (2, 19.99);
UPDATE "t1" SET price = 5.55 WHERE id = 1;
-- input:
SELECT * FROM "t1" ORDER BY id;
-- expected output:
1|5.55
2|19.99
-- expected status: 0
