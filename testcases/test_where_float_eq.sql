-- where equality on float column
-- setup:
CREATE TABLE "t1" (id INT, price FLOAT);
INSERT INTO "t1" (id, price) VALUES (1, 9.99), (2, 19.99), (3, 29.99);
-- input:
SELECT id FROM "t1" WHERE price = 19.99;
-- expected output:
2
-- expected status: 0
