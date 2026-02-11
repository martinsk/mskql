-- NUMERIC/DECIMAL type basic
-- setup:
CREATE TABLE "t1" (id INT, price NUMERIC);
INSERT INTO "t1" VALUES (1, 19.99);
INSERT INTO "t1" VALUES (2, 5.50);
INSERT INTO "t1" VALUES (3, 100.00);
-- input:
SELECT * FROM "t1" ORDER BY price;
-- expected output:
2|5.5
1|19.99
3|100
-- expected status: 0
