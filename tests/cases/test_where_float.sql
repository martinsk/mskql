-- where clause on float column with float literal
-- setup:
CREATE TABLE "t1" (id INT, price FLOAT);
INSERT INTO "t1" (id, price) VALUES (1, 1.50), (2, 2.50), (3, 3.50);
-- input:
SELECT id FROM "t1" WHERE price > 2.00;
-- expected output:
2
3
-- expected status: 0
