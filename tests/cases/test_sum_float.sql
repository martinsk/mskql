-- sum aggregate on float column
-- setup:
CREATE TABLE "t1" (id INT, price FLOAT);
INSERT INTO "t1" (id, price) VALUES (1, 1.50), (2, 2.50), (3, 3.50);
-- input:
SELECT SUM(price) FROM "t1";
-- expected output:
7.5
-- expected status: 0
