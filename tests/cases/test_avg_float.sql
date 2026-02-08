-- avg on float column
-- setup:
CREATE TABLE "t1" (id INT, price FLOAT);
INSERT INTO "t1" (id, price) VALUES (1, 10.50), (2, 20.50);
-- input:
SELECT AVG(price) FROM "t1";
-- expected output:
15.5
-- expected status: 0
