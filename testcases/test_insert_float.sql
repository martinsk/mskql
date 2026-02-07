-- insert and select float values
-- setup:
CREATE TABLE "t1" (id INT, price FLOAT);
INSERT INTO "t1" (id, price) VALUES (1, 3.14);
-- input:
SELECT * FROM "t1";
-- expected output:
1|3.14
-- expected status: 0
