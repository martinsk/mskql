-- index lookup with int literal on float column
-- setup:
CREATE TABLE "t1" (id INT, price FLOAT);
INSERT INTO "t1" (id, price) VALUES (1, 10.00), (2, 20.00), (3, 30.00);
CREATE INDEX idx_price ON "t1" (price);
-- input:
SELECT id FROM "t1" WHERE price = 20;
-- expected output:
2
-- expected status: 0
