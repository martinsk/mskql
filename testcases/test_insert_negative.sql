-- insert and select negative numbers
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, -100), (2, -50), (3, 0);
-- input:
SELECT * FROM "t1" ORDER BY val;
-- expected output:
1|-100
2|-50
3|0
-- expected status: 0
