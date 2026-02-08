-- count aggregate
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT COUNT(*) FROM "t1";
-- expected output:
3
-- expected status: 0
