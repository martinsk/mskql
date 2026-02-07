-- multiple aggregates in one query
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT SUM(val), COUNT(*), AVG(val) FROM "t1";
-- expected output:
60|3|20
-- expected status: 0
