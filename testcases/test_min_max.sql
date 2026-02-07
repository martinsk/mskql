-- min and max aggregates
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 50), (3, 30);
-- input:
SELECT MIN(val), MAX(val) FROM "t1";
-- expected output:
10|50
-- expected status: 0
