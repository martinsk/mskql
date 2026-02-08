-- is distinct from null-safe comparison
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, NULL), (3, 10);
-- input:
SELECT id FROM "t1" WHERE val IS DISTINCT FROM 10 ORDER BY id;
-- expected output:
2
-- expected status: 0
