-- insert with fewer values than columns
-- setup:
CREATE TABLE "t1" (id INT, name TEXT, val INT);
INSERT INTO "t1" (id, name, val) VALUES (1, 'alice');
-- input:
SELECT * FROM "t1";
-- expected output:
1|alice|
-- expected status: 0
