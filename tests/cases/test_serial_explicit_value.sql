-- SERIAL with explicit value updates sequence
-- setup:
CREATE TABLE "t1" (id SERIAL, name TEXT);
INSERT INTO "t1" (id, name) VALUES (10, 'alice');
INSERT INTO "t1" (name) VALUES ('bob');
-- input:
SELECT * FROM "t1";
-- expected output:
10|alice
11|bob
-- expected status: 0
