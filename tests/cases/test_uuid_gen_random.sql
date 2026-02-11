-- UUID gen_random_uuid() generates valid UUID
-- setup:
CREATE TABLE "t1" (id UUID, name TEXT);
INSERT INTO "t1" (id, name) VALUES (gen_random_uuid(), 'alice');
INSERT INTO "t1" (id, name) VALUES (gen_random_uuid(), 'bob');
-- input:
SELECT COUNT(*) FROM "t1";
-- expected output:
2
-- expected status: 0
