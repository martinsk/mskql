-- UUID type basic insert and select
-- setup:
CREATE TABLE "t1" (id UUID, name TEXT);
INSERT INTO "t1" VALUES ('550e8400-e29b-41d4-a716-446655440000', 'alice');
INSERT INTO "t1" VALUES ('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'bob');
-- input:
SELECT * FROM "t1" ORDER BY id;
-- expected output:
550e8400-e29b-41d4-a716-446655440000|alice
6ba7b810-9dad-11d1-80b4-00c04fd430c8|bob
-- expected status: 0
