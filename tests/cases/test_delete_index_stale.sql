-- delete with index leaves stale index entries
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
CREATE INDEX idx_name ON "t1" (name);
DELETE FROM "t1" WHERE name = 'bob';
-- input:
SELECT * FROM "t1" WHERE name = 'alice';
-- expected output:
1|alice
-- expected status: 0
