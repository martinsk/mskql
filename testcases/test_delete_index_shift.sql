-- delete shifts rows but index still has old row IDs
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
CREATE INDEX idx_name ON "t1" (name);
DELETE FROM "t1" WHERE name = 'alice';
-- input:
SELECT * FROM "t1" WHERE name = 'charlie';
-- expected output:
3|charlie
-- expected status: 0
