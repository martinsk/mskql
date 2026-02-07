-- update indexed column leaves stale index
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE INDEX idx_name ON "t1" (name);
UPDATE "t1" SET name = 'zara' WHERE id = 1;
-- input:
SELECT * FROM "t1" WHERE name = 'zara';
-- expected output:
1|zara
-- expected status: 0
