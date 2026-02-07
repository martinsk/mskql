-- select where with text value
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
CREATE INDEX idx_name ON "t1" (name);
-- input:
SELECT * FROM "t1" WHERE name = 'bob';
-- expected output:
2|bob
-- expected status: 0
