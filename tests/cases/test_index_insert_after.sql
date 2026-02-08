-- index updated on insert after creation
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
CREATE INDEX idx_id ON "t1" (id);
INSERT INTO "t1" (id, name) VALUES (1, 'first'), (2, 'second'), (3, 'third');
-- input:
SELECT * FROM "t1" WHERE id = 2;
-- expected output:
2|second
-- expected status: 0
