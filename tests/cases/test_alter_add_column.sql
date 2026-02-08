-- alter table add column then select
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob');
ALTER TABLE "t1" ADD COLUMN score INT;
INSERT INTO "t1" (id, name, score) VALUES (3, 'charlie', 95);
-- input:
SELECT id, name, score FROM "t1" ORDER BY id;
-- expected output:
1|alice|
2|bob|
3|charlie|95
-- expected status: 0
