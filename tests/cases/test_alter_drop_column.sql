-- alter table drop column
-- setup:
CREATE TABLE "t1" (id INT, name TEXT, val INT);
INSERT INTO "t1" (id, name, val) VALUES (1, 'alice', 10), (2, 'bob', 20);
ALTER TABLE "t1" DROP COLUMN name;
-- input:
SELECT id, val FROM "t1" ORDER BY id;
-- expected output:
1|10
2|20
-- expected status: 0
