-- not in filter
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie'), (4, 'diana');
-- input:
SELECT id, name FROM "t1" WHERE id NOT IN (2, 4) ORDER BY id;
-- expected output:
1|alice
3|charlie
-- expected status: 0
