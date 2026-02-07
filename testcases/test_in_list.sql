-- in list filter
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie'), (4, 'diana');
-- input:
SELECT id, name FROM "t1" WHERE id IN (1, 3, 4) ORDER BY id;
-- expected output:
1|alice
3|charlie
4|diana
-- expected status: 0
