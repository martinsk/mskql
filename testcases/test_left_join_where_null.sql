-- left join then filter for unmatched rows using IS NULL
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20), (3, 30);
CREATE TABLE "t2" (id INT, name TEXT);
INSERT INTO "t2" (id, name) VALUES (1, 'alice'), (3, 'charlie');
-- input:
SELECT t1.id, val FROM t1 LEFT JOIN t2 ON t1.id = t2.id WHERE name IS NULL;
-- expected output:
2|20
-- expected status: 0
