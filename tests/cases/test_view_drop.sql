-- VIEW create and drop
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" VALUES (1, 'alice');
CREATE VIEW v1 AS SELECT * FROM "t1";
-- input:
SELECT * FROM v1;
DROP VIEW v1;
-- expected output:
1|alice
DROP VIEW
-- expected status: 0
