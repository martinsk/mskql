-- DATE type basic insert and select
-- setup:
CREATE TABLE "t1" (id INT, d DATE);
INSERT INTO "t1" VALUES (1, '2024-01-15');
INSERT INTO "t1" VALUES (2, '2024-06-30');
INSERT INTO "t1" VALUES (3, '2023-12-01');
-- input:
SELECT * FROM "t1" ORDER BY d;
-- expected output:
3|2023-12-01
1|2024-01-15
2|2024-06-30
-- expected status: 0
