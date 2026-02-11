-- TIMESTAMP type basic insert and select
-- setup:
CREATE TABLE "t1" (id INT, ts TIMESTAMP);
INSERT INTO "t1" VALUES (1, '2024-01-15 08:30:00');
INSERT INTO "t1" VALUES (2, '2024-06-30 14:00:00');
INSERT INTO "t1" VALUES (3, '2023-12-01 23:59:59');
-- input:
SELECT * FROM "t1" ORDER BY ts;
-- expected output:
3|2023-12-01 23:59:59
1|2024-01-15 08:30:00
2|2024-06-30 14:00:00
-- expected status: 0
