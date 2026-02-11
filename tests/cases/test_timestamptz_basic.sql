-- TIMESTAMPTZ type basic insert and select
-- setup:
CREATE TABLE "t1" (id INT, ts TIMESTAMPTZ);
INSERT INTO "t1" VALUES (1, '2024-01-15 08:30:00+00');
INSERT INTO "t1" VALUES (2, '2024-06-30 14:00:00+00');
-- input:
SELECT * FROM "t1" ORDER BY ts;
-- expected output:
1|2024-01-15 08:30:00+00
2|2024-06-30 14:00:00+00
-- expected status: 0
