-- DECIMAL is alias for NUMERIC
-- setup:
CREATE TABLE "t1" (id INT, amount DECIMAL);
INSERT INTO "t1" VALUES (1, 42.5);
INSERT INTO "t1" VALUES (2, 100.25);
-- input:
SELECT * FROM "t1" WHERE amount > 50;
-- expected output:
2|100.25
-- expected status: 0
