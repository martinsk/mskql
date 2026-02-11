-- SERIAL with multi-row insert
-- setup:
CREATE TABLE "t1" (id SERIAL, name TEXT);
-- input:
INSERT INTO "t1" (name) VALUES ('alice'), ('bob'), ('charlie');
SELECT * FROM "t1";
-- expected output:
INSERT 0 3
1|alice
2|bob
3|charlie
-- expected status: 0
