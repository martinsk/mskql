-- SERIAL auto-increment basic
-- setup:
CREATE TABLE "t1" (id SERIAL, name TEXT);
INSERT INTO "t1" (name) VALUES ('alice');
INSERT INTO "t1" (name) VALUES ('bob');
INSERT INTO "t1" (name) VALUES ('charlie');
-- input:
SELECT * FROM "t1";
-- expected output:
1|alice
2|bob
3|charlie
-- expected status: 0
