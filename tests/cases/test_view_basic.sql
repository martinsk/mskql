-- VIEW basic select
-- setup:
CREATE TABLE "t1" (id INT, name TEXT, active BOOLEAN);
INSERT INTO "t1" VALUES (1, 'alice', true);
INSERT INTO "t1" VALUES (2, 'bob', false);
INSERT INTO "t1" VALUES (3, 'charlie', true);
CREATE VIEW active_users AS SELECT id, name FROM "t1" WHERE active = true;
-- input:
SELECT * FROM active_users;
-- expected output:
1|alice
3|charlie
-- expected status: 0
