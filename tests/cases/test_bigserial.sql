-- BIGSERIAL auto-increment
-- setup:
CREATE TABLE "t1" (id BIGSERIAL, name TEXT);
INSERT INTO "t1" (name) VALUES ('alice');
INSERT INTO "t1" (name) VALUES ('bob');
-- input:
SELECT * FROM "t1";
-- expected output:
1|alice
2|bob
-- expected status: 0
