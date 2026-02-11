-- SEQUENCE used in INSERT
-- setup:
CREATE SEQUENCE id_seq START WITH 100;
CREATE TABLE "t1" (id BIGINT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (nextval('id_seq'), 'alice');
INSERT INTO "t1" (id, name) VALUES (nextval('id_seq'), 'bob');
-- input:
SELECT * FROM "t1";
-- expected output:
100|alice
101|bob
-- expected status: 0
