-- nested function calls: COALESCE(UPPER(x), 'default')
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice');
INSERT INTO "t1" (id) VALUES (2);
-- input:
SELECT id, COALESCE(UPPER(name), 'DEFAULT') FROM "t1" ORDER BY id;
-- expected output:
1|ALICE
2|DEFAULT
-- expected status: 0
