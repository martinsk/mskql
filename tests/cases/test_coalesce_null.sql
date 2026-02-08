-- coalesce with null column falls through to default
-- setup:
CREATE TABLE "t1" (id INT, name TEXT, nickname TEXT);
INSERT INTO "t1" (id, name, nickname) VALUES (1, 'alice', 'ali'), (2, 'bob', NULL), (3, NULL, NULL);
-- input:
SELECT id, COALESCE(nickname, name, 'unknown') FROM "t1" ORDER BY id;
-- expected output:
1|ali
2|bob
3|unknown
-- expected status: 0
