-- select specific columns with where and order by
-- setup:
CREATE TABLE "t1" (id INT, name TEXT, score INT);
INSERT INTO "t1" (id, name, score) VALUES (1, 'alice', 85), (2, 'bob', 92), (3, 'charlie', 78), (4, 'diana', 95);
-- input:
SELECT name, score FROM "t1" WHERE score >= 85 ORDER BY score DESC;
-- expected output:
diana|95
bob|92
alice|85
-- expected status: 0
