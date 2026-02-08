-- select enum with where clause
-- setup:
CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');
CREATE TABLE "people" (id INT, name TEXT, mood mood);
INSERT INTO "people" (id, name, mood) VALUES (1, 'alice', 'happy'), (2, 'bob', 'sad'), (3, 'charlie', 'neutral');
-- input:
SELECT * FROM "people" WHERE mood = 'sad';
-- expected output:
2|bob|sad
-- expected status: 0
