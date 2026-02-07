-- insert and select enum values
-- setup:
CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');
CREATE TABLE "people" (id INT, name TEXT, mood mood);
INSERT INTO "people" (id, name, mood) VALUES (1, 'alice', 'happy'), (2, 'bob', 'sad'), (3, 'charlie', 'neutral');
-- input:
SELECT * FROM "people";
-- expected output:
1|alice|happy
2|bob|sad
3|charlie|neutral
-- expected status: 0
