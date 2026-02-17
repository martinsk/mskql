-- tutorial: sequences & enums - select all enum values (sequences-enums.html)
-- setup:
CREATE TYPE vibe AS ENUM ('happy', 'sad', 'neutral', 'excited');
CREATE TABLE people (id SERIAL PRIMARY KEY, name TEXT NOT NULL, vibe vibe NOT NULL);
INSERT INTO people (name, vibe) VALUES ('Alice', 'happy'), ('Bob', 'sad'), ('Charlie', 'neutral'), ('Diana', 'excited');
-- input:
SELECT name, vibe FROM people ORDER BY id;
-- expected output:
Alice|0
Bob|1
Charlie|2
Diana|3
-- expected status: 0
