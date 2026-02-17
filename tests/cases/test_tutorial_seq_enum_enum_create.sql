-- tutorial: sequences & enums - CREATE TYPE AS ENUM (sequences-enums.html)
-- setup:
CREATE TYPE feeling AS ENUM ('happy', 'sad', 'neutral', 'excited');
CREATE TABLE people (id SERIAL PRIMARY KEY, name TEXT NOT NULL, feeling feeling NOT NULL);
INSERT INTO people (name, feeling) VALUES ('Alice', 'happy'), ('Bob', 'sad'), ('Charlie', 'neutral'), ('Diana', 'excited');
-- input:
SELECT name, feeling FROM people WHERE feeling = 'happy' ORDER BY name;
-- expected output:
Alice|0
-- expected status: 0
