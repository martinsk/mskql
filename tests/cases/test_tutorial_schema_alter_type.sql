-- tutorial: schema evolution - ALTER COLUMN TYPE (schema-evolution.html step 4)
-- setup:
CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE, active INT DEFAULT 1);
INSERT INTO users (name, email, active) VALUES ('Alice', 'alice@example.com', 1), ('Bob', 'bob@example.com', 1), ('Carol', 'carol@example.com', 0), ('Dave', 'dave@example.com', 1);
ALTER TABLE users ALTER COLUMN active TYPE BOOLEAN;
-- input:
SELECT id, name, active FROM users ORDER BY id;
-- expected output:
1|Alice|t
2|Bob|t
3|Carol|f
4|Dave|t
-- expected status: 0
