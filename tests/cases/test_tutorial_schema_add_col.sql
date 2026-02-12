-- tutorial: schema evolution - ADD COLUMN (schema-evolution.html)
-- setup:
CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE, active INT DEFAULT 1);
INSERT INTO users (name, email, active) VALUES ('Alice', 'alice@example.com', 1), ('Bob', 'bob@example.com', 1), ('Carol', 'carol@example.com', 0), ('Dave', 'dave@example.com', 1);
ALTER TABLE users ADD COLUMN created_at DATE;
-- input:
SELECT id, name, created_at FROM users ORDER BY id;
-- expected output:
1|Alice|
2|Bob|
3|Carol|
4|Dave|
-- expected status: 0
