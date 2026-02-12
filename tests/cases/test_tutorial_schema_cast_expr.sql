-- tutorial: schema evolution - CAST and string concat (schema-evolution.html)
-- setup:
CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE, active INT DEFAULT 1);
INSERT INTO users (name, email, active) VALUES ('Alice', 'alice@example.com', 1), ('Bob', 'bob@example.com', 1), ('Carol', 'carol@example.com', 0), ('Dave', 'dave@example.com', 1);
ALTER TABLE users RENAME COLUMN name TO display_name;
-- input:
SELECT display_name, active::INT AS active_int, CAST(id AS TEXT) || '-' || display_name AS code FROM users ORDER BY id;
-- expected output:
Alice|1|1-Alice
Bob|1|2-Bob
Carol|0|3-Carol
Dave|1|4-Dave
-- expected status: 0
