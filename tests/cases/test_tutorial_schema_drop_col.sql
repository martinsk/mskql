-- tutorial: schema evolution - DROP COLUMN (schema-evolution.html)
-- setup:
CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE, active INT DEFAULT 1);
INSERT INTO users (name, email, active) VALUES ('Alice', 'alice@example.com', 1), ('Bob', 'bob@example.com', 1), ('Carol', 'carol@example.com', 0), ('Dave', 'dave@example.com', 1);
ALTER TABLE users RENAME COLUMN name TO display_name;
ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'user';
UPDATE users SET role = 'admin' WHERE display_name = 'Alice';
ALTER TABLE users DROP COLUMN active;
-- input:
SELECT id, display_name, email, role FROM users ORDER BY id;
-- expected output:
1|Alice|alice@example.com|admin
2|Bob|bob@example.com|
3|Carol|carol@example.com|
4|Dave|dave@example.com|
-- expected status: 0
