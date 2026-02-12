-- tutorial: schema evolution - transaction with ALTER and UPDATE (schema-evolution.html)
-- setup:
CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE, active INT DEFAULT 1);
INSERT INTO users (name, email, active) VALUES ('Alice', 'alice@example.com', 1), ('Bob', 'bob@example.com', 1), ('Carol', 'carol@example.com', 0), ('Dave', 'dave@example.com', 1);
ALTER TABLE users RENAME COLUMN name TO display_name;
ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'user';
UPDATE users SET role = 'admin' WHERE display_name = 'Alice';
-- input:
SELECT display_name, role FROM users ORDER BY id;
-- expected output:
Alice|admin
Bob|
Carol|
Dave|
-- expected status: 0
