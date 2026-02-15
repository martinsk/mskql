-- tutorial: schema evolution - nested transactions (schema-evolution.html step 7)
-- setup:
CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE, active INT DEFAULT 1);
INSERT INTO users (name, email, active) VALUES ('Alice', 'alice@example.com', 1), ('Bob', 'bob@example.com', 1), ('Carol', 'carol@example.com', 0), ('Dave', 'dave@example.com', 1);
ALTER TABLE users ADD COLUMN created_at DATE;
UPDATE users SET created_at = '2025-01-01';
ALTER TABLE users RENAME COLUMN name TO display_name;
ALTER TABLE users ALTER COLUMN active TYPE BOOLEAN;
BEGIN;
ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'user';
UPDATE users SET role = 'admin' WHERE display_name = 'Alice';
COMMIT;
BEGIN;
UPDATE users SET role = 'moderator' WHERE display_name = 'Bob';
BEGIN;
UPDATE users SET role = 'moderator' WHERE display_name = 'Carol';
ROLLBACK;
COMMIT;
-- input:
SELECT display_name, role FROM users ORDER BY id;
-- expected output:
Alice|admin
Bob|moderator
Carol|user
Dave|user
-- expected status: 0
