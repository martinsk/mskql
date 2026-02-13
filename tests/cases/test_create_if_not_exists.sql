-- CREATE TABLE IF NOT EXISTS basic
-- setup:
CREATE TABLE users (id INT, name TEXT)
INSERT INTO users VALUES (1, 'alice')
CREATE TABLE IF NOT EXISTS users (id INT, score FLOAT)
-- input:
SELECT * FROM users
-- expected output:
1|alice
