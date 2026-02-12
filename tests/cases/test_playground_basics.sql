-- playground example: basics (SELECT with WHERE and ORDER BY)
-- setup:
CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT, age INT);
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 32), (2, 'Bob', 'bob@example.com', 28), (3, 'Charlie', 'charlie@example.com', 35), (4, 'Diana', 'diana@example.com', 29);
-- input:
SELECT * FROM users WHERE age > 30 ORDER BY name;
-- expected output:
1|Alice|alice@example.com|32
3|Charlie|charlie@example.com|35
-- expected status: 0
