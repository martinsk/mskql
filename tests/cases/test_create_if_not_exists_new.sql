-- CREATE TABLE IF NOT EXISTS creates when table does not exist
-- setup:
CREATE TABLE IF NOT EXISTS items (id INT, val TEXT)
INSERT INTO items VALUES (1, 'hello')
-- input:
SELECT * FROM items
-- expected output:
1|hello
