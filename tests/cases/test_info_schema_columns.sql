-- information_schema.columns lists columns for a table
-- setup:
CREATE TABLE users (id INT NOT NULL, name TEXT, active BOOLEAN)
-- input:
SELECT * FROM information_schema.columns WHERE table_name = 'users'
-- expected output:
mskql|public|users|id|1||NO|integer
mskql|public|users|name|2||YES|text
mskql|public|users|active|3||YES|boolean
