-- information_schema.tables lists created tables
-- setup:
CREATE TABLE users (id INT, name TEXT)
CREATE TABLE orders (id INT, user_id INT)
-- input:
SELECT * FROM information_schema.tables
-- expected output:
mskql|public|users|BASE TABLE
mskql|public|orders|BASE TABLE
