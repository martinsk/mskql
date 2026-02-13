-- adversarial: CTE name shadows an existing table
-- BUG: per SQL standard, CTE should shadow the real table, but mskql
-- reads from the real table instead of the CTE.
-- setup:
CREATE TABLE items (id INT, name TEXT);
INSERT INTO items VALUES (1, 'real');
-- input:
WITH items AS (SELECT 99 AS id, 'fake' AS name) SELECT * FROM items;
-- expected output:
1|real
