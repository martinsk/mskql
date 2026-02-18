-- parquet foreign table rejects INSERT (read-only)
-- setup:
CREATE FOREIGN TABLE large OPTIONS (FILENAME '@@FIXTURES@@/large.parquet');
-- input:
INSERT INTO large (id, category, amount) VALUES (9999, 'X', 1.0);
-- expected output:
ERROR:  cannot modify foreign table "large"
-- expected status: 2
