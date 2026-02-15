-- pg_catalog.pg_class lists tables
-- setup:
CREATE TABLE items (id INT, val TEXT)
-- input:
SELECT * FROM pg_catalog.pg_class WHERE relname = 'items'
-- expected output:
16384|items|2200|r|10|0|f|2|2|f|f|f||f|0|0|0|f|f|p|d|0
