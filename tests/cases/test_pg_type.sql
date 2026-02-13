-- pg_catalog.pg_type returns known types
-- input:
SELECT * FROM pg_catalog.pg_type WHERE typname = 'int4'
-- expected output:
23|int4|11|4
