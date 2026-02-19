-- bug: 'on' and 'y' cast to BOOLEAN should return true (PostgreSQL compatibility)
-- setup:
-- input:
SELECT 'on'::BOOLEAN;
SELECT 'y'::BOOLEAN;
-- expected output:
t
t
-- expected status: 0
