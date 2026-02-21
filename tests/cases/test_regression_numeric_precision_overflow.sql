-- BUG: NUMERIC(p,s) does not error on overflow (value exceeds precision)
-- In PostgreSQL: 1000.00::NUMERIC(5,2) errors because 1000 requires 6 digits total (3 before decimal + 2 after)
-- input:
SELECT 1000.00::NUMERIC(5,2);
-- expected output:
ERROR:  numeric field overflow
-- expected status: 0
