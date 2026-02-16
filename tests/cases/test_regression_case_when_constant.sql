-- bug: CASE WHEN with constant comparison (no table) fails with parse error
-- setup:
-- input:
SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END;
-- expected output:
yes
