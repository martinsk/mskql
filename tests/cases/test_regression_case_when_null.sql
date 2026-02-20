-- BUG: CASE WHEN NULL THEN ... errors instead of treating NULL as falsy
-- input:
SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END;
-- expected output:
no
-- expected status: 0
