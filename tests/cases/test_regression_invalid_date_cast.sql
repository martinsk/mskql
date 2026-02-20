-- BUG: Casting invalid date string should error, not produce garbage
-- input:
SELECT 'not-a-date'::DATE;
-- expected output:
ERROR:  invalid input syntax for type date: "not-a-date"
-- expected status: 0
