-- BUG: Casting invalid text to INT should error, not return empty
-- input:
SELECT 'abc'::INT;
-- expected output:
ERROR:  invalid input syntax for type integer: "abc"
-- expected status: 0
