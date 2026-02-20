-- BUG: Casting empty string to INT should error, not return empty
-- input:
SELECT ''::INT;
-- expected output:
ERROR:  invalid input syntax for type integer: ""
-- expected status: 0
