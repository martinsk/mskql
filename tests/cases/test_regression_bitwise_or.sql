-- BUG: Bitwise OR operator (|) not supported in SELECT expressions
-- input:
SELECT 5 | 3;
-- expected output:
7
-- expected status: 0
