-- BUG: Bitwise AND operator (&) not supported in SELECT expressions
-- input:
SELECT 5 & 3;
-- expected output:
1
-- expected status: 0
