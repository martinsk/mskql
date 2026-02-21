-- BUG: TO_CHAR for numbers returns the format string instead of formatted number
-- input:
SELECT TO_CHAR(1234.5, '9999.99');
-- expected output:
 1234.50
-- expected status: 0
