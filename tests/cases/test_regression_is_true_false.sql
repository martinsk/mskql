-- BUG: IS TRUE / IS FALSE / IS UNKNOWN not supported in SELECT expressions
-- input:
SELECT TRUE IS TRUE;
-- expected output:
t
-- expected status: 0
