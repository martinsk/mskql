-- BUG: SUBSTR(string, start, length) function-call syntax not supported
-- input:
SELECT SUBSTR('hello world', 7, 5);
-- expected output:
world
-- expected status: 0
