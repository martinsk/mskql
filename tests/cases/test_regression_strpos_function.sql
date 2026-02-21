-- BUG: STRPOS() function not supported
-- input:
SELECT STRPOS('hello world', 'world');
-- expected output:
7
-- expected status: 0
