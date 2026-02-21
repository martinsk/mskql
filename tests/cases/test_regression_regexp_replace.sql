-- BUG: REGEXP_REPLACE() function not supported
-- input:
SELECT REGEXP_REPLACE('hello world', 'world', 'there');
-- expected output:
hello there
-- expected status: 0
