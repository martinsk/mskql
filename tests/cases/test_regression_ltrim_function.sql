-- BUG: LTRIM function not supported
-- input:
SELECT LTRIM('  hello');
-- expected output:
hello
-- expected status: 0
