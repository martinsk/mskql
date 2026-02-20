-- BUG: RTRIM function not supported
-- input:
SELECT RTRIM('hello  ');
-- expected output:
hello
-- expected status: 0
