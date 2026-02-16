-- bug: TRIM(LEADING/TRAILING/BOTH ... FROM ...) syntax fails with parse error
-- setup:
-- input:
SELECT TRIM(LEADING 'x' FROM 'xxxhello');
SELECT TRIM(TRAILING 'x' FROM 'helloxx');
SELECT TRIM(BOTH 'x' FROM 'xxhelloxx');
-- expected output:
hello
hello
hello
