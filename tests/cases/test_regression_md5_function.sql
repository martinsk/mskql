-- BUG: MD5() function not supported
-- input:
SELECT MD5('hello');
-- expected output:
5d41402abc4b2a76b9719d911017c592
-- expected status: 0
