-- Bug: SUBSTR with start position 0 returns the full string instead of treating 0 as 1
-- In PostgreSQL, SUBSTR('hello', 0, 3) returns 'he' (positions 1 and 2, since 0 -> 1)
-- mskql returns 'hel' (3 characters starting from position 1)
-- setup:
-- input:
SELECT SUBSTR('hello', 0, 3);
-- expected output:
he
-- expected status: 0
