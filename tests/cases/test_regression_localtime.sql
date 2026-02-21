-- BUG: LOCALTIME returns NULL instead of current local time
-- input:
SELECT LOCALTIME IS NOT NULL;
-- expected output:
t
-- expected status: 0
