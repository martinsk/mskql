-- BUG: CURRENT_TIME returns NULL instead of current time
-- input:
SELECT CURRENT_TIME IS NOT NULL;
-- expected output:
t
-- expected status: 0
