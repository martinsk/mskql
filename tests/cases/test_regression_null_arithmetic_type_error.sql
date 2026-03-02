-- Bug: 1 + NULL raises "operator does not exist: integer + text" instead of returning NULL
-- In PostgreSQL, any arithmetic with NULL returns NULL (NULL propagation)
-- mskql incorrectly treats NULL as text and raises a type error
-- setup:
-- input:
SELECT 1 + NULL;
-- expected output:

-- expected status: 0
