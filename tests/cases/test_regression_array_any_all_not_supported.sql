-- Bug: ANY(ARRAY[...]) and ALL(ARRAY[...]) operators are not supported
-- In PostgreSQL, = ANY(ARRAY[1,2,3]) checks if the value equals any element
-- mskql raises "expected FROM, got 'ANY'" parse error
-- setup:
-- input:
SELECT 2 = ANY(ARRAY[1,2,3]);
-- expected output:
t
-- expected status: 0
