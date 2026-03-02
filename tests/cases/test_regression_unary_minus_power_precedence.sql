-- Bug: unary minus has wrong precedence relative to ^ (power operator)
-- In PostgreSQL, ^ binds tighter than unary minus, so -2^2 = -(2^2) = -4
-- mskql returns 4, treating it as (-2)^2
-- setup:
-- input:
SELECT -2^2;
-- expected output:
-4
-- expected status: 0
