-- BUG: Dollar-quoted string literals ($$...$$) not supported
-- input:
SELECT $$hello world$$;
-- expected output:
hello world
-- expected status: 0
