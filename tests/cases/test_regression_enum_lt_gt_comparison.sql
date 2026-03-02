-- Bug: ENUM less-than comparison returns false instead of true
-- In PostgreSQL, ENUM values compare by declaration order (ordinal position)
-- 'a' < 'b' should be true because 'a' is declared before 'b'
-- mskql returns false for all ENUM inequality comparisons (<, >, <>)
-- setup:
CREATE TYPE bug_ord AS ENUM ('a', 'b', 'c');
-- input:
SELECT 'a'::bug_ord < 'b'::bug_ord;
-- expected output:
t
-- expected status: 0
