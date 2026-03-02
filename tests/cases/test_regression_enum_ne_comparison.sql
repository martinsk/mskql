-- Bug: ENUM not-equal comparison (<>) returns false instead of true
-- In PostgreSQL, 'a'::enum <> 'b'::enum is true (different values)
-- mskql returns false for ENUM <> comparisons
-- setup:
CREATE TYPE bug_ord2 AS ENUM ('a', 'b', 'c');
-- input:
SELECT 'a'::bug_ord2 <> 'b'::bug_ord2;
-- expected output:
t
-- expected status: 0
