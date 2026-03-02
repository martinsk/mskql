-- Bug: ENUM value comparisons always return false regardless of ordinal order
-- In PostgreSQL, ENUM values are ordered by their declaration order
-- 'done' > 'pending' should be true (done is declared after pending)
-- mskql returns false for all ENUM comparisons
-- setup:
CREATE TYPE bug_status AS ENUM ('pending', 'active', 'done');
-- input:
SELECT 'done'::bug_status > 'pending'::bug_status;
-- expected output:
t
-- expected status: 0
