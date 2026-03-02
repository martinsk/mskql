-- Bug: NOT IN with a subquery causes parse error "expected FROM, got 'v'"
-- SELECT 5 NOT IN (SELECT v FROM t) fails to parse
-- In PostgreSQL this is valid syntax; the result should be NULL when the
-- subquery returns any NULL value and the test value is not found
-- setup:
CREATE TABLE t_nin_null (v INT);
INSERT INTO t_nin_null VALUES (1),(NULL),(3);
-- input:
SELECT 5 NOT IN (SELECT v FROM t_nin_null);
-- expected output:

-- expected status: 0
