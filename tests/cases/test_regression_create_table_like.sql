-- REGRESSION: CREATE TABLE LIKE not supported
-- setup:
CREATE TABLE t_orig (id INT, name TEXT, val INT);
-- input:
CREATE TABLE t_copy LIKE t_orig;
-- expected output:
CREATE TABLE
-- expected status: 0
