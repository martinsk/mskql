-- COPY TO STDOUT empty table
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
-- input:
COPY t1 TO STDOUT
-- expected output:
-- expected status: 0
