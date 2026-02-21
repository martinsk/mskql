-- BUG: ALTER SEQUENCE not supported (errors with 'expected TABLE after ALTER')
-- setup:
CREATE SEQUENCE s;
SELECT NEXTVAL('s');
SELECT NEXTVAL('s');
-- input:
ALTER SEQUENCE s RESTART WITH 1;
-- expected output:
ALTER SEQUENCE
-- expected status: 0
