-- SEQUENCE basic nextval
-- setup:
CREATE SEQUENCE my_seq START WITH 1 INCREMENT BY 1;
-- input:
SELECT nextval('my_seq');
SELECT nextval('my_seq');
SELECT nextval('my_seq');
-- expected output:
1
2
3
-- expected status: 0
