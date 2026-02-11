-- SEQUENCE currval after nextval
-- setup:
CREATE SEQUENCE my_seq START WITH 10 INCREMENT BY 5;
-- input:
SELECT nextval('my_seq');
SELECT currval('my_seq');
SELECT nextval('my_seq');
SELECT currval('my_seq');
-- expected output:
10
10
15
15
-- expected status: 0
