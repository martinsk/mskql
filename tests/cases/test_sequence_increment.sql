-- sequence with custom increment
-- setup:
CREATE SEQUENCE myseq START 10 INCREMENT 5;
-- input:
SELECT nextval('myseq');
SELECT nextval('myseq');
SELECT nextval('myseq');
-- expected output:
10
15
20
-- expected status: 0
