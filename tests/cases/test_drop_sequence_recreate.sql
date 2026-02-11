-- DROP SEQUENCE then re-create should reset counter
-- setup:
CREATE SEQUENCE myseq START 1;
SELECT nextval('myseq');
SELECT nextval('myseq');
DROP SEQUENCE myseq;
CREATE SEQUENCE myseq START 1;
-- input:
SELECT nextval('myseq');
-- expected output:
1
-- expected status: 0
