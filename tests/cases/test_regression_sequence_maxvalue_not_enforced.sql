-- Bug: sequence MAXVALUE is not enforced; NEXTVAL continues past the maximum
-- In PostgreSQL, calling NEXTVAL after reaching MAXVALUE raises an error:
-- "nextval: reached maximum value of sequence"
-- mskql silently returns values beyond MAXVALUE
-- setup:
CREATE SEQUENCE seq_max_bug START 1 INCREMENT 1 MAXVALUE 2;
SELECT NEXTVAL('seq_max_bug');
SELECT NEXTVAL('seq_max_bug');
-- input:
SELECT NEXTVAL('seq_max_bug');
-- expected output:
ERROR:  nextval: reached maximum value of sequence "seq_max_bug" (2)
-- expected status: 1
