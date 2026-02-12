-- currval before nextval should return NULL (sequence not yet called)
-- setup:
CREATE SEQUENCE myseq START 1;
-- input:
SELECT currval('myseq');
-- expected output:

-- expected status: 0
