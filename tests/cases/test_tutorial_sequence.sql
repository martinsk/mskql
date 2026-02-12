-- tutorial: sequence and NEXTVAL (index.html step 8)
-- setup:
CREATE SEQUENCE invoice_seq START WITH 1000;
-- input:
SELECT NEXTVAL('invoice_seq') AS inv;
-- expected output:
1000
-- expected status: 0
