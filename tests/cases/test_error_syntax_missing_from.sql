-- Missing FROM keyword should produce specific parse error
-- input:
SELECT * t1;
-- expected output:
ERROR:  expected FROM, got 't1'
-- expected status: 1
