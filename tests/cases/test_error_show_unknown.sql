-- SHOW with unrecognized parameter should error
-- input:
SHOW nonexistent_param;
-- expected output:
ERROR:  unrecognized configuration parameter "nonexistent_param"
-- expected status: 1
