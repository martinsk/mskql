-- BUG: Double-quoted identifiers (case-sensitive names) are not supported
-- input:
CREATE TABLE "MyTable" (id INT, "MyColumn" TEXT);
-- expected output:
CREATE TABLE
-- expected status: 0
