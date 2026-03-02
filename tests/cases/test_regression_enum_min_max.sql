-- Bug: SELECT MIN(p), MAX(p) on an ENUM column returns wrong value for MAX
-- In PostgreSQL, MIN/MAX on ENUM uses declaration order; both should return the label string
-- mskql returns the correct MIN label but MAX returns an ordinal integer (e.g. "2") instead
-- of the label string "high"
-- setup:
CREATE TYPE bug_priority AS ENUM ('low', 'medium', 'high');
CREATE TABLE t_enum_agg (p bug_priority);
INSERT INTO t_enum_agg VALUES ('high'), ('low'), ('medium');
-- input:
SELECT MIN(p), MAX(p) FROM t_enum_agg;
-- expected output:
low|high
-- expected status: 0
