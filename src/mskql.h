/*
 * mskql.h — public embeddable C API for mskql
 *
 * Link against libmskql.a (built via: make lib) and -lm -lzstd -lz.
 *
 * Usage:
 *   mskql_db *db = mskql_open("mydb");
 *   mskql_exec(db, "CREATE TABLE t (id INT, name TEXT)");
 *   mskql_exec(db, "INSERT INTO t VALUES (1, 'hello')");
 *
 *   mskql_result *res = NULL;
 *   mskql_query(db, "SELECT * FROM t", &res);
 *   for (int r = 0; r < mskql_result_nrows(res); r++)
 *       printf("%s %s\n", mskql_result_value(res, r, 0),
 *                          mskql_result_value(res, r, 1));
 *   mskql_result_free(res);
 *   mskql_close(db);
 */
#ifndef MSKQL_H
#define MSKQL_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct mskql_db     mskql_db;
typedef struct mskql_result mskql_result;

/* Open an in-memory database.  name is used for internal identification only.
 * Returns NULL on allocation failure. */
mskql_db *mskql_open(const char *name);

/* Execute a DDL/DML statement (CREATE, INSERT, UPDATE, DELETE, etc.).
 * Returns 0 on success, negative on error. */
int mskql_exec(mskql_db *db, const char *sql);

/* Execute a query and retrieve results.
 * On success, *out is set to a result handle (caller must free with
 * mskql_result_free).  Returns 0 on success, negative on error.
 * On error, *out is set to NULL. */
int mskql_query(mskql_db *db, const char *sql, mskql_result **out);

/* Execute a statement, discarding any result rows.
 * Faster than mskql_query when you don't need the output.
 * Returns 0 on success, negative on error. */
int mskql_exec_discard(mskql_db *db, const char *sql);

/* Number of rows in the result. */
int mskql_result_nrows(const mskql_result *r);

/* Number of columns in the result. */
int mskql_result_ncols(const mskql_result *r);

/* Get the value at (row, col) as a NUL-terminated string.
 * Returns NULL for SQL NULL values.
 * The returned pointer is valid until mskql_result_free is called. */
const char *mskql_result_value(const mskql_result *r, int row, int col);

/* Free a result handle. NULL-safe. */
void mskql_result_free(mskql_result *r);

/* Close a database and free all resources. NULL-safe. */
void mskql_close(mskql_db *db);

/* Reset a database to its initial state (drops all tables, types, sequences).
 * The database handle remains valid for further use. */
void mskql_reset(mskql_db *db);

#ifdef __cplusplus
}
#endif

#endif /* MSKQL_H */
