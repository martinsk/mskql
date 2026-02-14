#ifndef DATABASE_H
#define DATABASE_H

#include "dynamic_array.h"
#include "table.h"
#include "query.h"

struct sequence {
    char *name;
    long long current_value;
    long long increment;
    long long min_value;
    long long max_value;
    int has_been_called; /* 0 until first nextval() */
};

struct db_snapshot {
    /* Lazy COW: on BEGIN we just record table names + generations.
     * On first write to a table, we deep-copy it into saved_tables. */
    size_t orig_table_count;     /* number of tables at BEGIN time */
    char **table_names;          /* [orig_table_count] strdup'd names */
    uint64_t *table_generations; /* [orig_table_count] generation at BEGIN */
    struct table *saved_tables;  /* [orig_table_count] deep-copied on COW */
    int *saved_valid;            /* [orig_table_count] 1 if saved_tables[i] populated */
    /* Types/sequences: small, just copy eagerly */
    DYNAMIC_ARRAY(struct enum_type) types;
    /* Nested transactions: pointer to parent snapshot */
    struct db_snapshot *parent;
};

/* Per-connection transaction state.  Owned by client_state in pgwire.c;
 * the database holds a pointer to the currently-active one. */
struct txn_state {
    int in_transaction;
    struct db_snapshot *snapshot;
};

struct database {
    // TODO: STRINGVIEW OPPORTUNITY: name is strdup'd from a string literal in main.c;
    // could be sv if the caller guaranteed lifetime.
    char *name;
    DYNAMIC_ARRAY(struct table) tables;
    DYNAMIC_ARRAY(struct enum_type) types;
    DYNAMIC_ARRAY(struct sequence) sequences;
    /* Points to the txn_state of the client whose query is currently executing.
     * Set by pgwire before each query, cleared after.  NULL when idle.
     * Safe because the server is single-threaded. */
    struct txn_state *active_txn;
};

void db_init(struct database *db, const char *name);
struct enum_type *db_find_type(struct database *db, const char *name);
int  db_create_table(struct database *db, const char *name, struct column *cols);
struct table *db_find_table(struct database *db, const char *name);
struct table *db_find_table_sv(struct database *db, sv name);
int  db_table_exec_query(struct database *db, sv table_name,
                         struct query *q, struct rows *result, struct bump_alloc *rb);
int  db_exec(struct database *db, struct query *q, struct rows *result, struct bump_alloc *rb);
int  db_exec_sql(struct database *db, const char *sql, struct rows *result);
void db_free(struct database *db);
void db_reset(struct database *db);

/* Materialize a subquery into a temporary table added to db->tables.
 * Returns a pointer to the new table, or NULL on failure.
 * The caller is responsible for calling remove_temp_table when done. */
struct table *materialize_subquery(struct database *db, const char *sql,
                                   const char *table_name);

/* Remove a temporary table from the database by pointer */
void remove_temp_table(struct database *db, struct table *t);

/* Snapshot management (used by transaction BEGIN/COMMIT/ROLLBACK) */
struct db_snapshot *snapshot_create(struct database *db);
void snapshot_free(struct db_snapshot *snap);

/* COW trigger: call before mutating a table inside a transaction.
 * Saves a deep-copy of the table if not already saved. */
void snapshot_cow_table(struct db_snapshot *snap, struct database *db, const char *table_name);

#endif
