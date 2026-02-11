#ifndef COLUMN_H
#define COLUMN_H

#include "dynamic_array.h"

enum column_type {
    COLUMN_TYPE_INT,
    COLUMN_TYPE_FLOAT,
    COLUMN_TYPE_TEXT,
    COLUMN_TYPE_ENUM,
    COLUMN_TYPE_BOOLEAN,
    COLUMN_TYPE_BIGINT,
    COLUMN_TYPE_NUMERIC,
    COLUMN_TYPE_DATE,
    COLUMN_TYPE_TIME,
    COLUMN_TYPE_TIMESTAMP,
    COLUMN_TYPE_TIMESTAMPTZ,
    COLUMN_TYPE_INTERVAL,
    COLUMN_TYPE_UUID
};

/* returns 1 if the column type stores its value as a heap-allocated char* */
static inline int column_type_is_text(enum column_type t)
{
    return t == COLUMN_TYPE_TEXT || t == COLUMN_TYPE_ENUM ||
           t == COLUMN_TYPE_DATE || t == COLUMN_TYPE_TIME ||
           t == COLUMN_TYPE_TIMESTAMP || t == COLUMN_TYPE_TIMESTAMPTZ ||
           t == COLUMN_TYPE_INTERVAL || t == COLUMN_TYPE_UUID;
}

struct enum_type {
    // TODO: STRINGVIEW OPPORTUNITY: name is always strdup'd from an sv-originated string.
    // If this were an sv pointing into a persistent schema string, the strdup/free overhead
    // in enum_type_free and all callers could be eliminated.
    char *name;
    DYNAMIC_ARRAY(char *) values;
};

void enum_type_free(struct enum_type *et);
int  enum_type_valid(struct enum_type *et, const char *value);

struct cell; /* forward declaration for default_value */

struct column {
    // TODO: STRINGVIEW OPPORTUNITY: name and enum_type_name are char* requiring strdup/free
    // on every copy (table_add_column, do_single_join, rebuild_indexes, query_free, etc.).
    // These originate from parser tokens which are already sv. Storing them as sv (pointing
    // into the original SQL or a persistent schema buffer) would eliminate many allocations.
    char *name;
    enum column_type type;
    char *enum_type_name;
    int not_null;
    int has_default;
    struct cell *default_value;
    int is_unique;
    int is_primary_key;
    int is_serial;          /* SERIAL / BIGSERIAL auto-increment */
    long long serial_next;  /* next value for auto-increment (starts at 1) */
    /* REFERENCES (foreign key) */
    char *fk_table;         /* referenced table name, or NULL */
    char *fk_column;        /* referenced column name, or NULL */
    int fk_on_delete_cascade;
    int fk_on_update_cascade;
};

void column_free(struct column *col);

#endif
