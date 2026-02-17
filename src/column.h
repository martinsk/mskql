#ifndef COLUMN_H
#define COLUMN_H

#include <stdint.h>
#include "dynamic_array.h"

enum column_type {
    COLUMN_TYPE_SMALLINT,
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
    return t == COLUMN_TYPE_TEXT;
}

/* returns 1 if the column type is a temporal type stored as integer/struct */
static inline int column_type_is_temporal(enum column_type t)
{
    switch (t) {
    case COLUMN_TYPE_DATE:
    case COLUMN_TYPE_TIME:
    case COLUMN_TYPE_TIMESTAMP:
    case COLUMN_TYPE_TIMESTAMPTZ:
    case COLUMN_TYPE_INTERVAL:
        return 1;
    case COLUMN_TYPE_SMALLINT:
    case COLUMN_TYPE_INT:
    case COLUMN_TYPE_FLOAT:
    case COLUMN_TYPE_TEXT:
    case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_BOOLEAN:
    case COLUMN_TYPE_BIGINT:
    case COLUMN_TYPE_NUMERIC:
    case COLUMN_TYPE_UUID:
        return 0;
    }
    __builtin_unreachable();
}

/* Unified PostgreSQL type metadata table — single source of truth for
 * OID, typname, display name, and storage length across catalog.c and pgwire.c. */
struct pg_type_info {
    uint32_t    oid;
    const char *typname;    /* pg_type.typname (e.g. "int4") */
    const char *pg_name;    /* information_schema display name (e.g. "integer") */
    int16_t     typlen;     /* pg_type.typlen (-1 = variable) */
};

static const struct pg_type_info pg_type_table[] = {
    /* [COLUMN_TYPE_SMALLINT]    */ { 21,   "int2",        "smallint",                    2 },
    /* [COLUMN_TYPE_INT]         */ { 23,   "int4",        "integer",                     4 },
    /* [COLUMN_TYPE_FLOAT]       */ { 701,  "float8",      "double precision",            8 },
    /* [COLUMN_TYPE_TEXT]        */ { 25,   "text",        "text",                       -1 },
    /* [COLUMN_TYPE_ENUM]        */ { 25,   "text",        "USER-DEFINED",                4 },
    /* [COLUMN_TYPE_BOOLEAN]     */ { 16,   "bool",        "boolean",                     1 },
    /* [COLUMN_TYPE_BIGINT]      */ { 20,   "int8",        "bigint",                      8 },
    /* [COLUMN_TYPE_NUMERIC]     */ { 1700, "numeric",     "numeric",                    -1 },
    /* [COLUMN_TYPE_DATE]        */ { 1082, "date",        "date",                        4 },
    /* [COLUMN_TYPE_TIME]        */ { 1083, "time",        "time without time zone",      8 },
    /* [COLUMN_TYPE_TIMESTAMP]   */ { 1114, "timestamp",   "timestamp without time zone", 8 },
    /* [COLUMN_TYPE_TIMESTAMPTZ] */ { 1184, "timestamptz", "timestamp with time zone",    8 },
    /* [COLUMN_TYPE_INTERVAL]    */ { 1186, "interval",    "interval",                   16 },
    /* [COLUMN_TYPE_UUID]        */ { 2950, "uuid",        "uuid",                       16 },
};

static inline const struct pg_type_info *pg_type_lookup(enum column_type t)
{
    return &pg_type_table[(int)t];
}

static inline const char *column_type_name(enum column_type t)
{
    return pg_type_table[(int)t].pg_name;
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
int  enum_ordinal(struct enum_type *et, const char *value);  /* -1 if not found */
const char *enum_label(struct enum_type *et, int32_t ordinal); /* NULL if out of range */

struct cell; /* forward declaration for default_value */

enum fk_action {
    FK_NO_ACTION,    /* default — error on violation (same as RESTRICT) */
    FK_RESTRICT,     /* error on violation */
    FK_CASCADE,      /* propagate delete/update to referencing rows */
    FK_SET_NULL,     /* set referencing column to NULL */
    FK_SET_DEFAULT   /* set referencing column to its DEFAULT value */
};

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
    enum fk_action fk_on_delete;
    enum fk_action fk_on_update;
    /* CHECK constraint */
    char *check_expr_sql;       /* raw SQL text of CHECK body, or NULL */
};

void column_free(struct column *col);

#endif
