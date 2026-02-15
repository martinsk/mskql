#include "catalog.h"
#include "table.h"
#include "column.h"
#include "row.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

/* ---- helpers ---- */

static uint32_t col_type_to_oid(enum column_type t)
{
    switch (t) {
        case COLUMN_TYPE_SMALLINT:   return 21;
        case COLUMN_TYPE_INT:        return 23;
        case COLUMN_TYPE_FLOAT:      return 701;
        case COLUMN_TYPE_TEXT:       return 25;
        case COLUMN_TYPE_ENUM:       return 25;
        case COLUMN_TYPE_BOOLEAN:    return 16;
        case COLUMN_TYPE_BIGINT:     return 20;
        case COLUMN_TYPE_NUMERIC:    return 1700;
        case COLUMN_TYPE_DATE:       return 1082;
        case COLUMN_TYPE_TIME:       return 1083;
        case COLUMN_TYPE_TIMESTAMP:  return 1114;
        case COLUMN_TYPE_TIMESTAMPTZ:return 1184;
        case COLUMN_TYPE_INTERVAL:   return 1186;
        case COLUMN_TYPE_UUID:       return 2950;
    }
    return 25;
}

static int16_t col_type_to_len(enum column_type t)
{
    if (t == COLUMN_TYPE_SMALLINT) return 2;
    if (t == COLUMN_TYPE_INT)      return 4;
    if (t == COLUMN_TYPE_FLOAT)    return 8;
    if (t == COLUMN_TYPE_BOOLEAN)  return 1;
    if (t == COLUMN_TYPE_BIGINT)   return 8;
    return -1;
}

/* Map column_type to PostgreSQL type name */
static const char *col_type_pg_name(enum column_type t)
{
    switch (t) {
        case COLUMN_TYPE_SMALLINT:   return "smallint";
        case COLUMN_TYPE_INT:        return "integer";
        case COLUMN_TYPE_BIGINT:     return "bigint";
        case COLUMN_TYPE_FLOAT:      return "double precision";
        case COLUMN_TYPE_NUMERIC:    return "numeric";
        case COLUMN_TYPE_TEXT:       return "text";
        case COLUMN_TYPE_ENUM:       return "USER-DEFINED";
        case COLUMN_TYPE_BOOLEAN:    return "boolean";
        case COLUMN_TYPE_DATE:       return "date";
        case COLUMN_TYPE_TIME:       return "time without time zone";
        case COLUMN_TYPE_TIMESTAMP:  return "timestamp without time zone";
        case COLUMN_TYPE_TIMESTAMPTZ:return "timestamp with time zone";
        case COLUMN_TYPE_INTERVAL:   return "interval";
        case COLUMN_TYPE_UUID:       return "uuid";
    }
    return "text";
}

/* Map column_type to pg_type typname */
static const char *col_type_to_typname(enum column_type t)
{
    switch (t) {
        case COLUMN_TYPE_SMALLINT:   return "int2";
        case COLUMN_TYPE_INT:        return "int4";
        case COLUMN_TYPE_FLOAT:      return "float8";
        case COLUMN_TYPE_TEXT:       return "text";
        case COLUMN_TYPE_ENUM:       return "text";
        case COLUMN_TYPE_BOOLEAN:    return "bool";
        case COLUMN_TYPE_BIGINT:     return "int8";
        case COLUMN_TYPE_NUMERIC:    return "numeric";
        case COLUMN_TYPE_DATE:       return "date";
        case COLUMN_TYPE_TIME:       return "time";
        case COLUMN_TYPE_TIMESTAMP:  return "timestamp";
        case COLUMN_TYPE_TIMESTAMPTZ:return "timestamptz";
        case COLUMN_TYPE_INTERVAL:   return "interval";
        case COLUMN_TYPE_UUID:       return "uuid";
    }
    return "text";
}

/* Add a text column to a table */
static void add_text_col(struct table *t, const char *name)
{
    struct column c = {0};
    c.name = strdup(name);
    c.type = COLUMN_TYPE_TEXT;
    table_add_column(t, &c);
}

/* Add an integer column to a table */
static void add_int_col(struct table *t, const char *name)
{
    struct column c = {0};
    c.name = strdup(name);
    c.type = COLUMN_TYPE_INT;
    table_add_column(t, &c);
}

/* Add a boolean column to a table */
static void add_bool_col(struct table *t, const char *name)
{
    struct column c = {0};
    c.name = strdup(name);
    c.type = COLUMN_TYPE_BOOLEAN;
    table_add_column(t, &c);
}

/* Create a text cell */
static struct cell text_cell(const char *val)
{
    struct cell c = {0};
    c.type = COLUMN_TYPE_TEXT;
    if (val) {
        c.value.as_text = strdup(val);
    } else {
        c.is_null = 1;
    }
    return c;
}

/* Create an int cell */
static struct cell int_cell(int val)
{
    struct cell c = {0};
    c.type = COLUMN_TYPE_INT;
    c.value.as_int = val;
    return c;
}

/* Create a bool cell */
static struct cell bool_cell(int val)
{
    struct cell c = {0};
    c.type = COLUMN_TYPE_BOOLEAN;
    c.value.as_bool = val;
    return c;
}

/* Create a null cell of given type */
static struct cell null_cell(enum column_type t)
{
    struct cell c = {0};
    c.type = t;
    c.is_null = 1;
    return c;
}

/* Add a row to a table from an array of cells */
static void push_row(struct table *t, struct cell *cells, size_t ncols)
{
    struct row r = {0};
    for (size_t i = 0; i < ncols; i++)
        da_push(&r.cells, cells[i]);
    da_push(&t->rows, r);
}

/* Remove existing catalog table by name if it exists */
static void remove_catalog_table(struct database *db, const char *name)
{
    for (size_t i = 0; i < db->tables.count; i++) {
        if (strcmp(db->tables.items[i].name, name) == 0) {
            table_free(&db->tables.items[i]);
            db->tables.items[i] = db->tables.items[db->tables.count - 1];
            db->tables.count--;
            return;
        }
    }
}

/* ---- catalog table builders ---- */

static void build_pg_namespace(struct database *db)
{
    remove_catalog_table(db, "pg_namespace");
    struct table t;
    table_init(&t, "pg_namespace");
    add_int_col(&t, "oid");
    add_text_col(&t, "nspname");
    add_int_col(&t, "nspowner");

    struct cell r1[] = { int_cell(11), text_cell("pg_catalog"), int_cell(10) };
    push_row(&t, r1, 3);
    struct cell r2[] = { int_cell(2200), text_cell("public"), int_cell(10) };
    push_row(&t, r2, 3);
    struct cell r3[] = { int_cell(13060), text_cell("information_schema"), int_cell(10) };
    push_row(&t, r3, 3);

    da_push(&db->tables, t);
}

static void build_pg_type(struct database *db)
{
    remove_catalog_table(db, "pg_type");
    struct table t;
    table_init(&t, "pg_type");
    add_int_col(&t, "oid");
    add_text_col(&t, "typname");
    add_int_col(&t, "typnamespace");
    add_int_col(&t, "typlen");
    add_text_col(&t, "typtype");
    add_int_col(&t, "typbasetype");
    add_int_col(&t, "typtypmod");
    add_bool_col(&t, "typnotnull");
    add_int_col(&t, "typrelid");
    add_int_col(&t, "typcollation");

    static const struct {
        int oid; const char *name; int len; const char *typtype; int typcoll;
    } types[] = {
        {16,   "bool",        1,   "b", 0},
        {20,   "int8",        8,   "b", 0},
        {21,   "int2",        2,   "b", 0},
        {23,   "int4",        4,   "b", 0},
        {25,   "text",        -1,  "b", 100},
        {701,  "float8",      8,   "b", 0},
        {1043, "varchar",     -1,  "b", 100},
        {1082, "date",        4,   "b", 0},
        {1083, "time",        8,   "b", 0},
        {1114, "timestamp",   8,   "b", 0},
        {1184, "timestamptz", 8,   "b", 0},
        {1186, "interval",    16,  "b", 0},
        {1700, "numeric",     -1,  "b", 0},
        {2950, "uuid",        16,  "b", 0},
    };
    for (size_t i = 0; i < sizeof(types)/sizeof(types[0]); i++) {
        struct cell r[] = {
            int_cell(types[i].oid),
            text_cell(types[i].name),
            int_cell(11), /* pg_catalog namespace */
            int_cell(types[i].len),
            text_cell(types[i].typtype),
            int_cell(0), /* typbasetype */
            int_cell(-1), /* typtypmod */
            bool_cell(0), /* typnotnull */
            int_cell(0), /* typrelid */
            int_cell(types[i].typcoll), /* typcollation */
        };
        push_row(&t, r, 10);
    }

    da_push(&db->tables, t);
}

static void build_pg_class(struct database *db)
{
    remove_catalog_table(db, "pg_class");
    struct table t;
    table_init(&t, "pg_class");
    add_int_col(&t, "oid");           /* 0 */
    add_text_col(&t, "relname");      /* 1 */
    add_int_col(&t, "relnamespace");  /* 2 */
    add_text_col(&t, "relkind");      /* 3 */
    add_int_col(&t, "relowner");      /* 4 */
    add_int_col(&t, "reltuples");     /* 5 */
    add_bool_col(&t, "relhasindex");  /* 6 */
    add_int_col(&t, "relnatts");      /* 7 */
    add_int_col(&t, "relam");         /* 8 */
    add_bool_col(&t, "relhasrules");  /* 9 */
    add_bool_col(&t, "relhastriggers"); /* 10 */
    add_bool_col(&t, "relhassubclass"); /* 11 */
    add_text_col(&t, "relacl");       /* 12 */
    add_bool_col(&t, "relispartition"); /* 13 */
    add_int_col(&t, "reltablespace"); /* 14 */
    add_int_col(&t, "relchecks");     /* 15 */
    add_int_col(&t, "reloftype");     /* 16 */
    add_bool_col(&t, "relrowsecurity"); /* 17 */
    add_bool_col(&t, "relforcerowsecurity"); /* 18 */
    add_text_col(&t, "relpersistence"); /* 19 */
    add_text_col(&t, "relreplident"); /* 20 */
    add_int_col(&t, "reltoastrelid"); /* 21 */

    for (size_t i = 0; i < db->tables.count; i++) {
        struct table *ut = &db->tables.items[i];
        /* skip catalog tables themselves and internal temp tables */
        if (ut->name[0] == '_' && ut->name[1] == '_') continue;
        if (catalog_is_catalog_table(ut->name)) continue;

        char relkind = ut->view_sql ? 'v' : 'r';
        char rk[2] = { relkind, '\0' };
        int has_idx = (ut->indexes.count > 0) ? 1 : 0;

        struct cell r[] = {
            int_cell((int)(16384 + i)),
            text_cell(ut->name),
            int_cell(2200), /* public namespace */
            text_cell(rk),
            int_cell(10), /* owner */
            int_cell((int)ut->rows.count),
            bool_cell(has_idx),
            int_cell((int)ut->columns.count),
            int_cell(2), /* relam: heap */
            bool_cell(0), /* relhasrules */
            bool_cell(0), /* relhastriggers */
            bool_cell(0), /* relhassubclass */
            null_cell(COLUMN_TYPE_TEXT), /* relacl */
            bool_cell(0), /* relispartition */
            int_cell(0), /* reltablespace */
            int_cell(0), /* relchecks */
            int_cell(0), /* reloftype */
            bool_cell(0), /* relrowsecurity */
            bool_cell(0), /* relforcerowsecurity */
            text_cell("p"), /* relpersistence: permanent */
            text_cell("d"), /* relreplident: default */
            int_cell(0), /* reltoastrelid */
        };
        push_row(&t, r, 22);

        /* also add index entries as pg_class rows */
        for (size_t ix = 0; ix < ut->indexes.count; ix++) {
            struct cell ri[] = {
                int_cell((int)(32768 + i * 100 + ix)),
                text_cell(ut->indexes.items[ix].name),
                int_cell(2200),
                text_cell("i"), /* index */
                int_cell(10),
                int_cell(0),
                bool_cell(0),
                int_cell(1),
                int_cell(403), /* btree */
                bool_cell(0),
                bool_cell(0),
                bool_cell(0),
                null_cell(COLUMN_TYPE_TEXT),
                bool_cell(0),
                int_cell(0),
                int_cell(0), /* relchecks */
                int_cell(0), /* reloftype */
                bool_cell(0), /* relrowsecurity */
                bool_cell(0), /* relforcerowsecurity */
                text_cell("p"), /* relpersistence */
                text_cell("d"), /* relreplident */
                int_cell(0), /* reltoastrelid */
            };
            push_row(&t, ri, 22);
        }
    }

    da_push(&db->tables, t);
}

static void build_pg_attribute(struct database *db)
{
    remove_catalog_table(db, "pg_attribute");
    struct table t;
    table_init(&t, "pg_attribute");
    add_int_col(&t, "attrelid");
    add_text_col(&t, "attname");
    add_int_col(&t, "atttypid");
    add_int_col(&t, "atttypmod");
    add_int_col(&t, "attlen");
    add_int_col(&t, "attnum");
    add_bool_col(&t, "attnotnull");
    add_bool_col(&t, "atthasdef");
    add_bool_col(&t, "attisdropped");
    add_text_col(&t, "attidentity");
    add_text_col(&t, "attgenerated");
    add_int_col(&t, "attcollation");

    for (size_t i = 0; i < db->tables.count; i++) {
        struct table *ut = &db->tables.items[i];
        if (ut->name[0] == '_' && ut->name[1] == '_') continue;
        if (catalog_is_catalog_table(ut->name)) continue;

        int relid = (int)(16384 + i);
        for (size_t c = 0; c < ut->columns.count; c++) {
            struct column *col = &ut->columns.items[c];
            /* text-like types get default collation OID 100 */
            int collation = column_type_is_text(col->type) ? 100 : 0;
            struct cell r[] = {
                int_cell(relid),
                text_cell(col->name),
                int_cell((int)col_type_to_oid(col->type)),
                int_cell(-1), /* atttypmod */
                int_cell((int)col_type_to_len(col->type)),
                int_cell((int)(c + 1)),
                bool_cell(col->not_null ? 1 : 0),
                bool_cell(col->has_default ? 1 : 0),
                bool_cell(0), /* attisdropped */
                text_cell(""), /* attidentity */
                text_cell(""), /* attgenerated */
                int_cell(collation), /* attcollation */
            };
            push_row(&t, r, 12);
        }
    }

    da_push(&db->tables, t);
}

static void build_pg_index(struct database *db)
{
    remove_catalog_table(db, "pg_index");
    struct table t;
    table_init(&t, "pg_index");
    add_int_col(&t, "indexrelid");
    add_int_col(&t, "indrelid");
    add_int_col(&t, "indnatts");
    add_bool_col(&t, "indisunique");
    add_bool_col(&t, "indisprimary");
    add_text_col(&t, "indkey");
    add_bool_col(&t, "indisvalid");

    for (size_t i = 0; i < db->tables.count; i++) {
        struct table *ut = &db->tables.items[i];
        if (ut->name[0] == '_' && ut->name[1] == '_') continue;
        if (catalog_is_catalog_table(ut->name)) continue;

        int relid = (int)(16384 + i);
        for (size_t ix = 0; ix < ut->indexes.count; ix++) {
            struct index *idx = &ut->indexes.items[ix];
            /* find column number */
            int col_num = idx->column_idx + 1;
            char key_buf[16];
            snprintf(key_buf, sizeof(key_buf), "%d", col_num);

            /* check if this is a primary key or unique index */
            int is_pk = 0, is_unique = 0;
            if ((size_t)idx->column_idx < ut->columns.count) {
                is_pk = ut->columns.items[idx->column_idx].is_primary_key;
                is_unique = ut->columns.items[idx->column_idx].is_unique || is_pk;
            }

            struct cell r[] = {
                int_cell((int)(32768 + i * 100 + ix)),
                int_cell(relid),
                int_cell(1), /* indnatts */
                bool_cell(is_unique),
                bool_cell(is_pk),
                text_cell(key_buf),
                bool_cell(1), /* indisvalid */
            };
            push_row(&t, r, 7);
        }
    }

    da_push(&db->tables, t);
}

static void build_pg_attrdef(struct database *db)
{
    remove_catalog_table(db, "pg_attrdef");
    struct table t;
    table_init(&t, "pg_attrdef");
    add_int_col(&t, "oid");
    add_int_col(&t, "adrelid");
    add_int_col(&t, "adnum");
    add_text_col(&t, "adbin");

    int oid_counter = 40000;
    for (size_t i = 0; i < db->tables.count; i++) {
        struct table *ut = &db->tables.items[i];
        if (ut->name[0] == '_' && ut->name[1] == '_') continue;
        if (catalog_is_catalog_table(ut->name)) continue;

        int relid = (int)(16384 + i);
        for (size_t c = 0; c < ut->columns.count; c++) {
            struct column *col = &ut->columns.items[c];
            if (!col->has_default && !col->is_serial) continue;
            struct cell r[] = {
                int_cell(oid_counter++),
                int_cell(relid),
                int_cell((int)(c + 1)),
                text_cell(col->is_serial ? "nextval('seq')" : "default"),
            };
            push_row(&t, r, 4);
        }
    }

    da_push(&db->tables, t);
}

static void build_pg_constraint(struct database *db)
{
    remove_catalog_table(db, "pg_constraint");
    struct table t;
    table_init(&t, "pg_constraint");
    add_int_col(&t, "oid");
    add_text_col(&t, "conname");
    add_int_col(&t, "connamespace");
    add_text_col(&t, "contype");
    add_int_col(&t, "conrelid");
    add_int_col(&t, "confrelid");
    add_text_col(&t, "conkey");
    add_text_col(&t, "confkey");
    add_bool_col(&t, "convalidated");

    int oid_counter = 50000;
    for (size_t i = 0; i < db->tables.count; i++) {
        struct table *ut = &db->tables.items[i];
        if (ut->name[0] == '_' && ut->name[1] == '_') continue;
        if (catalog_is_catalog_table(ut->name)) continue;

        int relid = (int)(16384 + i);
        for (size_t c = 0; c < ut->columns.count; c++) {
            struct column *col = &ut->columns.items[c];
            char key_buf[16];
            snprintf(key_buf, sizeof(key_buf), "{%d}", (int)(c + 1));

            if (col->is_primary_key) {
                char name_buf[256];
                snprintf(name_buf, sizeof(name_buf), "%s_%s_pkey", ut->name, col->name);
                struct cell r[] = {
                    int_cell(oid_counter++),
                    text_cell(name_buf),
                    int_cell(2200),
                    text_cell("p"),
                    int_cell(relid),
                    int_cell(0),
                    text_cell(key_buf),
                    null_cell(COLUMN_TYPE_TEXT),
                    bool_cell(1),
                };
                push_row(&t, r, 9);
            }
            if (col->is_unique && !col->is_primary_key) {
                char name_buf[256];
                snprintf(name_buf, sizeof(name_buf), "%s_%s_key", ut->name, col->name);
                struct cell r[] = {
                    int_cell(oid_counter++),
                    text_cell(name_buf),
                    int_cell(2200),
                    text_cell("u"),
                    int_cell(relid),
                    int_cell(0),
                    text_cell(key_buf),
                    null_cell(COLUMN_TYPE_TEXT),
                    bool_cell(1),
                };
                push_row(&t, r, 9);
            }
            if (col->fk_table) {
                char name_buf[256];
                snprintf(name_buf, sizeof(name_buf), "%s_%s_fkey", ut->name, col->name);
                /* find referenced table oid */
                int conf_relid = 0;
                char conf_key[16] = "{1}";
                for (size_t j = 0; j < db->tables.count; j++) {
                    if (strcmp(db->tables.items[j].name, col->fk_table) == 0) {
                        conf_relid = (int)(16384 + j);
                        /* find referenced column */
                        for (size_t k = 0; k < db->tables.items[j].columns.count; k++) {
                            if (strcmp(db->tables.items[j].columns.items[k].name, col->fk_column) == 0) {
                                snprintf(conf_key, sizeof(conf_key), "{%d}", (int)(k + 1));
                                break;
                            }
                        }
                        break;
                    }
                }
                struct cell r[] = {
                    int_cell(oid_counter++),
                    text_cell(name_buf),
                    int_cell(2200),
                    text_cell("f"),
                    int_cell(relid),
                    int_cell(conf_relid),
                    text_cell(key_buf),
                    text_cell(conf_key),
                    bool_cell(1),
                };
                push_row(&t, r, 9);
            }
        }
    }

    da_push(&db->tables, t);
}

static void build_pg_am(struct database *db)
{
    remove_catalog_table(db, "pg_am");
    struct table t;
    table_init(&t, "pg_am");
    add_int_col(&t, "oid");
    add_text_col(&t, "amname");
    add_text_col(&t, "amtype");

    struct cell r1[] = { int_cell(2), text_cell("heap"), text_cell("t") };
    push_row(&t, r1, 3);
    struct cell r2[] = { int_cell(403), text_cell("btree"), text_cell("i") };
    push_row(&t, r2, 3);
    struct cell r3[] = { int_cell(405), text_cell("hash"), text_cell("i") };
    push_row(&t, r3, 3);

    da_push(&db->tables, t);
}

static void build_pg_database(struct database *db)
{
    remove_catalog_table(db, "pg_database");
    struct table t;
    table_init(&t, "pg_database");
    add_int_col(&t, "oid");
    add_text_col(&t, "datname");
    add_int_col(&t, "datdba");
    add_int_col(&t, "encoding");
    add_text_col(&t, "datcollate");
    add_text_col(&t, "datctype");
    add_text_col(&t, "datacl");

    struct cell r[] = {
        int_cell(16384),
        text_cell(db->name),
        int_cell(10),
        int_cell(6), /* UTF8 */
        text_cell("en_US.UTF-8"),
        text_cell("en_US.UTF-8"),
        null_cell(COLUMN_TYPE_TEXT),
    };
    push_row(&t, r, 7);

    da_push(&db->tables, t);
}

static void build_pg_roles(struct database *db)
{
    remove_catalog_table(db, "pg_roles");
    struct table t;
    table_init(&t, "pg_roles");
    add_int_col(&t, "oid");
    add_text_col(&t, "rolname");
    add_bool_col(&t, "rolsuper");
    add_bool_col(&t, "rolcreatedb");
    add_bool_col(&t, "rolcreaterole");
    add_bool_col(&t, "rolinherit");
    add_bool_col(&t, "rolcanlogin");

    struct cell r[] = {
        int_cell(10),
        text_cell(db->name),
        bool_cell(1),
        bool_cell(1),
        bool_cell(1),
        bool_cell(1),
        bool_cell(1),
    };
    push_row(&t, r, 7);

    da_push(&db->tables, t);
}

static void build_pg_settings(struct database *db)
{
    remove_catalog_table(db, "pg_settings");
    struct table t;
    table_init(&t, "pg_settings");
    add_text_col(&t, "name");
    add_text_col(&t, "setting");
    add_text_col(&t, "category");

    static const struct { const char *name; const char *setting; const char *cat; } settings[] = {
        {"server_version",     "15.0",                "Version"},
        {"server_encoding",    "UTF8",                "Client"},
        {"client_encoding",    "UTF8",                "Client"},
        {"search_path",        "\"$user\", public",   "Client"},
        {"standard_conforming_strings", "on",          "Client"},
        {"TimeZone",           "UTC",                  "Client"},
        {"DateStyle",          "ISO, MDY",             "Client"},
        {"IntervalStyle",      "postgres",             "Client"},
        {"integer_datetimes",  "on",                   "Preset"},
        {"max_identifier_length", "63",                "Preset"},
    };
    for (size_t i = 0; i < sizeof(settings)/sizeof(settings[0]); i++) {
        struct cell r[] = {
            text_cell(settings[i].name),
            text_cell(settings[i].setting),
            text_cell(settings[i].cat),
        };
        push_row(&t, r, 3);
    }

    da_push(&db->tables, t);
}

static void build_information_schema_tables(struct database *db)
{
    remove_catalog_table(db, "information_schema_tables");
    struct table t;
    table_init(&t, "information_schema_tables");
    add_text_col(&t, "table_catalog");
    add_text_col(&t, "table_schema");
    add_text_col(&t, "table_name");
    add_text_col(&t, "table_type");

    for (size_t i = 0; i < db->tables.count; i++) {
        struct table *ut = &db->tables.items[i];
        if (ut->name[0] == '_' && ut->name[1] == '_') continue;
        if (catalog_is_catalog_table(ut->name)) continue;
        const char *ttype = ut->view_sql ? "VIEW" : "BASE TABLE";
        struct cell r[] = {
            text_cell(db->name),
            text_cell("public"),
            text_cell(ut->name),
            text_cell(ttype),
        };
        push_row(&t, r, 4);
    }

    da_push(&db->tables, t);
}

static void build_information_schema_columns(struct database *db)
{
    remove_catalog_table(db, "information_schema_columns");
    struct table t;
    table_init(&t, "information_schema_columns");
    add_text_col(&t, "table_catalog");
    add_text_col(&t, "table_schema");
    add_text_col(&t, "table_name");
    add_text_col(&t, "column_name");
    add_int_col(&t, "ordinal_position");
    add_text_col(&t, "column_default");
    add_text_col(&t, "is_nullable");
    add_text_col(&t, "data_type");
    add_text_col(&t, "udt_name");

    for (size_t i = 0; i < db->tables.count; i++) {
        struct table *ut = &db->tables.items[i];
        if (ut->name[0] == '_' && ut->name[1] == '_') continue;
        if (catalog_is_catalog_table(ut->name)) continue;

        for (size_t c = 0; c < ut->columns.count; c++) {
            struct column *col = &ut->columns.items[c];
            struct cell r[] = {
                text_cell(db->name),
                text_cell("public"),
                text_cell(ut->name),
                text_cell(col->name),
                int_cell((int)(c + 1)),
                null_cell(COLUMN_TYPE_TEXT), /* column_default */
                text_cell(col->not_null ? "NO" : "YES"),
                text_cell(col_type_pg_name(col->type)),
                text_cell(col_type_to_typname(col->type)),
            };
            push_row(&t, r, 9);
        }
    }

    da_push(&db->tables, t);
}

static void build_pg_policy(struct database *db)
{
    remove_catalog_table(db, "pg_policy");
    struct table t;
    table_init(&t, "pg_policy");
    add_int_col(&t, "oid");
    add_text_col(&t, "polname");
    add_int_col(&t, "polrelid");
    add_text_col(&t, "polcmd");
    add_bool_col(&t, "polpermissive");
    add_text_col(&t, "polroles");
    add_int_col(&t, "polqual");
    add_int_col(&t, "polwithcheck");
    /* empty — no row-level security policies */
    da_push(&db->tables, t);
}

static void build_pg_collation(struct database *db)
{
    remove_catalog_table(db, "pg_collation");
    struct table t;
    table_init(&t, "pg_collation");
    add_int_col(&t, "oid");
    add_text_col(&t, "collname");
    add_int_col(&t, "collnamespace");
    /* empty — no custom collations */
    da_push(&db->tables, t);
}

/* ---- public API ---- */

int catalog_is_catalog_table(const char *name)
{
    return (strncmp(name, "pg_", 3) == 0 ||
            strncmp(name, "information_schema_", 19) == 0);
}

void catalog_cleanup(struct database *db)
{
    static const char *catalog_names[] = {
        "pg_namespace", "pg_type", "pg_class", "pg_attribute",
        "pg_index", "pg_attrdef", "pg_constraint", "pg_am",
        "pg_database", "pg_roles", "pg_settings",
        "pg_policy", "pg_collation",
        "information_schema_tables", "information_schema_columns",
    };
    for (size_t i = 0; i < sizeof(catalog_names)/sizeof(catalog_names[0]); i++)
        remove_catalog_table(db, catalog_names[i]);
}

void catalog_refresh(struct database *db)
{
    /* Remove old catalog tables first */
    catalog_cleanup(db);

    /* Build in dependency order (pg_class references pg_namespace OIDs etc.) */
    build_pg_namespace(db);
    build_pg_type(db);
    build_pg_class(db);
    build_pg_attribute(db);
    build_pg_index(db);
    build_pg_attrdef(db);
    build_pg_constraint(db);
    build_pg_am(db);
    build_pg_database(db);
    build_pg_roles(db);
    build_pg_settings(db);
    build_pg_policy(db);
    build_pg_collation(db);
    build_information_schema_tables(db);
    build_information_schema_columns(db);
}

const char *catalog_resolve_name(const char *schema, size_t schema_len,
                                  const char *table, size_t table_len,
                                  char *buf, size_t bufsz)
{
    /* pg_catalog.X -> X (bare name, catalog tables are stored as "pg_X") */
    if (schema_len == 10 && strncasecmp(schema, "pg_catalog", 10) == 0) {
        if (table_len < bufsz) {
            memcpy(buf, table, table_len);
            buf[table_len] = '\0';
            return buf;
        }
    }
    /* information_schema.X -> "information_schema_X" */
    if (schema_len == 18 && strncasecmp(schema, "information_schema", 18) == 0) {
        size_t needed = 19 + table_len + 1; /* "information_schema_" + table + NUL */
        if (needed <= bufsz) {
            memcpy(buf, "information_schema_", 19);
            memcpy(buf + 19, table, table_len);
            buf[19 + table_len] = '\0';
            return buf;
        }
    }
    /* public.X -> X */
    if (schema_len == 6 && strncasecmp(schema, "public", 6) == 0) {
        if (table_len < bufsz) {
            memcpy(buf, table, table_len);
            buf[table_len] = '\0';
            return buf;
        }
    }
    return NULL;
}
