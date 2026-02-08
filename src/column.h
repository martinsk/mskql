#ifndef COLUMN_H
#define COLUMN_H

#include "dynamic_array.h"

enum column_type {
    COLUMN_TYPE_INT,
    COLUMN_TYPE_FLOAT,
    COLUMN_TYPE_TEXT,
    COLUMN_TYPE_ENUM
};

struct enum_type {
    // TODO: STRINGVIEW OPPORTUNITY: name is always strdup'd from an sv-originated string.
    // If this were an sv pointing into a persistent schema string, the strdup/free overhead
    // in enum_type_free and all callers could be eliminated.
    char *name;
    DYNAMIC_ARRAY(char *) values;
};

void enum_type_free(struct enum_type *et);
int  enum_type_valid(struct enum_type *et, const char *value);

struct column {
    // TODO: STRINGVIEW OPPORTUNITY: name and enum_type_name are char* requiring strdup/free
    // on every copy (table_add_column, do_single_join, rebuild_indexes, query_free, etc.).
    // These originate from parser tokens which are already sv. Storing them as sv (pointing
    // into the original SQL or a persistent schema buffer) would eliminate many allocations.
    char *name;
    enum column_type type;
    char *enum_type_name;
};

#endif
