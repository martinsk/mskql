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
    char *name;
    DYNAMIC_ARRAY(char *) values;
};

void enum_type_free(struct enum_type *et);
int  enum_type_valid(struct enum_type *et, const char *value);

struct column {
    char *name;
    enum column_type type;
    char *enum_type_name;
};

#endif
