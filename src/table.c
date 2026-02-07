#include "table.h"
#include <string.h>

void table_init(struct table *t, const char *name)
{
    t->name = strdup(name);
    da_init(&t->columns);
    da_init(&t->rows);
    da_init(&t->indexes);
}

void table_add_column(struct table *t, struct column *col)
{
    struct column c = {
        .name = strdup(col->name),
        .type = col->type,
        .enum_type_name = col->enum_type_name ? strdup(col->enum_type_name) : NULL
    };
    da_push(&t->columns, c);
}

void table_free(struct table *t)
{
    free(t->name);
    for (size_t i = 0; i < t->columns.count; i++) {
        free(t->columns.items[i].name);
        free(t->columns.items[i].enum_type_name);
    }
    da_free(&t->columns);

    for (size_t i = 0; i < t->rows.count; i++) {
        row_free(&t->rows.items[i]);
    }
    da_free(&t->rows);

    for (size_t i = 0; i < t->indexes.count; i++) {
        index_free(&t->indexes.items[i]);
    }
    da_free(&t->indexes);
}
