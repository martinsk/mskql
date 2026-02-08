#include "table.h"
#include "row.h"
#include <string.h>
#include <stdlib.h>

void table_init(struct table *t, const char *name)
{
    t->name = strdup(name);
    da_init(&t->columns);
    da_init(&t->rows);
    da_init(&t->indexes);
}

void table_init_own(struct table *t, char *name)
{
    t->name = name;
    da_init(&t->columns);
    da_init(&t->rows);
    da_init(&t->indexes);
}

void table_add_column(struct table *t, struct column *col)
{
    struct column c = {
        .name = strdup(col->name),
        .type = col->type,
        .enum_type_name = col->enum_type_name ? strdup(col->enum_type_name) : NULL,
        .not_null = col->not_null,
        .has_default = col->has_default,
        .default_value = NULL,
        .is_unique = col->is_unique,
        .is_primary_key = col->is_primary_key
    };
    if (col->has_default && col->default_value) {
        c.default_value = calloc(1, sizeof(struct cell));
        c.default_value->type = col->default_value->type;
        c.default_value->is_null = col->default_value->is_null;
        if (column_type_is_text(col->default_value->type) && col->default_value->value.as_text)
            c.default_value->value.as_text = strdup(col->default_value->value.as_text);
        else
            c.default_value->value = col->default_value->value;
    }
    da_push(&t->columns, c);
}

void table_deep_copy(struct table *dst, const struct table *src)
{
    dst->name = strdup(src->name);
    da_init(&dst->columns);
    da_init(&dst->rows);
    da_init(&dst->indexes);

    /* deep-copy columns */
    for (size_t i = 0; i < src->columns.count; i++) {
        struct column *sc = &src->columns.items[i];
        struct column c = {
            .name = strdup(sc->name),
            .type = sc->type,
            .enum_type_name = sc->enum_type_name ? strdup(sc->enum_type_name) : NULL,
            .not_null = sc->not_null,
            .has_default = sc->has_default,
            .default_value = NULL,
            .is_unique = sc->is_unique,
            .is_primary_key = sc->is_primary_key
        };
        if (sc->has_default && sc->default_value) {
            c.default_value = calloc(1, sizeof(struct cell));
            c.default_value->type = sc->default_value->type;
            c.default_value->is_null = sc->default_value->is_null;
            if (column_type_is_text(sc->default_value->type) && sc->default_value->value.as_text)
                c.default_value->value.as_text = strdup(sc->default_value->value.as_text);
            else
                c.default_value->value = sc->default_value->value;
        }
        da_push(&dst->columns, c);
    }

    /* deep-copy rows */
    for (size_t i = 0; i < src->rows.count; i++) {
        struct row r = {0};
        da_init(&r.cells);
        for (size_t j = 0; j < src->rows.items[i].cells.count; j++) {
            struct cell *sc = &src->rows.items[i].cells.items[j];
            struct cell c = { .type = sc->type, .is_null = sc->is_null };
            if (column_type_is_text(sc->type) && sc->value.as_text)
                c.value.as_text = strdup(sc->value.as_text);
            else
                c.value = sc->value;
            da_push(&r.cells, c);
        }
        da_push(&dst->rows, r);
    }

    /* skip indexes — they will be rebuilt if needed */
}

void table_free(struct table *t)
{
    free(t->name);
    for (size_t i = 0; i < t->columns.count; i++) {
        free(t->columns.items[i].name);
        free(t->columns.items[i].enum_type_name);
        if (t->columns.items[i].default_value) {
            if (column_type_is_text(t->columns.items[i].default_value->type)
                && t->columns.items[i].default_value->value.as_text)
                free(t->columns.items[i].default_value->value.as_text);
            free(t->columns.items[i].default_value);
        }
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
