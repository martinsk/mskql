#include "table.h"
#include "row.h"
#include "stringview.h"
#include <string.h>
#include <stdlib.h>

void table_init(struct table *t, const char *name)
{
    t->name = strdup(name);
    t->view_sql = NULL;
    da_init(&t->columns);
    da_init(&t->rows);
    da_init(&t->indexes);
}

void table_init_own(struct table *t, char *name)
{
    t->name = name;
    t->view_sql = NULL;
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
        .is_primary_key = col->is_primary_key,
        .is_serial = col->is_serial,
        .serial_next = col->serial_next,
        .fk_table = col->fk_table ? strdup(col->fk_table) : NULL,
        .fk_column = col->fk_column ? strdup(col->fk_column) : NULL,
        .fk_on_delete_cascade = col->fk_on_delete_cascade,
        .fk_on_update_cascade = col->fk_on_update_cascade
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
    dst->view_sql = src->view_sql ? strdup(src->view_sql) : NULL;
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
            .is_primary_key = sc->is_primary_key,
            .is_serial = sc->is_serial,
            .serial_next = sc->serial_next,
            .fk_table = sc->fk_table ? strdup(sc->fk_table) : NULL,
            .fk_column = sc->fk_column ? strdup(sc->fk_column) : NULL,
            .fk_on_delete_cascade = sc->fk_on_delete_cascade,
            .fk_on_update_cascade = sc->fk_on_update_cascade
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

int table_find_column_sv(struct table *t, sv name)
{
    /* exact match first */
    for (size_t i = 0; i < t->columns.count; i++) {
        if (sv_eq_cstr(name, t->columns.items[i].name))
            return (int)i;
    }
    /* strip "table." prefix and retry */
    sv col = name;
    for (size_t k = 0; k < name.len; k++) {
        if (name.data[k] == '.') {
            col = sv_from(name.data + k + 1, name.len - k - 1);
            break;
        }
    }
    if (col.data != name.data) {
        for (size_t i = 0; i < t->columns.count; i++) {
            if (sv_eq_cstr(col, t->columns.items[i].name))
                return (int)i;
        }
    }
    /* try matching bare name against suffix of "table.col" stored names */
    for (size_t i = 0; i < t->columns.count; i++) {
        const char *cname = t->columns.items[i].name;
        const char *dot = strchr(cname, '.');
        if (dot) {
            const char *suffix = dot + 1;
            if (sv_eq_cstr(name, suffix))
                return (int)i;
        }
    }
    return -1;
}

int table_find_column(struct table *t, const char *name)
{
    return table_find_column_sv(t, sv_from(name, strlen(name)));
}

int resolve_alias_to_column(struct table *t, sv columns, sv alias)
{
    const char *p = columns.data;
    const char *end = columns.data + columns.len;
    while (p < end) {
        while (p < end && (*p == ' ' || *p == '\t' || *p == '\n')) p++;
        const char *col_start = p;
        while (p < end && *p != ' ' && *p != ',' && *p != '\t' && *p != '\n') p++;
        sv col_name = sv_from(col_start, (size_t)(p - col_start));
        while (p < end && (*p == ' ' || *p == '\t')) p++;
        if (p + 2 < end && (p[0] == 'A' || p[0] == 'a') &&
            (p[1] == 'S' || p[1] == 's') && (p[2] == ' ' || p[2] == '\t')) {
            p += 2;
            while (p < end && (*p == ' ' || *p == '\t')) p++;
            const char *alias_start = p;
            while (p < end && *p != ' ' && *p != ',' && *p != '\t' && *p != '\n' && *p != ';') p++;
            sv alias_name = sv_from(alias_start, (size_t)(p - alias_start));
            if (sv_eq_ignorecase(alias, alias_name))
                return table_find_column_sv(t, col_name);
        }
        while (p < end && *p != ',') p++;
        if (p < end) p++;
    }
    return -1;
}

void table_free(struct table *t)
{
    free(t->name);
    free(t->view_sql);
    for (size_t i = 0; i < t->columns.count; i++) {
        free(t->columns.items[i].name);
        free(t->columns.items[i].enum_type_name);
        free(t->columns.items[i].fk_table);
        free(t->columns.items[i].fk_column);
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
