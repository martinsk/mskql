#include "explain_ast.h"
#include "arena_helpers.h"
#include "stringview.h"
#include <stdio.h>
#include <string.h>

/* ---- helpers ---- */

static const char *agg_func_name(enum agg_func f)
{
    switch (f) {
    case AGG_SUM:       return "SUM";
    case AGG_COUNT:     return "COUNT";
    case AGG_AVG:       return "AVG";
    case AGG_MIN:       return "MIN";
    case AGG_MAX:       return "MAX";
    case AGG_STRING_AGG: return "STRING_AGG";
    case AGG_ARRAY_AGG:  return "ARRAY_AGG";
    case AGG_BOOL_AND:   return "BOOL_AND";
    case AGG_BOOL_OR:    return "BOOL_OR";
    case AGG_STDDEV:     return "STDDEV";
    case AGG_VARIANCE:   return "VARIANCE";
    case AGG_NONE:        return "AGG";
    }
}

static const char *join_type_name(int jt)
{
    switch (jt) {
    case 0: return "INNER"; case 1: return "LEFT";
    case 2: return "RIGHT"; case 3: return "FULL";
    case 4: return "CROSS"; default: return "?";
    }
}

static const char *cmp_op_name(enum cmp_op op)
{
    switch (op) {
    case CMP_EQ:  return "=";  case CMP_NE: return "!=";
    case CMP_LT:  return "<";  case CMP_GT: return ">";
    case CMP_LE:  return "<="; case CMP_GE: return ">=";
    case CMP_IS_NULL:     return "IS NULL";
    case CMP_IS_NOT_NULL: return "IS NOT NULL";
    case CMP_BETWEEN:     return "BETWEEN";
    case CMP_LIKE:        return "LIKE";
    case CMP_ILIKE:       return "ILIKE";
    case CMP_IN:                    return "IN";
    case CMP_NOT_IN:               return "NOT IN";
    case CMP_IS_DISTINCT:          return "IS DISTINCT FROM";
    case CMP_IS_NOT_DISTINCT:      return "IS NOT DISTINCT FROM";
    case CMP_EXISTS:               return "EXISTS";
    case CMP_NOT_EXISTS:           return "NOT EXISTS";
    case CMP_REGEX_MATCH:          return "~";
    case CMP_REGEX_NOT_MATCH:      return "!~";
    case CMP_REGEX_ICASE_MATCH:    return "~*";
    case CMP_REGEX_ICASE_NOT_MATCH: return "!~*";
    case CMP_IS_NOT_TRUE:          return "IS NOT TRUE";
    case CMP_IS_NOT_FALSE:         return "IS NOT FALSE";
    case CMP_SIMILAR_TO:           return "SIMILAR TO";
    case CMP_NOT_SIMILAR_TO:       return "NOT SIMILAR TO";
    }
}

/* Print condition tree as human-readable text. Returns bytes written. */
static int cond_str(struct query_arena *arena, uint32_t cond_idx,
                    char *buf, int buflen)
{
    if (cond_idx == IDX_NONE || cond_idx >= (uint32_t)arena->conditions.count)
        return snprintf(buf, buflen, "(?)");
    struct condition *c = &arena->conditions.items[cond_idx];
    int w = 0, r;
    switch (c->type) {
    case COND_AND:
        w  = cond_str(arena, c->left, buf, buflen);
        r  = snprintf(buf+w, buflen-w, " AND ");  if (r > 0) w += r;
        r  = cond_str(arena, c->right, buf+w, buflen-w); if (r > 0) w += r;
        return w;
    case COND_OR:
        r = snprintf(buf, buflen, "("); if (r>0) w+=r;
        r = cond_str(arena, c->left, buf+w, buflen-w); if (r>0) w+=r;
        r = snprintf(buf+w, buflen-w, " OR "); if (r>0) w+=r;
        r = cond_str(arena, c->right, buf+w, buflen-w); if (r>0) w+=r;
        r = snprintf(buf+w, buflen-w, ")"); if (r>0) w+=r;
        return w;
    case COND_NOT:
        r = snprintf(buf, buflen, "NOT "); if (r>0) w+=r;
        r = cond_str(arena, c->left, buf+w, buflen-w); if (r>0) w+=r;
        return w;
    case COND_COMPARE: {
        const char *op = cmp_op_name(c->op);
        if (c->rhs_column.len > 0)
            return snprintf(buf, buflen, SV_FMT " %s " SV_FMT,
                            (int)c->column.len, c->column.data,
                            op,
                            (int)c->rhs_column.len, c->rhs_column.data);
        if (c->op == CMP_IS_NULL || c->op == CMP_IS_NOT_NULL)
            return snprintf(buf, buflen, SV_FMT " %s",
                            (int)c->column.len, c->column.data, op);
        if (c->value.is_null)
            return snprintf(buf, buflen, SV_FMT " %s NULL",
                            (int)c->column.len, c->column.data, op);
        switch (c->value.type) {
        case COLUMN_TYPE_INT:
            return snprintf(buf, buflen, SV_FMT " %s %d",
                            (int)c->column.len, c->column.data, op,
                            c->value.value.as_int);
        case COLUMN_TYPE_BIGINT:
            return snprintf(buf, buflen, SV_FMT " %s %lld",
                            (int)c->column.len, c->column.data, op,
                            (long long)c->value.value.as_bigint);
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC:
            return snprintf(buf, buflen, SV_FMT " %s %.15g",
                            (int)c->column.len, c->column.data, op,
                            c->value.value.as_float);
        case COLUMN_TYPE_TEXT:
            return snprintf(buf, buflen, SV_FMT " %s '%s'",
                            (int)c->column.len, c->column.data, op,
                            c->value.value.as_text ? c->value.value.as_text : "");
        case COLUMN_TYPE_BOOLEAN:
            return snprintf(buf, buflen, SV_FMT " %s %s",
                            (int)c->column.len, c->column.data, op,
                            c->value.value.as_bool ? "true" : "false");
        case COLUMN_TYPE_SMALLINT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
        case COLUMN_TYPE_UUID:
        case COLUMN_TYPE_VECTOR:
            return snprintf(buf, buflen, SV_FMT " %s ?",
                            (int)c->column.len, c->column.data, op);
        }
        return snprintf(buf, buflen, SV_FMT " %s ?",
                        (int)c->column.len, c->column.data, op);
    }
    case COND_MULTI_IN:
        return snprintf(buf, buflen, "(multi-col) IN (...)");
    }
    return snprintf(buf, buflen, "(?)");
}

/* ---- query_select_print ---- */

int query_select_print(struct query_select *s, struct query_arena *arena,
                       char *buf, int buflen)
{
    int w = 0, r;

#define W(...)  do { r = snprintf(buf+w, buflen-w, __VA_ARGS__); if (r > 0) w += r; } while(0)
#define FIELD(name) W("  %-10s ", name)

    W("Parse AST (SELECT)\n");

    /* FROM */
    FIELD("FROM:");
    if (s->table.len > 0) {
        W(SV_FMT, (int)s->table.len, s->table.data);
        if (s->table_alias.len > 0)
            W(" [alias: " SV_FMT "]", (int)s->table_alias.len, s->table_alias.data);
    } else if (s->from_subquery_sql != IDX_NONE) {
        W("(subquery)");
        if (s->from_subquery_alias.len > 0)
            W(" [alias: " SV_FMT "]",
              (int)s->from_subquery_alias.len, s->from_subquery_alias.data);
    } else if (s->has_generate_series) {
        W("generate_series(...)");
    } else {
        W("(none)");
    }
    W("\n");

    /* COLUMNS */
    FIELD("COLUMNS:");
    if (s->columns.len > 0) {
        W(SV_FMT, (int)s->columns.len, s->columns.data);
    } else if (s->parsed_columns_count > 0) {
        for (uint32_t i = 0; i < s->parsed_columns_count && w < buflen-4; i++) {
            struct select_column *sc = &arena->select_cols.items[s->parsed_columns_start + i];
            if (i > 0) W(", ");
            if (sc->alias.len > 0)
                W(SV_FMT " AS " SV_FMT,
                  (int)sc->alias.len, sc->alias.data,
                  (int)sc->alias.len, sc->alias.data);
            else
                W("expr#%u", i);
        }
    } else if (s->aggregates_count > 0) {
        for (uint32_t i = 0; i < s->aggregates_count && w < buflen-4; i++) {
            struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + i];
            if (i > 0) W(", ");
            if (ae->column.len > 0)
                W("%s(" SV_FMT ")", agg_func_name(ae->func),
                  (int)ae->column.len, ae->column.data);
            else
                W("%s(*)", agg_func_name(ae->func));
            if (ae->alias.len > 0)
                W(" AS " SV_FMT, (int)ae->alias.len, ae->alias.data);
        }
    } else {
        W("*");
    }
    if (s->has_distinct) W(" [DISTINCT]");
    W("\n");

    /* WHERE */
    if (s->where.has_where && s->where.where_cond != IDX_NONE) {
        char pbuf[512] = "?";
        cond_str(arena, s->where.where_cond, pbuf, sizeof(pbuf));
        FIELD("WHERE:");
        W("%s\n", pbuf);
    }

    /* JOIN(s) */
    if (s->has_join && s->joins_count > 0) {
        for (uint32_t j = 0; j < s->joins_count && w < buflen-4; j++) {
            struct join_info *ji = &arena->joins.items[s->joins_start + j];
            FIELD("JOIN:");
            W(SV_FMT, (int)ji->join_table.len, ji->join_table.data);
            if (ji->join_alias.len > 0)
                W(" " SV_FMT, (int)ji->join_alias.len, ji->join_alias.data);
            if (ji->join_left_col.len > 0 && ji->join_right_col.len > 0)
                W(" ON " SV_FMT " = " SV_FMT,
                  (int)ji->join_left_col.len, ji->join_left_col.data,
                  (int)ji->join_right_col.len, ji->join_right_col.data);
            else if (ji->is_natural)
                W(" (NATURAL)");
            W(" (%s)\n", join_type_name(ji->join_type));
        }
    }

    /* GROUP BY */
    if (s->has_group_by && s->group_by_count > 0) {
        FIELD("GROUP BY:");
        for (uint32_t k = 0; k < s->group_by_count && w < buflen-4; k++) {
            sv col = arena->svs.items[s->group_by_start + k];
            if (k > 0) W(", ");
            W(SV_FMT, (int)col.len, col.data);
        }
        W("\n");
    }

    /* HAVING */
    if (s->has_having && s->having_cond != IDX_NONE) {
        char pbuf[512] = "?";
        cond_str(arena, s->having_cond, pbuf, sizeof(pbuf));
        FIELD("HAVING:");
        W("%s\n", pbuf);
    }

    /* ORDER BY */
    if (s->has_order_by && s->order_by_count > 0) {
        FIELD("ORDER BY:");
        for (uint32_t k = 0; k < s->order_by_count && w < buflen-4; k++) {
            struct order_by_item *ob = &arena->order_items.items[s->order_by_start + k];
            if (k > 0) W(", ");
            W(SV_FMT "%s", (int)ob->column.len, ob->column.data,
              ob->desc ? " DESC" : "");
        }
        W("\n");
    }

    /* LIMIT / OFFSET */
    if (s->has_limit) {
        FIELD("LIMIT:");
        W("%d\n", s->limit_count);
    }
    if (s->has_offset) {
        FIELD("OFFSET:");
        W("%d\n", s->offset_count);
    }

    /* CTEs */
    if (s->ctes_count > 0) {
        FIELD("CTEs:");
        W("%u CTEs\n", s->ctes_count);
    } else if (s->cte_name != IDX_NONE) {
        const char *cname = ASTRING(arena, s->cte_name);
        FIELD("CTEs:");
        W("%s\n", cname ? cname : "?");
    }

#undef W
#undef FIELD

    /* null-terminate */
    if (w < buflen) buf[w] = '\0';
    return w;
}
