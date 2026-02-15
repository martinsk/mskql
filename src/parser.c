#include "parser.h"
#include "stringview.h"
#include "row.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum token_type {
    TOK_KEYWORD,
    TOK_IDENTIFIER,
    TOK_STRING,
    TOK_STAR,
    TOK_COMMA,
    TOK_SEMICOLON,
    TOK_NUMBER,
    TOK_LPAREN,
    TOK_RPAREN,
    TOK_DOT,
    TOK_EQUALS,
    TOK_NOT_EQUALS,
    TOK_LESS,
    TOK_GREATER,
    TOK_LESS_EQ,
    TOK_GREATER_EQ,
    TOK_PLUS,
    TOK_SLASH,
    TOK_MINUS,
    TOK_PERCENT,
    TOK_PIPE_PIPE,
    TOK_DOUBLE_COLON,
    TOK_TILDE,
    TOK_BANG_TILDE,
    TOK_EOF,
    TOK_UNKNOWN
};

struct token {
    enum token_type type;
    sv value;
};

struct lexer {
    const char *input;
    size_t pos;
};

static void lexer_init(struct lexer *l, const char *input)
{
    l->input = input;
    l->pos = 0;
}

static void skip_whitespace(struct lexer *l)
{
    if (!l->input) return;
    while (l->input[l->pos] && isspace((unsigned char)l->input[l->pos]))
        l->pos++;
}

static int is_keyword(sv word)
{
    const char *keywords[] = {
        "SELECT", "INSERT", "INTO", "VALUES", "FROM",
        "DELETE", "WHERE", "CREATE", "TABLE", "DROP",
        "JOIN", "ON", "RETURNING", "INDEX",
        "TYPE", "AS", "ENUM",
        "SUM", "COUNT", "AVG", "MIN", "MAX", "STRING_AGG", "ARRAY_AGG",
        "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
        "PERCENT_RANK", "CUME_DIST",
        "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE",
        "OVER", "PARTITION", "BY", "ORDER",
        "UPDATE", "SET", "AND", "OR", "NOT", "NULL", "IS",
        "LIMIT", "OFFSET", "ASC", "DESC", "GROUP", "HAVING",
        "SMALLINT", "INT2", "SMALLSERIAL", "SERIAL2",
        "INT", "INTEGER", "INT4", "SERIAL", "FLOAT", "FLOAT8", "DOUBLE", "REAL", "TEXT",
        "VARCHAR", "CHAR", "CHARACTER", "BOOLEAN", "BOOL",
        "BIGINT", "INT8", "BIGSERIAL", "NUMERIC", "DECIMAL",
        "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "INTERVAL", "UUID",
        "DISTINCT", "IN", "BETWEEN", "LIKE", "ILIKE",
        "LEFT", "RIGHT", "FULL", "OUTER", "COALESCE", "CASE",
        "WHEN", "THEN", "ELSE", "END", "TRUE", "FALSE",
        "PRIMARY", "KEY", "DEFAULT", "CHECK", "UNIQUE",
        "ALTER", "ADD", "RENAME", "COLUMN", "TO",
        "BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION",
        "INNER", "CROSS", "NATURAL", "USING", "LATERAL",
        "UNION", "INTERSECT", "EXCEPT", "ALL",
        "WITH", "RECURSIVE", "EXISTS",
        "CONFLICT", "DO", "NOTHING",
        "NULLIF", "GREATEST", "LEAST",
        "UPPER", "LOWER", "LENGTH", "SUBSTRING", "TRIM",
        "ANY", "SOME", "ARRAY",
        "ROWS", "RANGE", "UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT",
        "ROLLUP", "CUBE", "GROUPING", "SETS",
        "SEQUENCE", "VIEW", "REPLACE", "START", "INCREMENT",
        "MINVALUE", "MAXVALUE", "REFERENCES", "CASCADE",
        "NEXTVAL", "CURRVAL",
        "CAST",
        "EXTRACT", "CURRENT_TIMESTAMP",
        "TRUNCATE",
        "EXPLAIN", "ANALYZE", "COPY", "STDIN", "STDOUT", "CSV", "HEADER",
        "ABS", "CEIL", "CEILING", "FLOOR", "ROUND", "POWER", "SQRT", "MOD", "SIGN", "RANDOM",
        "LPAD", "RPAD", "CONCAT", "CONCAT_WS", "POSITION", "SPLIT_PART",
        "LEFT", "RIGHT", "REPEAT", "REVERSE", "INITCAP",
        "SHOW", "RESET", "DISCARD", "DEALLOCATE",
        "OPERATOR", "COLLATE",
        "NULLS", "FIRST", "LAST",
        NULL
    };
    for (int i = 0; keywords[i]; i++) {
        if (sv_eq_ignorecase_cstr(word, keywords[i]))
            return 1;
    }
    return 0;
}

static struct token lexer_next(struct lexer *l)
{
    struct token tok = { .type = TOK_EOF, .value = SV_NULL };
    if (!l->input) return tok;
    skip_whitespace(l);

    char c = l->input[l->pos];
    if (c == '\0') return tok;

    if (c == '*') {
        tok.type = TOK_STAR;
        tok.value = sv_from(&l->input[l->pos], 1);
        l->pos++;
        return tok;
    }
    if (c == ',') {
        tok.type = TOK_COMMA;
        tok.value = sv_from(&l->input[l->pos], 1);
        l->pos++;
        return tok;
    }
    if (c == ';') {
        tok.type = TOK_SEMICOLON;
        tok.value = sv_from(&l->input[l->pos], 1);
        l->pos++;
        return tok;
    }
    if (c == '(') {
        tok.type = TOK_LPAREN;
        tok.value = sv_from(&l->input[l->pos], 1);
        l->pos++;
        return tok;
    }
    if (c == ')') {
        tok.type = TOK_RPAREN;
        tok.value = sv_from(&l->input[l->pos], 1);
        l->pos++;
        return tok;
    }
    if (c == '.') {
        tok.type = TOK_DOT;
        tok.value = sv_from(&l->input[l->pos], 1);
        l->pos++;
        return tok;
    }
    if (c == '+') {
        tok.type = TOK_PLUS;
        tok.value = sv_from(&l->input[l->pos], 1);
        l->pos++;
        return tok;
    }
    if (c == '/') {
        tok.type = TOK_SLASH;
        tok.value = sv_from(&l->input[l->pos], 1);
        l->pos++;
        return tok;
    }
    if (c == '%') {
        tok.type = TOK_PERCENT;
        tok.value = sv_from(&l->input[l->pos], 1);
        l->pos++;
        return tok;
    }
    if (c == '|' && l->input[l->pos + 1] == '|') {
        tok.type = TOK_PIPE_PIPE;
        tok.value = sv_from(&l->input[l->pos], 2);
        l->pos += 2;
        return tok;
    }
    if (c == ':' && l->input[l->pos + 1] == ':') {
        tok.type = TOK_DOUBLE_COLON;
        tok.value = sv_from(&l->input[l->pos], 2);
        l->pos += 2;
        return tok;
    }
    if (c == '=' ) {
        tok.type = TOK_EQUALS;
        tok.value = sv_from(&l->input[l->pos], 1);
        l->pos++;
        return tok;
    }
    if (c == '!' && l->input[l->pos + 1] == '~') {
        tok.type = TOK_BANG_TILDE;
        tok.value = sv_from(&l->input[l->pos], 2);
        l->pos += 2;
        return tok;
    }
    if (c == '!' && l->input[l->pos + 1] == '=') {
        tok.type = TOK_NOT_EQUALS;
        tok.value = sv_from(&l->input[l->pos], 2);
        l->pos += 2;
        return tok;
    }
    if (c == '~') {
        tok.type = TOK_TILDE;
        tok.value = sv_from(&l->input[l->pos], 1);
        l->pos++;
        return tok;
    }
    if (c == '<') {
        if (l->input[l->pos + 1] == '=') {
            tok.type = TOK_LESS_EQ;
            tok.value = sv_from(&l->input[l->pos], 2);
            l->pos += 2;
            return tok;
        }
        if (l->input[l->pos + 1] == '>') {
            tok.type = TOK_NOT_EQUALS;
            tok.value = sv_from(&l->input[l->pos], 2);
            l->pos += 2;
            return tok;
        }
        tok.type = TOK_LESS;
        tok.value = sv_from(&l->input[l->pos], 1);
        l->pos++;
        return tok;
    }
    if (c == '>') {
        if (l->input[l->pos + 1] == '=') {
            tok.type = TOK_GREATER_EQ;
            tok.value = sv_from(&l->input[l->pos], 2);
            l->pos += 2;
            return tok;
        }
        tok.type = TOK_GREATER;
        tok.value = sv_from(&l->input[l->pos], 1);
        l->pos++;
        return tok;
    }

    /* quoted string: "..." or '...' with SQL-standard '' escape */
    if (c == '"' || c == '\'') {
        char quote = c;
        l->pos++;
        size_t start = l->pos;
        /* scan to find the end, handling doubled-quote escape */
        int has_escape = 0;
        while (l->input[l->pos]) {
            if (l->input[l->pos] == quote) {
                if (l->input[l->pos + 1] == quote) {
                    /* doubled quote — escaped, skip both */
                    has_escape = 1;
                    l->pos += 2;
                    continue;
                }
                break; /* end of string */
            }
            l->pos++;
        }
        if (!has_escape) {
            /* no escapes — zero-copy path */
            tok.value = sv_from(l->input + start, l->pos - start);
        } else {
            /* collapse doubled quotes into single quotes using scratch buffer */
            static __thread char esc_buf[8192];
            static __thread char *esc_dyn = NULL;
            /* free any previous dynamic allocation */
            if (esc_dyn) { free(esc_dyn); esc_dyn = NULL; }
            size_t raw_len = l->pos - start;
            char *buf = esc_buf;
            size_t buf_cap = sizeof(esc_buf);
            if (raw_len >= buf_cap) {
                esc_dyn = (char *)malloc(raw_len + 1);
                if (esc_dyn) { buf = esc_dyn; buf_cap = raw_len + 1; }
            }
            size_t out = 0;
            for (size_t i = start; i < l->pos && out < buf_cap - 1; i++) {
                buf[out++] = l->input[i];
                if (l->input[i] == quote && l->input[i + 1] == quote)
                    i++; /* skip the second quote */
            }
            tok.value = sv_from(buf, out);
        }
        tok.type = TOK_STRING;
        if (l->input[l->pos] == quote) l->pos++;
        return tok;
    }

    /* minus: as unary negative only at start or after operator/keyword/comma/lparen */
    if (c == '-' && !isdigit((unsigned char)l->input[l->pos + 1])) {
        tok.type = TOK_MINUS;
        tok.value = sv_from(&l->input[l->pos], 1);
        l->pos++;
        return tok;
    }
    if (c == '-' && isdigit((unsigned char)l->input[l->pos + 1])) {
        /* look back to decide: if previous non-space is digit, identifier char, or ')' -> binary minus */
        int binary = 0;
        if (l->pos > 0) {
            size_t bp = l->pos - 1;
            while (bp > 0 && (l->input[bp] == ' ' || l->input[bp] == '\t')) bp--;
            char prev = l->input[bp];
            if (isalnum((unsigned char)prev) || prev == '_' || prev == ')') binary = 1;
        }
        if (binary) {
            tok.type = TOK_MINUS;
            tok.value = sv_from(&l->input[l->pos], 1);
            l->pos++;
            return tok;
        }
    }

    /* number (integer or float) */
    if (isdigit((unsigned char)c) || (c == '-' && isdigit((unsigned char)l->input[l->pos + 1]))) {
        size_t start = l->pos;
        if (c == '-') l->pos++;
        while (isdigit((unsigned char)l->input[l->pos])) l->pos++;
        if (l->input[l->pos] == '.' && isdigit((unsigned char)l->input[l->pos + 1])) {
            l->pos++; /* skip '.' */
            while (isdigit((unsigned char)l->input[l->pos])) l->pos++;
        }
        tok.value = sv_from(l->input + start, l->pos - start);
        tok.type = TOK_NUMBER;
        return tok;
    }

    /* word (keyword or identifier) */
    if (isalpha((unsigned char)c) || c == '_') {
        size_t start = l->pos;
        while (isalnum((unsigned char)l->input[l->pos]) || l->input[l->pos] == '_')
            l->pos++;
        tok.value = sv_from(l->input + start, l->pos - start);
        tok.type = is_keyword(tok.value) ? TOK_KEYWORD : TOK_IDENTIFIER;
        return tok;
    }

    tok.type = TOK_UNKNOWN;
    tok.value = sv_from(&l->input[l->pos], 1);
    l->pos++;
    return tok;
}

static struct token lexer_peek(struct lexer *l)
{
    size_t saved = l->pos;
    struct token tok = lexer_next(l);
    l->pos = saved;
    return tok;
}

/* ---- parser ---- */

/* consume an identifier, skipping over dots (e.g. table.column) */
static sv consume_identifier(struct lexer *l, struct token first)
{
    sv result = first.value;
    for (;;) {
        struct token peek = lexer_peek(l);
        if (peek.type == TOK_DOT) {
            lexer_next(l); /* consume dot */
            struct token col = lexer_next(l);
            /* extend the sv to cover table.column */
            result = sv_from(result.data,
                (size_t)((col.value.data + col.value.len) - result.data));
        } else {
            break;
        }
    }
    return result;
}

static int is_agg_keyword(sv word)
{
    return sv_eq_ignorecase_cstr(word, "SUM")
        || sv_eq_ignorecase_cstr(word, "COUNT")
        || sv_eq_ignorecase_cstr(word, "AVG")
        || sv_eq_ignorecase_cstr(word, "MIN")
        || sv_eq_ignorecase_cstr(word, "MAX")
        || sv_eq_ignorecase_cstr(word, "STRING_AGG")
        || sv_eq_ignorecase_cstr(word, "ARRAY_AGG");
}

static int is_win_keyword(sv word)
{
    return sv_eq_ignorecase_cstr(word, "ROW_NUMBER")
        || sv_eq_ignorecase_cstr(word, "RANK")
        || sv_eq_ignorecase_cstr(word, "DENSE_RANK")
        || sv_eq_ignorecase_cstr(word, "NTILE")
        || sv_eq_ignorecase_cstr(word, "PERCENT_RANK")
        || sv_eq_ignorecase_cstr(word, "CUME_DIST")
        || sv_eq_ignorecase_cstr(word, "LAG")
        || sv_eq_ignorecase_cstr(word, "LEAD")
        || sv_eq_ignorecase_cstr(word, "FIRST_VALUE")
        || sv_eq_ignorecase_cstr(word, "LAST_VALUE")
        || sv_eq_ignorecase_cstr(word, "NTH_VALUE")
        || is_agg_keyword(word);
}

static int is_win_only_keyword(sv word)
{
    return sv_eq_ignorecase_cstr(word, "ROW_NUMBER")
        || sv_eq_ignorecase_cstr(word, "RANK")
        || sv_eq_ignorecase_cstr(word, "DENSE_RANK")
        || sv_eq_ignorecase_cstr(word, "NTILE")
        || sv_eq_ignorecase_cstr(word, "PERCENT_RANK")
        || sv_eq_ignorecase_cstr(word, "CUME_DIST")
        || sv_eq_ignorecase_cstr(word, "LAG")
        || sv_eq_ignorecase_cstr(word, "LEAD")
        || sv_eq_ignorecase_cstr(word, "FIRST_VALUE")
        || sv_eq_ignorecase_cstr(word, "LAST_VALUE")
        || sv_eq_ignorecase_cstr(word, "NTH_VALUE");
}

static enum agg_func agg_from_keyword(sv word)
{
    if (sv_eq_ignorecase_cstr(word, "SUM"))        return AGG_SUM;
    if (sv_eq_ignorecase_cstr(word, "COUNT"))      return AGG_COUNT;
    if (sv_eq_ignorecase_cstr(word, "AVG"))        return AGG_AVG;
    if (sv_eq_ignorecase_cstr(word, "MIN"))        return AGG_MIN;
    if (sv_eq_ignorecase_cstr(word, "MAX"))        return AGG_MAX;
    if (sv_eq_ignorecase_cstr(word, "STRING_AGG")) return AGG_STRING_AGG;
    if (sv_eq_ignorecase_cstr(word, "ARRAY_AGG"))  return AGG_ARRAY_AGG;
    return AGG_NONE;
}

static enum win_func win_from_keyword(sv word)
{
    if (sv_eq_ignorecase_cstr(word, "ROW_NUMBER"))  return WIN_ROW_NUMBER;
    if (sv_eq_ignorecase_cstr(word, "RANK"))        return WIN_RANK;
    if (sv_eq_ignorecase_cstr(word, "DENSE_RANK"))  return WIN_DENSE_RANK;
    if (sv_eq_ignorecase_cstr(word, "NTILE"))       return WIN_NTILE;
    if (sv_eq_ignorecase_cstr(word, "PERCENT_RANK")) return WIN_PERCENT_RANK;
    if (sv_eq_ignorecase_cstr(word, "CUME_DIST"))   return WIN_CUME_DIST;
    if (sv_eq_ignorecase_cstr(word, "LAG"))         return WIN_LAG;
    if (sv_eq_ignorecase_cstr(word, "LEAD"))        return WIN_LEAD;
    if (sv_eq_ignorecase_cstr(word, "FIRST_VALUE")) return WIN_FIRST_VALUE;
    if (sv_eq_ignorecase_cstr(word, "LAST_VALUE"))  return WIN_LAST_VALUE;
    if (sv_eq_ignorecase_cstr(word, "NTH_VALUE"))   return WIN_NTH_VALUE;
    if (sv_eq_ignorecase_cstr(word, "SUM"))         return WIN_SUM;
    if (sv_eq_ignorecase_cstr(word, "COUNT"))       return WIN_COUNT;
    if (sv_eq_ignorecase_cstr(word, "AVG"))         return WIN_AVG;
    return WIN_ROW_NUMBER;
}

/* parse OVER (PARTITION BY col ORDER BY col) */
static int parse_over_clause(struct lexer *l, struct query_arena *a, struct win_expr *w)
{
    struct token tok = lexer_next(l); /* consume OVER */
    tok = lexer_next(l); /* ( */
    if (tok.type != TOK_LPAREN) {
        arena_set_error(a, "42601", "expected '(' after OVER");
        return -1;
    }

    w->has_partition = 0;
    w->has_order = 0;

    for (;;) {
        struct token peek = lexer_peek(l);
        if (peek.type == TOK_RPAREN) {
            lexer_next(l);
            break;
        }
        if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "PARTITION")) {
            lexer_next(l); /* PARTITION */
            tok = lexer_next(l); /* BY */
            if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "BY")) {
                arena_set_error(a, "42601", "expected BY after PARTITION");
                return -1;
            }
            tok = lexer_next(l);
            if (tok.type != TOK_IDENTIFIER && tok.type != TOK_KEYWORD) {
                arena_set_error(a, "42601", "expected column after PARTITION BY");
                return -1;
            }
            w->has_partition = 1;
            w->partition_col = consume_identifier(l, tok);
        } else if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "ORDER")) {
            lexer_next(l); /* ORDER */
            tok = lexer_next(l); /* BY */
            if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "BY")) {
                arena_set_error(a, "42601", "expected BY after ORDER");
                return -1;
            }
            tok = lexer_next(l);
            if (tok.type != TOK_IDENTIFIER && tok.type != TOK_KEYWORD) {
                arena_set_error(a, "42601", "expected column after ORDER BY");
                return -1;
            }
            w->has_order = 1;
            w->order_col = consume_identifier(l, tok);
            /* if the order expression is a function call like SUM(d.amount),
             * consume the parenthesized arguments so we don't choke on them */
            peek = lexer_peek(l);
            if (peek.type == TOK_LPAREN) {
                /* extend order_col to cover the whole expression text */
                const char *expr_start = w->order_col.data;
                int depth = 0;
                for (;;) {
                    struct token t2 = lexer_peek(l);
                    if (t2.type == TOK_LPAREN) { depth++; lexer_next(l); }
                    else if (t2.type == TOK_RPAREN) {
                        if (depth <= 1) { lexer_next(l);
                            w->order_col = sv_from(expr_start,
                                (size_t)((t2.value.data + t2.value.len) - expr_start));
                            break;
                        }
                        depth--; lexer_next(l);
                    }
                    else if (t2.type == TOK_EOF) break;
                    else lexer_next(l);
                }
            }
            /* consume optional ASC/DESC */
            peek = lexer_peek(l);
            if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "DESC")) {
                lexer_next(l);
                w->order_desc = 1;
            } else if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "ASC")) {
                lexer_next(l);
                w->order_desc = 0;
            }
        } else if (peek.type == TOK_KEYWORD &&
                   (sv_eq_ignorecase_cstr(peek.value, "ROWS") ||
                    sv_eq_ignorecase_cstr(peek.value, "RANGE"))) {
            lexer_next(l); /* consume ROWS or RANGE */
            tok = lexer_next(l); /* BETWEEN */
            if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "BETWEEN")) {
                arena_set_error(a, "42601", "expected BETWEEN after ROWS/RANGE");
                return -1;
            }
            w->has_frame = 1;
            /* parse frame start */
            tok = lexer_next(l);
            if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "UNBOUNDED")) {
                lexer_next(l); /* PRECEDING */
                w->frame_start = FRAME_UNBOUNDED_PRECEDING;
            } else if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "CURRENT")) {
                lexer_next(l); /* ROW */
                w->frame_start = FRAME_CURRENT_ROW;
            } else if (tok.type == TOK_NUMBER) {
                long long v = 0;
                for (size_t k = 0; k < tok.value.len; k++)
                    v = v * 10 + (tok.value.data[k] - '0');
                w->frame_start_n = (int)v;
                tok = lexer_next(l); /* PRECEDING or FOLLOWING */
                if (sv_eq_ignorecase_cstr(tok.value, "PRECEDING"))
                    w->frame_start = FRAME_N_PRECEDING;
                else
                    w->frame_start = FRAME_N_FOLLOWING;
            }
            /* AND */
            tok = lexer_next(l);
            /* parse frame end */
            tok = lexer_next(l);
            if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "UNBOUNDED")) {
                lexer_next(l); /* FOLLOWING */
                w->frame_end = FRAME_UNBOUNDED_FOLLOWING;
            } else if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "CURRENT")) {
                lexer_next(l); /* ROW */
                w->frame_end = FRAME_CURRENT_ROW;
            } else if (tok.type == TOK_NUMBER) {
                long long v = 0;
                for (size_t k = 0; k < tok.value.len; k++)
                    v = v * 10 + (tok.value.data[k] - '0');
                w->frame_end_n = (int)v;
                tok = lexer_next(l); /* PRECEDING or FOLLOWING */
                if (sv_eq_ignorecase_cstr(tok.value, "FOLLOWING"))
                    w->frame_end = FRAME_N_FOLLOWING;
                else
                    w->frame_end = FRAME_N_PRECEDING;
            }
        } else {
            arena_set_error(a, "42601", "unexpected token in OVER clause");
            return -1;
        }
    }
    return 0;
}

/* ---- WHERE condition parsing ---- */

static int is_cmp_token(enum token_type t)
{
    return t == TOK_EQUALS || t == TOK_NOT_EQUALS ||
           t == TOK_LESS || t == TOK_GREATER ||
           t == TOK_LESS_EQ || t == TOK_GREATER_EQ ||
           t == TOK_TILDE || t == TOK_BANG_TILDE;
}

static enum cmp_op cmp_from_token(enum token_type t)
{
    switch (t) {
        case TOK_EQUALS:     return CMP_EQ;
        case TOK_NOT_EQUALS: return CMP_NE;
        case TOK_LESS:       return CMP_LT;
        case TOK_GREATER:    return CMP_GT;
        case TOK_LESS_EQ:    return CMP_LE;
        case TOK_GREATER_EQ: return CMP_GE;
        case TOK_TILDE:      return CMP_REGEX_MATCH;
        case TOK_BANG_TILDE: return CMP_REGEX_NOT_MATCH;
        case TOK_KEYWORD:
        case TOK_IDENTIFIER:
        case TOK_STRING:
        case TOK_STAR:
        case TOK_COMMA:
        case TOK_SEMICOLON:
        case TOK_NUMBER:
        case TOK_LPAREN:
        case TOK_RPAREN:
        case TOK_DOT:
        case TOK_PLUS:
        case TOK_SLASH:
        case TOK_MINUS:
        case TOK_PERCENT:
        case TOK_PIPE_PIPE:
        case TOK_DOUBLE_COLON:
        case TOK_EOF:
        case TOK_UNKNOWN:
            return CMP_EQ;
    }
    return CMP_EQ; /* unreachable — silences -Wreturn-type */
}

/* ---------------------------------------------------------------------------
 * Expression AST parser — recursive descent with operator precedence.
 *
 * Precedence (lowest to highest):
 *   ||              (string concatenation)
 *   +  -            (additive)
 *   *  /  %         (multiplicative)
 *   unary -         (negation)
 *   atoms           (literals, columns, functions, CASE, subqueries, parens)
 * ------------------------------------------------------------------------- */

/* forward declarations for mutual recursion with condition parser */
static uint32_t parse_or_cond(struct lexer *l, struct query_arena *a);

static uint32_t parse_expr(struct lexer *l, struct query_arena *a);

static uint32_t expr_alloc(struct query_arena *a, enum expr_type type)
{
    uint32_t idx = arena_alloc_expr(a);
    EXPR(a, idx).type = type;
    return idx;
}

static int is_expr_func_keyword(sv name)
{
    return sv_eq_ignorecase_cstr(name, "COALESCE") ||
           sv_eq_ignorecase_cstr(name, "NULLIF") ||
           sv_eq_ignorecase_cstr(name, "GREATEST") ||
           sv_eq_ignorecase_cstr(name, "LEAST") ||
           sv_eq_ignorecase_cstr(name, "UPPER") ||
           sv_eq_ignorecase_cstr(name, "LOWER") ||
           sv_eq_ignorecase_cstr(name, "LENGTH") ||
           sv_eq_ignorecase_cstr(name, "TRIM") ||
           sv_eq_ignorecase_cstr(name, "SUBSTRING") ||
           sv_eq_ignorecase_cstr(name, "NEXTVAL") ||
           sv_eq_ignorecase_cstr(name, "CURRVAL") ||
           sv_eq_ignorecase_cstr(name, "GEN_RANDOM_UUID") ||
           sv_eq_ignorecase_cstr(name, "NOW") ||
           sv_eq_ignorecase_cstr(name, "DATE_TRUNC") ||
           sv_eq_ignorecase_cstr(name, "DATE_PART") ||
           sv_eq_ignorecase_cstr(name, "AGE") ||
           sv_eq_ignorecase_cstr(name, "TO_CHAR") ||
           sv_eq_ignorecase_cstr(name, "ABS") ||
           sv_eq_ignorecase_cstr(name, "CEIL") ||
           sv_eq_ignorecase_cstr(name, "CEILING") ||
           sv_eq_ignorecase_cstr(name, "FLOOR") ||
           sv_eq_ignorecase_cstr(name, "ROUND") ||
           sv_eq_ignorecase_cstr(name, "POWER") ||
           sv_eq_ignorecase_cstr(name, "SQRT") ||
           sv_eq_ignorecase_cstr(name, "MOD") ||
           sv_eq_ignorecase_cstr(name, "SIGN") ||
           sv_eq_ignorecase_cstr(name, "RANDOM") ||
           sv_eq_ignorecase_cstr(name, "REPLACE") ||
           sv_eq_ignorecase_cstr(name, "LPAD") ||
           sv_eq_ignorecase_cstr(name, "RPAD") ||
           sv_eq_ignorecase_cstr(name, "CONCAT") ||
           sv_eq_ignorecase_cstr(name, "CONCAT_WS") ||
           sv_eq_ignorecase_cstr(name, "POSITION") ||
           sv_eq_ignorecase_cstr(name, "SPLIT_PART") ||
           sv_eq_ignorecase_cstr(name, "LEFT") ||
           sv_eq_ignorecase_cstr(name, "RIGHT") ||
           sv_eq_ignorecase_cstr(name, "REPEAT") ||
           sv_eq_ignorecase_cstr(name, "REVERSE") ||
           sv_eq_ignorecase_cstr(name, "INITCAP") ||
           sv_eq_ignorecase_cstr(name, "pg_get_userbyid") ||
           sv_eq_ignorecase_cstr(name, "pg_table_is_visible") ||
           sv_eq_ignorecase_cstr(name, "format_type") ||
           sv_eq_ignorecase_cstr(name, "pg_get_expr") ||
           sv_eq_ignorecase_cstr(name, "obj_description") ||
           sv_eq_ignorecase_cstr(name, "col_description") ||
           sv_eq_ignorecase_cstr(name, "pg_encoding_to_char") ||
           sv_eq_ignorecase_cstr(name, "shobj_description") ||
           sv_eq_ignorecase_cstr(name, "has_table_privilege") ||
           sv_eq_ignorecase_cstr(name, "has_database_privilege") ||
           sv_eq_ignorecase_cstr(name, "pg_get_constraintdef") ||
           sv_eq_ignorecase_cstr(name, "pg_get_indexdef") ||
           sv_eq_ignorecase_cstr(name, "array_to_string") ||
           sv_eq_ignorecase_cstr(name, "current_schema") ||
           sv_eq_ignorecase_cstr(name, "current_schemas") ||
           sv_eq_ignorecase_cstr(name, "pg_is_in_recovery");
}

static enum expr_func expr_func_from_name(sv name)
{
    if (sv_eq_ignorecase_cstr(name, "COALESCE"))  return FUNC_COALESCE;
    if (sv_eq_ignorecase_cstr(name, "NULLIF"))    return FUNC_NULLIF;
    if (sv_eq_ignorecase_cstr(name, "GREATEST"))  return FUNC_GREATEST;
    if (sv_eq_ignorecase_cstr(name, "LEAST"))     return FUNC_LEAST;
    if (sv_eq_ignorecase_cstr(name, "UPPER"))     return FUNC_UPPER;
    if (sv_eq_ignorecase_cstr(name, "LOWER"))     return FUNC_LOWER;
    if (sv_eq_ignorecase_cstr(name, "LENGTH"))    return FUNC_LENGTH;
    if (sv_eq_ignorecase_cstr(name, "TRIM"))      return FUNC_TRIM;
    if (sv_eq_ignorecase_cstr(name, "SUBSTRING")) return FUNC_SUBSTRING;
    if (sv_eq_ignorecase_cstr(name, "NEXTVAL"))   return FUNC_NEXTVAL;
    if (sv_eq_ignorecase_cstr(name, "CURRVAL"))   return FUNC_CURRVAL;
    if (sv_eq_ignorecase_cstr(name, "GEN_RANDOM_UUID")) return FUNC_GEN_RANDOM_UUID;
    if (sv_eq_ignorecase_cstr(name, "NOW"))            return FUNC_NOW;
    if (sv_eq_ignorecase_cstr(name, "DATE_TRUNC"))     return FUNC_DATE_TRUNC;
    if (sv_eq_ignorecase_cstr(name, "DATE_PART"))      return FUNC_DATE_PART;
    if (sv_eq_ignorecase_cstr(name, "AGE"))            return FUNC_AGE;
    if (sv_eq_ignorecase_cstr(name, "TO_CHAR"))        return FUNC_TO_CHAR;
    if (sv_eq_ignorecase_cstr(name, "ABS"))             return FUNC_ABS;
    if (sv_eq_ignorecase_cstr(name, "CEIL"))            return FUNC_CEIL;
    if (sv_eq_ignorecase_cstr(name, "CEILING"))         return FUNC_CEIL;
    if (sv_eq_ignorecase_cstr(name, "FLOOR"))           return FUNC_FLOOR;
    if (sv_eq_ignorecase_cstr(name, "ROUND"))           return FUNC_ROUND;
    if (sv_eq_ignorecase_cstr(name, "POWER"))           return FUNC_POWER;
    if (sv_eq_ignorecase_cstr(name, "SQRT"))            return FUNC_SQRT;
    if (sv_eq_ignorecase_cstr(name, "MOD"))             return FUNC_MOD;
    if (sv_eq_ignorecase_cstr(name, "SIGN"))            return FUNC_SIGN;
    if (sv_eq_ignorecase_cstr(name, "RANDOM"))          return FUNC_RANDOM;
    if (sv_eq_ignorecase_cstr(name, "REPLACE"))         return FUNC_REPLACE;
    if (sv_eq_ignorecase_cstr(name, "LPAD"))            return FUNC_LPAD;
    if (sv_eq_ignorecase_cstr(name, "RPAD"))            return FUNC_RPAD;
    if (sv_eq_ignorecase_cstr(name, "CONCAT"))          return FUNC_CONCAT;
    if (sv_eq_ignorecase_cstr(name, "CONCAT_WS"))       return FUNC_CONCAT_WS;
    if (sv_eq_ignorecase_cstr(name, "POSITION"))        return FUNC_POSITION;
    if (sv_eq_ignorecase_cstr(name, "SPLIT_PART"))      return FUNC_SPLIT_PART;
    if (sv_eq_ignorecase_cstr(name, "LEFT"))            return FUNC_LEFT;
    if (sv_eq_ignorecase_cstr(name, "RIGHT"))           return FUNC_RIGHT;
    if (sv_eq_ignorecase_cstr(name, "REPEAT"))          return FUNC_REPEAT;
    if (sv_eq_ignorecase_cstr(name, "REVERSE"))         return FUNC_REVERSE;
    if (sv_eq_ignorecase_cstr(name, "INITCAP"))         return FUNC_INITCAP;
    if (sv_eq_ignorecase_cstr(name, "pg_get_userbyid"))  return FUNC_PG_GET_USERBYID;
    if (sv_eq_ignorecase_cstr(name, "pg_table_is_visible")) return FUNC_PG_TABLE_IS_VISIBLE;
    if (sv_eq_ignorecase_cstr(name, "format_type"))      return FUNC_FORMAT_TYPE;
    if (sv_eq_ignorecase_cstr(name, "pg_get_expr"))      return FUNC_PG_GET_EXPR;
    if (sv_eq_ignorecase_cstr(name, "obj_description"))  return FUNC_OBJ_DESCRIPTION;
    if (sv_eq_ignorecase_cstr(name, "col_description"))  return FUNC_COL_DESCRIPTION;
    if (sv_eq_ignorecase_cstr(name, "pg_encoding_to_char")) return FUNC_PG_ENCODING_TO_CHAR;
    if (sv_eq_ignorecase_cstr(name, "shobj_description")) return FUNC_SHOBJ_DESCRIPTION;
    if (sv_eq_ignorecase_cstr(name, "has_table_privilege")) return FUNC_HAS_TABLE_PRIVILEGE;
    if (sv_eq_ignorecase_cstr(name, "has_database_privilege")) return FUNC_HAS_DATABASE_PRIVILEGE;
    if (sv_eq_ignorecase_cstr(name, "pg_get_constraintdef")) return FUNC_PG_GET_CONSTRAINTDEF;
    if (sv_eq_ignorecase_cstr(name, "pg_get_indexdef"))  return FUNC_PG_GET_INDEXDEF;
    if (sv_eq_ignorecase_cstr(name, "array_to_string"))  return FUNC_ARRAY_TO_STRING;
    if (sv_eq_ignorecase_cstr(name, "current_schema"))   return FUNC_CURRENT_SCHEMA;
    if (sv_eq_ignorecase_cstr(name, "current_schemas"))  return FUNC_CURRENT_SCHEMAS;
    if (sv_eq_ignorecase_cstr(name, "pg_is_in_recovery")) return FUNC_PG_IS_IN_RECOVERY;
    return FUNC_COALESCE; /* fallback, should not happen */
}

/* SQL structural keywords that terminate an expression — these must never be
 * consumed as column references by the expression parser. */
static int is_expr_terminator_keyword(sv name)
{
    return sv_eq_ignorecase_cstr(name, "FROM") ||
           sv_eq_ignorecase_cstr(name, "WHERE") ||
           sv_eq_ignorecase_cstr(name, "SET") ||
           sv_eq_ignorecase_cstr(name, "ORDER") ||
           sv_eq_ignorecase_cstr(name, "GROUP") ||
           sv_eq_ignorecase_cstr(name, "HAVING") ||
           sv_eq_ignorecase_cstr(name, "LIMIT") ||
           sv_eq_ignorecase_cstr(name, "OFFSET") ||
           sv_eq_ignorecase_cstr(name, "JOIN") ||
           sv_eq_ignorecase_cstr(name, "INNER") ||
           sv_eq_ignorecase_cstr(name, "LEFT") ||
           sv_eq_ignorecase_cstr(name, "RIGHT") ||
           sv_eq_ignorecase_cstr(name, "FULL") ||
           sv_eq_ignorecase_cstr(name, "CROSS") ||
           sv_eq_ignorecase_cstr(name, "NATURAL") ||
           sv_eq_ignorecase_cstr(name, "LATERAL") ||
           sv_eq_ignorecase_cstr(name, "ON") ||
           sv_eq_ignorecase_cstr(name, "USING") ||
           sv_eq_ignorecase_cstr(name, "AND") ||
           sv_eq_ignorecase_cstr(name, "OR") ||
           sv_eq_ignorecase_cstr(name, "NOT") ||
           sv_eq_ignorecase_cstr(name, "IN") ||
           sv_eq_ignorecase_cstr(name, "IS") ||
           sv_eq_ignorecase_cstr(name, "BETWEEN") ||
           sv_eq_ignorecase_cstr(name, "LIKE") ||
           sv_eq_ignorecase_cstr(name, "ILIKE") ||
           sv_eq_ignorecase_cstr(name, "EXISTS") ||
           sv_eq_ignorecase_cstr(name, "ANY") ||
           sv_eq_ignorecase_cstr(name, "SOME") ||
           sv_eq_ignorecase_cstr(name, "ALL") ||
           sv_eq_ignorecase_cstr(name, "AS") ||
           sv_eq_ignorecase_cstr(name, "UNION") ||
           sv_eq_ignorecase_cstr(name, "INTERSECT") ||
           sv_eq_ignorecase_cstr(name, "EXCEPT") ||
           sv_eq_ignorecase_cstr(name, "RETURNING") ||
           sv_eq_ignorecase_cstr(name, "INTO") ||
           sv_eq_ignorecase_cstr(name, "VALUES") ||
           sv_eq_ignorecase_cstr(name, "INSERT") ||
           sv_eq_ignorecase_cstr(name, "UPDATE") ||
           sv_eq_ignorecase_cstr(name, "DELETE") ||
           sv_eq_ignorecase_cstr(name, "SELECT") ||
           sv_eq_ignorecase_cstr(name, "CREATE") ||
           sv_eq_ignorecase_cstr(name, "DROP") ||
           sv_eq_ignorecase_cstr(name, "ALTER") ||
           sv_eq_ignorecase_cstr(name, "BEGIN") ||
           sv_eq_ignorecase_cstr(name, "COMMIT") ||
           sv_eq_ignorecase_cstr(name, "ROLLBACK") ||
           sv_eq_ignorecase_cstr(name, "THEN") ||
           sv_eq_ignorecase_cstr(name, "ELSE") ||
           sv_eq_ignorecase_cstr(name, "END") ||
           sv_eq_ignorecase_cstr(name, "WHEN") ||
           sv_eq_ignorecase_cstr(name, "ASC") ||
           sv_eq_ignorecase_cstr(name, "DESC") ||
           sv_eq_ignorecase_cstr(name, "DISTINCT") ||
           sv_eq_ignorecase_cstr(name, "WITH") ||
           sv_eq_ignorecase_cstr(name, "RECURSIVE");
}

/* forward declaration — defined later in the file */
static enum column_type parse_column_type(sv type_name);

/* parse a SQL type name token and return the column_type */
static enum column_type parse_cast_type_name(struct lexer *l)
{
    struct token tok = lexer_next(l);
    /* Handle schema-qualified type: pg_catalog.regtype → skip schema */
    struct token dot_peek = lexer_peek(l);
    if (dot_peek.type == TOK_DOT) {
        lexer_next(l); /* consume . */
        tok = lexer_next(l); /* actual type name */
    }
    enum column_type ct = parse_column_type(tok.value);
    /* Handle multi-word type names:
     *   TIMESTAMP WITH TIME ZONE / TIMESTAMP WITHOUT TIME ZONE
     *   TIME WITH TIME ZONE / TIME WITHOUT TIME ZONE
     *   CHARACTER VARYING
     *   DOUBLE PRECISION
     * Note: WITHOUT, ZONE, VARYING, PRECISION may be TOK_IDENTIFIER (not in keyword list) */
    struct token peek = lexer_peek(l);
    if ((peek.type == TOK_KEYWORD || peek.type == TOK_IDENTIFIER) &&
        (ct == COLUMN_TYPE_TIMESTAMP || ct == COLUMN_TYPE_TIME)) {
        if (sv_eq_ignorecase_cstr(peek.value, "WITH") ||
            sv_eq_ignorecase_cstr(peek.value, "WITHOUT")) {
            int with_tz = sv_eq_ignorecase_cstr(peek.value, "WITH");
            lexer_next(l); /* consume WITH/WITHOUT */
            struct token t2 = lexer_peek(l);
            if ((t2.type == TOK_KEYWORD || t2.type == TOK_IDENTIFIER) &&
                sv_eq_ignorecase_cstr(t2.value, "TIME")) {
                lexer_next(l); /* consume TIME */
                struct token t3 = lexer_peek(l);
                if ((t3.type == TOK_KEYWORD || t3.type == TOK_IDENTIFIER) &&
                    sv_eq_ignorecase_cstr(t3.value, "ZONE")) {
                    lexer_next(l); /* consume ZONE */
                    if (with_tz && ct == COLUMN_TYPE_TIMESTAMP)
                        ct = COLUMN_TYPE_TIMESTAMPTZ;
                }
            }
        }
    } else if ((peek.type == TOK_KEYWORD || peek.type == TOK_IDENTIFIER) &&
               ct == COLUMN_TYPE_TEXT && sv_eq_ignorecase_cstr(peek.value, "VARYING")) {
        lexer_next(l); /* consume VARYING — CHARACTER VARYING → TEXT */
    } else if ((peek.type == TOK_KEYWORD || peek.type == TOK_IDENTIFIER) &&
               ct == COLUMN_TYPE_FLOAT && sv_eq_ignorecase_cstr(peek.value, "PRECISION")) {
        lexer_next(l); /* consume PRECISION — DOUBLE PRECISION → FLOAT */
    }
    /* skip optional (n) or (p,s) after type name */
    peek = lexer_peek(l);
    if (peek.type == TOK_LPAREN) {
        lexer_next(l); /* consume ( */
        for (;;) {
            struct token t = lexer_next(l);
            if (t.type == TOK_RPAREN || t.type == TOK_EOF) break;
        }
    }
    return ct;
}

/* --- helpers extracted from parse_expr_atom --- */

/* CAST(expr AS type) — assumes CAST keyword already peeked */
static uint32_t parse_cast_expr(struct lexer *l, struct query_arena *a)
{
    lexer_next(l); /* consume CAST */
    struct token lp = lexer_next(l); /* consume ( */
    if (lp.type != TOK_LPAREN) {
        arena_set_error(a, "42601", "expected '(' after CAST");
        return IDX_NONE;
    }
    uint32_t operand = parse_expr(l, a);
    struct token as_tok = lexer_next(l); /* consume AS */
    if (as_tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(as_tok.value, "AS")) {
        arena_set_error(a, "42601", "expected AS in CAST");
        return IDX_NONE;
    }
    enum column_type target = parse_cast_type_name(l);
    struct token rp = lexer_next(l); /* consume ) */
    if (rp.type != TOK_RPAREN) {
        arena_set_error(a, "42601", "expected ')' after CAST type");
    }
    uint32_t ei = expr_alloc(a, EXPR_CAST);
    EXPR(a, ei).cast.operand = operand;
    EXPR(a, ei).cast.target = target;
    return ei;
}

/* EXTRACT(field FROM expr) — assumes EXTRACT keyword already peeked */
static uint32_t parse_extract_expr(struct lexer *l, struct query_arena *a)
{
    lexer_next(l); /* consume EXTRACT */
    struct token lp = lexer_next(l); /* consume ( */
    if (lp.type != TOK_LPAREN) {
        arena_set_error(a, "42601", "expected '(' after EXTRACT");
        return IDX_NONE;
    }
    /* parse field name as a string literal argument */
    struct token field_tok = lexer_next(l);
    uint32_t field_ei = expr_alloc(a, EXPR_LITERAL);
    EXPR(a, field_ei).literal.type = COLUMN_TYPE_TEXT;
    /* lowercase the field name */
    char fbuf[64];
    size_t flen = field_tok.value.len < 63 ? field_tok.value.len : 63;
    for (size_t fi = 0; fi < flen; fi++)
        fbuf[fi] = tolower((unsigned char)field_tok.value.data[fi]);
    fbuf[flen] = '\0';
    EXPR(a, field_ei).literal.value.as_text = bump_strndup(&a->bump, fbuf, flen);
    /* consume FROM */
    struct token from_tok = lexer_next(l);
    if (from_tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(from_tok.value, "FROM")) {
        arena_set_error(a, "42601", "expected FROM in EXTRACT");
        return IDX_NONE;
    }
    uint32_t src_expr = parse_expr(l, a);
    struct token rp = lexer_next(l); /* consume ) */
    if (rp.type != TOK_RPAREN) {
        arena_set_error(a, "42601", "expected ')' after EXTRACT");
    }
    uint32_t ei = expr_alloc(a, EXPR_FUNC_CALL);
    EXPR(a, ei).func_call.func = FUNC_EXTRACT;
    uint32_t args_start = (uint32_t)a->arg_indices.count;
    da_push(&a->arg_indices, field_ei);
    da_push(&a->arg_indices, src_expr);
    EXPR(a, ei).func_call.args_start = args_start;
    EXPR(a, ei).func_call.args_count = 2;
    return ei;
}

/* EXISTS / NOT EXISTS subquery expression.
 * negate=0 for EXISTS, negate=1 for NOT EXISTS.
 * Caller has already consumed EXISTS (and NOT for the negated case).
 * Consumes '(' ... ')' containing the subquery. */
static uint32_t parse_exists_expr(struct lexer *l, struct query_arena *a, int negate)
{
    struct token lp = lexer_next(l); /* consume ( */
    if (lp.type != TOK_LPAREN) {
        arena_set_error(a, "42601", negate
            ? "expected '(' after NOT EXISTS"
            : "expected '(' after EXISTS");
        return IDX_NONE;
    }
    const char *sq_start = l->input + l->pos;
    int depth = 1;
    struct token st;
    while (depth > 0) {
        st = lexer_next(l);
        if (st.type == TOK_LPAREN) depth++;
        else if (st.type == TOK_RPAREN) depth--;
        else if (st.type == TOK_EOF) {
            arena_set_error(a, "42601", negate
                ? "unterminated subquery in NOT EXISTS"
                : "unterminated subquery in EXISTS");
            return IDX_NONE;
        }
    }
    const char *sq_end = st.value.data;
    while (sq_end > sq_start && (sq_end[-1] == ' ' || sq_end[-1] == '\n')) sq_end--;
    size_t sq_len = (size_t)(sq_end - sq_start);
    uint32_t ei = expr_alloc(a, EXPR_EXISTS);
    EXPR(a, ei).exists.sql_idx = arena_store_string(a, sq_start, sq_len);
    EXPR(a, ei).exists.negate = negate;
    return ei;
}

/* CASE WHEN ... THEN ... [ELSE ...] END — assumes CASE keyword already peeked */
static uint32_t parse_case_expr(struct lexer *l, struct query_arena *a)
{
    lexer_next(l); /* consume CASE */
    uint32_t ei = expr_alloc(a, EXPR_CASE_WHEN);
    uint32_t branches_start = (uint32_t)a->branches.count;
    uint32_t branches_count = 0;
    EXPR(a, ei).case_when.else_expr = IDX_NONE;
    EXPR(a, ei).case_when.operand = IDX_NONE;

    /* Check for simple CASE: CASE expr WHEN val THEN ...
     * vs searched CASE: CASE WHEN cond THEN ... */
    struct token peek = lexer_peek(l);
    int is_simple = 0;
    if (!(peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "WHEN"))) {
        /* simple CASE — parse the operand expression */
        EXPR(a, ei).case_when.operand = parse_expr(l, a);
        is_simple = 1;
    }

    for (;;) {
        struct token tok = lexer_peek(l);
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "WHEN")) {
            lexer_next(l); /* consume WHEN */
            uint32_t cond_idx;
            if (is_simple) {
                /* simple CASE: WHEN val — synthesize a COND_COMPARE with lhs_expr */
                uint32_t val_expr = parse_expr(l, a);
                cond_idx = (uint32_t)a->conditions.count;
                struct condition c = {0};
                memset(&c, 0, sizeof(c));
                c.type = COND_COMPARE;
                c.op = CMP_EQ;
                c.lhs_expr = EXPR(a, ei).case_when.operand;
                c.subquery_sql = IDX_NONE;
                c.scalar_subquery_sql = IDX_NONE;
                c.left = IDX_NONE;
                c.right = IDX_NONE;
                /* evaluate the WHEN value expression to get a literal cell */
                struct expr *ve = &EXPR(a, val_expr);
                if (ve->type == EXPR_LITERAL) {
                    c.value = ve->literal;
                    /* for text cells, strdup so the condition owns the text */
                    if (c.value.type == COLUMN_TYPE_TEXT && c.value.value.as_text && !c.value.is_null)
                        c.value.value.as_text = bump_strdup(&a->bump, c.value.value.as_text);
                } else {
                    /* non-literal WHEN value: store as rhs expression via a trick:
                     * use rhs_column as empty and set scalar_subquery_sql to IDX_NONE.
                     * Actually, let's just store the val_expr in lhs_expr and
                     * handle it differently. For now, store as text literal. */
                    c.value.type = COLUMN_TYPE_TEXT;
                    c.value.is_null = 1; /* will be evaluated at runtime */
                }
                da_push(&a->conditions, c);
            } else {
                cond_idx = parse_or_cond(l, a);
            }
            tok = lexer_next(l); /* consume THEN */
            if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "THEN")) {
                arena_set_error(a, "42601", "expected THEN in CASE");
                return ei;
            }
            uint32_t then_idx = parse_expr(l, a);
            struct case_when_branch branch = { .cond_idx = cond_idx, .then_expr_idx = then_idx };
            arena_push_branch(a, branch);
            branches_count++;
        } else if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "ELSE")) {
            lexer_next(l); /* consume ELSE */
            EXPR(a, ei).case_when.else_expr = parse_expr(l, a);
        } else if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "END")) {
            lexer_next(l); /* consume END */
            break;
        } else {
            arena_set_error(a, "42601", "unexpected token in CASE expression");
            break;
        }
    }
    EXPR(a, ei).case_when.branches_start = branches_start;
    EXPR(a, ei).case_when.branches_count = branches_count;
    return ei;
}

/* FUNC(args...) — assumes func name token is in *func_tok and '(' already peeked.
 * Caller has consumed the func name and the '('. */
static uint32_t parse_func_call_expr(struct lexer *l, struct query_arena *a,
                                     struct token func_tok)
{
    /* Parse all arguments first, collecting their root indices.
     * Nested parse_expr calls may interleave allocations, so the
     * arg root exprs are NOT consecutive in the arena. */
    uint32_t arg_indices[16];
    uint32_t args_count = 0;
    int is_position = sv_eq_ignorecase_cstr(func_tok.value, "POSITION");
    int is_substring = sv_eq_ignorecase_cstr(func_tok.value, "SUBSTRING");
    struct token p = lexer_peek(l);
    if (p.type != TOK_RPAREN) {
        for (;;) {
            uint32_t arg_idx = parse_expr(l, a);
            if (args_count < 16) {
                arg_indices[args_count] = arg_idx;
            } else if (args_count == 16) {
                arena_set_error(a, "54023", "too many function arguments (max 16)");
                return IDX_NONE;
            }
            args_count++;
            p = lexer_peek(l);
            /* POSITION(sub IN str): treat IN as argument separator */
            if (is_position && p.type == TOK_KEYWORD &&
                sv_eq_ignorecase_cstr(p.value, "IN")) {
                lexer_next(l); /* consume IN */
            /* SUBSTRING(str FROM n [FOR m]): treat FROM/FOR as separators */
            } else if (is_substring && (p.type == TOK_KEYWORD || p.type == TOK_IDENTIFIER) &&
                       (sv_eq_ignorecase_cstr(p.value, "FROM") ||
                        sv_eq_ignorecase_cstr(p.value, "FOR"))) {
                lexer_next(l); /* consume FROM or FOR */
            } else if (p.type == TOK_COMMA) {
                lexer_next(l); /* consume , */
            } else {
                break;
            }
        }
    }
    /* Store arg root indices in arena.arg_indices (consecutive). */
    uint32_t ei = expr_alloc(a, EXPR_FUNC_CALL);
    EXPR(a, ei).func_call.func = expr_func_from_name(func_tok.value);
    uint32_t args_start = (uint32_t)a->arg_indices.count;
    for (uint32_t ai = 0; ai < args_count && ai < 16; ai++)
        da_push(&a->arg_indices, arg_indices[ai]);
    EXPR(a, ei).func_call.args_start = args_start;
    EXPR(a, ei).func_call.args_count = args_count;
    struct token rp = lexer_next(l); /* consume ) */
    if (rp.type != TOK_RPAREN) {
        arena_set_error(a, "42601", "expected ')' after function arguments");
    }
    return ei;
}

/* number literal (integer or float) — assumes TOK_NUMBER already peeked */
static uint32_t parse_number_literal(struct lexer *l, struct query_arena *a)
{
    struct token tok = lexer_next(l);
    uint32_t ei = expr_alloc(a, EXPR_LITERAL);
    if (sv_contains_char(tok.value, '.')) {
        EXPR(a, ei).literal.type = COLUMN_TYPE_FLOAT;
        EXPR(a, ei).literal.value.as_float = sv_atof(tok.value);
    } else {
        long long v = 0;
        size_t k = 0;
        int neg = 0;
        int overflow = 0;
        if (tok.value.len > 0 && tok.value.data[0] == '-') { neg = 1; k = 1; }
        for (; k < tok.value.len; k++) {
            int d = tok.value.data[k] - '0';
            if (v > (9223372036854775807LL - d) / 10) { overflow = 1; break; }
            v = v * 10 + d;
        }
        if (overflow) {
            /* too large for int64 — store as float */
            EXPR(a, ei).literal.type = COLUMN_TYPE_FLOAT;
            EXPR(a, ei).literal.value.as_float = sv_atof(tok.value);
            return ei;
        }
        if (neg) v = -v;
        if (v > 2147483647LL || v < -2147483648LL) {
            EXPR(a, ei).literal.type = COLUMN_TYPE_BIGINT;
            EXPR(a, ei).literal.value.as_bigint = v;
        } else {
            EXPR(a, ei).literal.type = COLUMN_TYPE_INT;
            EXPR(a, ei).literal.value.as_int = (int)v;
        }
    }
    return ei;
}

/* parse an atom: literal, column ref, function call, CASE, subquery, parens */
static uint32_t parse_expr_atom(struct lexer *l, struct query_arena *a)
{
    struct token tok = lexer_peek(l);

    /* CAST(expr AS type) */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "CAST"))
        return parse_cast_expr(l, a);

    /* EXTRACT(field FROM expr) */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "EXTRACT"))
        return parse_extract_expr(l, a);

    /* CURRENT_TIMESTAMP / CURRENT_DATE (no parens needed) */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "CURRENT_TIMESTAMP")) {
        lexer_next(l);
        uint32_t ei = expr_alloc(a, EXPR_FUNC_CALL);
        EXPR(a, ei).func_call.func = FUNC_CURRENT_TIMESTAMP;
        EXPR(a, ei).func_call.args_start = 0;
        EXPR(a, ei).func_call.args_count = 0;
        return ei;
    }
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "CURRENT")) {
        /* look ahead for _DATE or _TIMESTAMP — but CURRENT is also used in window frames */
        size_t saved = l->pos;
        lexer_next(l); /* consume CURRENT */
        /* CURRENT_DATE is a single keyword, but our lexer splits on _ boundaries?
         * Actually no — our lexer keeps underscores in identifiers. So CURRENT_TIMESTAMP
         * would be one token. This path handles bare CURRENT which might be CURRENT ROW. */
        l->pos = saved;
        /* fall through to column ref handling */
    }

    /* EXISTS(SELECT ...) */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "EXISTS")) {
        lexer_next(l); /* consume EXISTS */
        return parse_exists_expr(l, a, 0);
    }

    /* NOT EXISTS(SELECT ...) */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "NOT")) {
        size_t saved = l->pos;
        lexer_next(l); /* consume NOT */
        struct token next = lexer_peek(l);
        if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "EXISTS")) {
            lexer_next(l); /* consume EXISTS */
            return parse_exists_expr(l, a, 1);
        }
        /* not NOT EXISTS — restore lexer */
        l->pos = saved;
    }

    /* unary minus */
    if (tok.type == TOK_MINUS) {
        lexer_next(l);
        uint32_t operand = parse_expr_atom(l, a);
        if (operand == IDX_NONE) return IDX_NONE;
        uint32_t ei = expr_alloc(a, EXPR_UNARY_OP);
        EXPR(a, ei).unary.op = OP_NEG;
        EXPR(a, ei).unary.operand = operand;
        return ei;
    }

    /* parenthesized expression or subquery */
    if (tok.type == TOK_LPAREN) {
        lexer_next(l); /* consume ( */
        struct token peek = lexer_peek(l);
        if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "SELECT")) {
            /* subquery: capture SQL until matching ) */
            const char *sq_start = peek.value.data;
            int depth = 1;
            struct token st;
            while (depth > 0) {
                st = lexer_next(l);
                if (st.type == TOK_LPAREN) depth++;
                else if (st.type == TOK_RPAREN) depth--;
                else if (st.type == TOK_EOF) {
                    arena_set_error(a, "42601", "unterminated subquery in expression");
                    return IDX_NONE;
                }
            }
            const char *sq_end = st.value.data;
            while (sq_end > sq_start && (sq_end[-1] == ' ' || sq_end[-1] == '\n')) sq_end--;
            size_t sq_len = (size_t)(sq_end - sq_start);
            uint32_t ei = expr_alloc(a, EXPR_SUBQUERY);
            EXPR(a, ei).subquery.sql_idx = arena_store_string(a, sq_start, sq_len);
            return ei;
        }
        /* parenthesized expression */
        uint32_t inner = parse_expr(l, a);
        if (inner == IDX_NONE) return IDX_NONE;
        tok = lexer_next(l); /* consume ) */
        if (tok.type != TOK_RPAREN) {
            arena_set_error(a, "42601", "expected ')' after expression");
            /* still return what we have */
        }
        return inner;
    }

    /* NULL literal */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "NULL")) {
        lexer_next(l);
        uint32_t ei = expr_alloc(a, EXPR_LITERAL);
        EXPR(a, ei).literal.type = COLUMN_TYPE_TEXT;
        EXPR(a, ei).literal.is_null = 1;
        return ei;
    }

    /* boolean literal */
    if (tok.type == TOK_KEYWORD &&
        (sv_eq_ignorecase_cstr(tok.value, "TRUE") ||
         sv_eq_ignorecase_cstr(tok.value, "FALSE"))) {
        lexer_next(l);
        uint32_t ei = expr_alloc(a, EXPR_LITERAL);
        EXPR(a, ei).literal.type = COLUMN_TYPE_BOOLEAN;
        EXPR(a, ei).literal.value.as_bool = sv_eq_ignorecase_cstr(tok.value, "TRUE") ? 1 : 0;
        return ei;
    }

    /* CASE WHEN ... THEN ... [ELSE ...] END */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "CASE"))
        return parse_case_expr(l, a);

    /* function call: FUNC(...) */
    if ((tok.type == TOK_KEYWORD || tok.type == TOK_IDENTIFIER) &&
        is_expr_func_keyword(tok.value)) {
        /* look ahead past the name to check for ( */
        size_t saved = l->pos;
        lexer_next(l); /* consume func name */
        struct token maybe_lp = lexer_peek(l);
        if (maybe_lp.type == TOK_LPAREN) {
            lexer_next(l); /* consume ( */
            return parse_func_call_expr(l, a, tok);
        }
        /* not a function call, restore and fall through to column ref */
        l->pos = saved;
    }

    /* number literal */
    if (tok.type == TOK_NUMBER)
        return parse_number_literal(l, a);

    /* string literal */
    if (tok.type == TOK_STRING) {
        lexer_next(l);
        uint32_t ei = expr_alloc(a, EXPR_LITERAL);
        EXPR(a, ei).literal.type = COLUMN_TYPE_TEXT;
        EXPR(a, ei).literal.value.as_text = bump_strndup(&a->bump, tok.value.data, tok.value.len);
        return ei;
    }

    /* column reference: [table.]column or bare identifier/keyword used as column */
    if (tok.type == TOK_KEYWORD && is_expr_terminator_keyword(tok.value)) {
        /* structural keyword — not part of this expression */
        return IDX_NONE;
    }
    if (tok.type == TOK_IDENTIFIER || tok.type == TOK_KEYWORD) {
        lexer_next(l);
        sv first = tok.value;
        struct token dot = lexer_peek(l);
        if (dot.type == TOK_DOT) {
            lexer_next(l); /* consume . */
            struct token col_tok = lexer_next(l);
            /* check if this is a schema-qualified function call: schema.func( */
            struct token maybe_lp = lexer_peek(l);
            if (maybe_lp.type == TOK_LPAREN && is_expr_func_keyword(col_tok.value)) {
                lexer_next(l); /* consume ( */
                return parse_func_call_expr(l, a, col_tok);
            }
            uint32_t ei = expr_alloc(a, EXPR_COLUMN_REF);
            EXPR(a, ei).column_ref.table = first;
            EXPR(a, ei).column_ref.column = col_tok.value;
            return ei;
        }
        uint32_t ei = expr_alloc(a, EXPR_COLUMN_REF);
        EXPR(a, ei).column_ref.table = sv_from(NULL, 0);
        EXPR(a, ei).column_ref.column = first;
        return ei;
    }

    /* star (*) as a column reference (for SELECT *) */
    if (tok.type == TOK_STAR) {
        lexer_next(l);
        uint32_t ei = expr_alloc(a, EXPR_COLUMN_REF);
        EXPR(a, ei).column_ref.table = sv_from(NULL, 0);
        EXPR(a, ei).column_ref.column = tok.value;
        return ei;
    }

    arena_set_error(a, "42601", "unexpected token in expression: '" SV_FMT "'", (int)tok.value.len, tok.value.data);
    return IDX_NONE;
}

/* wrap an expression in a CAST node if followed by :: */
static uint32_t maybe_parse_postfix_cast(struct lexer *l, struct query_arena *a, uint32_t node)
{
    while (node != IDX_NONE) {
        struct token peek = lexer_peek(l);
        if (peek.type != TOK_DOUBLE_COLON) break;
        lexer_next(l); /* consume :: */
        enum column_type target = parse_cast_type_name(l);
        uint32_t ei = expr_alloc(a, EXPR_CAST);
        EXPR(a, ei).cast.operand = node;
        EXPR(a, ei).cast.target = target;
        node = ei;
    }
    return node;
}

/* multiplicative: atom (('*' | '/' | '%') atom)* */
static uint32_t parse_expr_mul(struct lexer *l, struct query_arena *a)
{
    uint32_t left = parse_expr_atom(l, a);
    left = maybe_parse_postfix_cast(l, a, left);
    if (left == IDX_NONE) return IDX_NONE;

    for (;;) {
        struct token tok = lexer_peek(l);
        enum expr_op op;
        if (tok.type == TOK_STAR)        op = OP_MUL;
        else if (tok.type == TOK_SLASH)  op = OP_DIV;
        else if (tok.type == TOK_PERCENT) op = OP_MOD;
        else break;

        lexer_next(l); /* consume operator */
        uint32_t right = parse_expr_atom(l, a);
        right = maybe_parse_postfix_cast(l, a, right);
        if (right == IDX_NONE) return left;

        uint32_t bin = expr_alloc(a, EXPR_BINARY_OP);
        EXPR(a, bin).binary.op = op;
        EXPR(a, bin).binary.left = left;
        EXPR(a, bin).binary.right = right;
        left = bin;
    }
    return left;
}

/* additive: mul (('+' | '-') mul)* */
static uint32_t parse_expr_add(struct lexer *l, struct query_arena *a)
{
    uint32_t left = parse_expr_mul(l, a);
    if (left == IDX_NONE) return IDX_NONE;

    for (;;) {
        struct token tok = lexer_peek(l);
        enum expr_op op;
        if (tok.type == TOK_PLUS)       op = OP_ADD;
        else if (tok.type == TOK_MINUS) op = OP_SUB;
        else break;

        lexer_next(l); /* consume operator */
        uint32_t right = parse_expr_mul(l, a);
        if (right == IDX_NONE) return left;

        uint32_t bin = expr_alloc(a, EXPR_BINARY_OP);
        EXPR(a, bin).binary.op = op;
        EXPR(a, bin).binary.left = left;
        EXPR(a, bin).binary.right = right;
        left = bin;
    }
    return left;
}

/* top-level expression: add (('||') add)* */
static uint32_t parse_expr(struct lexer *l, struct query_arena *a)
{
    uint32_t left = parse_expr_add(l, a);
    if (left == IDX_NONE) return IDX_NONE;

    for (;;) {
        struct token tok = lexer_peek(l);
        if (tok.type != TOK_PIPE_PIPE) break;

        lexer_next(l); /* consume || */
        uint32_t right = parse_expr_add(l, a);
        if (right == IDX_NONE) return left;

        uint32_t bin = expr_alloc(a, EXPR_BINARY_OP);
        EXPR(a, bin).binary.op = OP_CONCAT;
        EXPR(a, bin).binary.left = left;
        EXPR(a, bin).binary.right = right;
        left = bin;
    }

    /* postfix: expr IS [NOT] NULL */
    {
        struct token pk = lexer_peek(l);
        if (pk.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(pk.value, "IS")) {
            size_t saved = l->pos;
            lexer_next(l); /* consume IS */
            struct token nxt = lexer_peek(l);
            int negate = 0;
            if (nxt.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(nxt.value, "NOT")) {
                negate = 1;
                lexer_next(l); /* consume NOT */
                nxt = lexer_peek(l);
            }
            if (nxt.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(nxt.value, "NULL")) {
                lexer_next(l); /* consume NULL */
                uint32_t ei = expr_alloc(a, EXPR_IS_NULL);
                EXPR(a, ei).is_null.operand_is = left;
                EXPR(a, ei).is_null.negate = negate;
                left = ei;
            } else {
                l->pos = saved; /* restore — not IS [NOT] NULL */
            }
        }
    }

    return left;
}

static struct cell parse_literal_value_arena(struct token tok, struct query_arena *a)
{
    struct cell c = {0};
    if (tok.type == TOK_NUMBER) {
        if (sv_contains_char(tok.value, '.')) {
            c.type = COLUMN_TYPE_FLOAT;
            c.value.as_float = sv_atof(tok.value);
        } else {
            c.type = COLUMN_TYPE_INT;
            c.value.as_int = sv_atoi(tok.value);
        }
    } else if (tok.type == TOK_STRING) {
        c.type = COLUMN_TYPE_TEXT;
        c.value.as_text = bump_strndup(&a->bump, tok.value.data, tok.value.len);
    } else if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "NULL")) {
        c.type = COLUMN_TYPE_TEXT;
        c.value.as_text = NULL;
    } else if (tok.type == TOK_KEYWORD &&
               (sv_eq_ignorecase_cstr(tok.value, "TRUE") ||
                sv_eq_ignorecase_cstr(tok.value, "FALSE"))) {
        c.type = COLUMN_TYPE_BOOLEAN;
        c.value.as_bool = sv_eq_ignorecase_cstr(tok.value, "TRUE") ? 1 : 0;
    }
    return c;
}

/* --- helpers extracted from parse_single_cond --- */

/* EXISTS (SELECT ...) condition — assumes EXISTS keyword already peeked.
 * Consumes EXISTS, '(', subquery, ')'. */
static uint32_t parse_cond_exists(struct lexer *l, struct query_arena *a)
{
    lexer_next(l); /* consume EXISTS */
    struct token lp = lexer_next(l);
    if (lp.type != TOK_LPAREN) {
        arena_set_error(a, "42601", "expected '(' after EXISTS");
        return IDX_NONE;
    }
    struct token peek_sel = lexer_peek(l);
    const char *sq_start = peek_sel.value.data;
    int depth = 1;
    while (depth > 0) {
        struct token t = lexer_next(l);
        if (t.type == TOK_LPAREN) depth++;
        else if (t.type == TOK_RPAREN) depth--;
        else if (t.type == TOK_EOF) {
            arena_set_error(a, "42601", "unterminated EXISTS subquery");
            return IDX_NONE;
        }
    }
    const char *sq_end = l->input + l->pos - 1; /* before closing ')' */
    uint32_t ci = arena_alloc_cond(a);
    COND(a, ci).type = COND_COMPARE;
    COND(a, ci).op = CMP_EXISTS;
    COND(a, ci).lhs_expr = IDX_NONE;
    COND(a, ci).left = IDX_NONE;
    COND(a, ci).right = IDX_NONE;
    COND(a, ci).subquery_sql = arena_store_string(a, sq_start, (size_t)(sq_end - sq_start));
    COND(a, ci).scalar_subquery_sql = IDX_NONE;
    COND(a, ci).in_values_start = 0;
    COND(a, ci).in_values_count = 0;
    COND(a, ci).array_values_start = 0;
    COND(a, ci).array_values_count = 0;
    COND(a, ci).multi_columns_start = 0;
    COND(a, ci).multi_columns_count = 0;
    COND(a, ci).multi_values_start = 0;
    COND(a, ci).multi_values_count = 0;
    return ci;
}

/* (col, col, ...) [NOT] IN ((v,...), ...) — multi-column IN.
 * Caller has consumed the opening '(' and determined this is a multi-column IN. */
static uint32_t parse_cond_multi_in(struct lexer *l, struct query_arena *a)
{
    uint32_t ci = arena_alloc_cond(a);
    COND(a, ci).type = COND_MULTI_IN;
    COND(a, ci).lhs_expr = IDX_NONE;
    COND(a, ci).left = IDX_NONE;
    COND(a, ci).right = IDX_NONE;
    COND(a, ci).subquery_sql = IDX_NONE;
    COND(a, ci).scalar_subquery_sql = IDX_NONE;
    COND(a, ci).in_values_start = 0;
    COND(a, ci).in_values_count = 0;
    COND(a, ci).array_values_start = 0;
    COND(a, ci).array_values_count = 0;
    uint32_t mc_start = (uint32_t)a->svs.count;
    uint32_t mc_count = 0;
    for (;;) {
        struct token col = lexer_next(l);
        if (col.type != TOK_IDENTIFIER && col.type != TOK_KEYWORD) {
            arena_set_error(a, "42601", "expected column in multi-column IN");
            return IDX_NONE;
        }
        sv colsv = consume_identifier(l, col);
        arena_push_sv(a, colsv);
        mc_count++;
        struct token sep = lexer_next(l);
        if (sep.type == TOK_RPAREN) break;
        if (sep.type != TOK_COMMA) {
            arena_set_error(a, "42601", "expected ',' or ')' in column list");
            return IDX_NONE;
        }
    }
    COND(a, ci).multi_columns_start = mc_start;
    COND(a, ci).multi_columns_count = mc_count;
    COND(a, ci).multi_tuple_width = (int)mc_count;
    COND(a, ci).op = CMP_IN;
    struct token kw = lexer_next(l);
    if (kw.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(kw.value, "NOT")) {
        COND(a, ci).op = CMP_NOT_IN;
        kw = lexer_next(l);
    }
    if (kw.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(kw.value, "IN")) {
        arena_set_error(a, "42601", "expected IN after column tuple");
        return IDX_NONE;
    }
    struct token lp = lexer_next(l);
    if (lp.type != TOK_LPAREN) {
        arena_set_error(a, "42601", "expected '(' after IN");
        return IDX_NONE;
    }
    uint32_t mv_start = (uint32_t)a->cells.count;
    uint32_t mv_count = 0;
    for (;;) {
        struct token tp = lexer_next(l);
        if (tp.type != TOK_LPAREN) {
            arena_set_error(a, "42601", "expected '(' for value tuple");
            return IDX_NONE;
        }
        for (int vi = 0; vi < COND(a, ci).multi_tuple_width; vi++) {
            struct token vt = lexer_next(l);
            struct cell v = parse_literal_value_arena(vt, a);
            arena_push_cell(a, v);
            mv_count++;
            if (vi < COND(a, ci).multi_tuple_width - 1) {
                struct token cm = lexer_next(l);
                if (cm.type != TOK_COMMA) {
                    arena_set_error(a, "42601", "expected ',' in value tuple");
                    return IDX_NONE;
                }
            }
        }
        struct token rp = lexer_next(l);
        if (rp.type != TOK_RPAREN) {
            arena_set_error(a, "42601", "expected ')' after value tuple");
            return IDX_NONE;
        }
        struct token sep = lexer_next(l);
        if (sep.type == TOK_RPAREN) break;
        if (sep.type != TOK_COMMA) {
            arena_set_error(a, "42601", "expected ',' or ')' in IN list");
            return IDX_NONE;
        }
    }
    COND(a, ci).multi_values_start = mv_start;
    COND(a, ci).multi_values_count = mv_count;
    return ci;
}

/* IS [NOT] NULL / IS [NOT] DISTINCT FROM — caller has already consumed IS.
 * ci is the pre-allocated condition index. Returns ci on success, IDX_NONE on error. */
static uint32_t parse_cond_is(struct lexer *l, struct query_arena *a, uint32_t ci)
{
    struct token next = lexer_next(l);
    if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "NOT")) {
        struct token next2 = lexer_peek(l);
        if (next2.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next2.value, "DISTINCT")) {
            lexer_next(l); /* consume DISTINCT */
            struct token from_tok = lexer_next(l);
            if (from_tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(from_tok.value, "FROM")) {
                arena_set_error(a, "42601", "expected FROM after IS NOT DISTINCT");
                return IDX_NONE;
            }
            COND(a, ci).op = CMP_IS_NOT_DISTINCT;
            struct token val_tok = lexer_next(l);
            if (val_tok.type == TOK_IDENTIFIER || (val_tok.type == TOK_KEYWORD && !sv_eq_ignorecase_cstr(val_tok.value, "NULL") && !sv_eq_ignorecase_cstr(val_tok.value, "TRUE") && !sv_eq_ignorecase_cstr(val_tok.value, "FALSE"))) {
                COND(a, ci).rhs_column = val_tok.value;
            } else {
                COND(a, ci).value = parse_literal_value_arena(val_tok, a);
            }
            return ci;
        }
        if (next2.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next2.value, "TRUE")) {
            lexer_next(l); /* consume TRUE */
            COND(a, ci).op = CMP_NE;
            COND(a, ci).value.type = COLUMN_TYPE_BOOLEAN;
            COND(a, ci).value.value.as_int = 1;
            return ci;
        }
        if (next2.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next2.value, "FALSE")) {
            lexer_next(l); /* consume FALSE */
            COND(a, ci).op = CMP_NE;
            COND(a, ci).value.type = COLUMN_TYPE_BOOLEAN;
            COND(a, ci).value.value.as_int = 0;
            return ci;
        }
        next = lexer_next(l);
        if (next.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(next.value, "NULL")) {
            arena_set_error(a, "42601", "expected NULL after IS NOT");
            return IDX_NONE;
        }
        COND(a, ci).op = CMP_IS_NOT_NULL;
    } else if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "TRUE")) {
        COND(a, ci).op = CMP_EQ;
        COND(a, ci).value.type = COLUMN_TYPE_BOOLEAN;
        COND(a, ci).value.value.as_int = 1;
        return ci;
    } else if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "FALSE")) {
        COND(a, ci).op = CMP_EQ;
        COND(a, ci).value.type = COLUMN_TYPE_BOOLEAN;
        COND(a, ci).value.value.as_int = 0;
        return ci;
    } else if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "DISTINCT")) {
        struct token from_tok = lexer_next(l);
        if (from_tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(from_tok.value, "FROM")) {
            arena_set_error(a, "42601", "expected FROM after IS DISTINCT");
            return IDX_NONE;
        }
        COND(a, ci).op = CMP_IS_DISTINCT;
        struct token val_tok = lexer_next(l);
        if (val_tok.type == TOK_IDENTIFIER || (val_tok.type == TOK_KEYWORD && !sv_eq_ignorecase_cstr(val_tok.value, "NULL") && !sv_eq_ignorecase_cstr(val_tok.value, "TRUE") && !sv_eq_ignorecase_cstr(val_tok.value, "FALSE"))) {
            COND(a, ci).rhs_column = val_tok.value;
        } else {
            COND(a, ci).value = parse_literal_value_arena(val_tok, a);
        }
        return ci;
    } else if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "NULL")) {
        COND(a, ci).op = CMP_IS_NULL;
    } else {
        arena_set_error(a, "42601", "expected NULL, NOT, or DISTINCT after IS");
        return IDX_NONE;
    }
    return ci;
}

/* [NOT] IN (values...) or IN (SELECT ...) — caller has already set ci.op to CMP_IN or CMP_NOT_IN.
 * Consumes '(' values ')'. Returns ci on success, IDX_NONE on error. */
static uint32_t parse_cond_in_list(struct lexer *l, struct query_arena *a, uint32_t ci)
{
    struct token tok = lexer_next(l);
    if (tok.type != TOK_LPAREN) {
        arena_set_error(a, "42601", "expected '(' after IN");
        return IDX_NONE;
    }
    /* check for subquery: IN (SELECT ...) */
    {
        struct token peek_sel = lexer_peek(l);
        if (peek_sel.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek_sel.value, "SELECT")) {
            /* capture everything from SELECT to matching ')' as subquery SQL */
            const char *sq_start = peek_sel.value.data;
            int depth = 1;
            while (depth > 0) {
                tok = lexer_next(l);
                if (tok.type == TOK_LPAREN) depth++;
                else if (tok.type == TOK_RPAREN) depth--;
                else if (tok.type == TOK_EOF) {
                    arena_set_error(a, "42601", "unterminated subquery");
                    return IDX_NONE;
                }
            }
            /* tok is now the closing ')' */
            const char *sq_end = tok.value.data; /* points at ')' */
            size_t sq_len = (size_t)(sq_end - sq_start);
            char *sql_copy = malloc(sq_len + 1);
            memcpy(sql_copy, sq_start, sq_len);
            sql_copy[sq_len] = '\0';
            /* trim trailing whitespace */
            while (sq_len > 0 && (sql_copy[sq_len-1] == ' ' || sql_copy[sq_len-1] == '\t'))
                sql_copy[--sq_len] = '\0';
            COND(a, ci).subquery_sql = arena_own_string(a, sql_copy);
            return ci;
        }
    }
    uint32_t iv_start = (uint32_t)a->cells.count;
    uint32_t iv_count = 0;
    for (;;) {
        tok = lexer_next(l);
        struct cell v = parse_literal_value_arena(tok, a);
        arena_push_cell(a, v);
        iv_count++;
        tok = lexer_next(l);
        if (tok.type == TOK_RPAREN) break;
        if (tok.type != TOK_COMMA) {
            arena_set_error(a, "42601", "expected ',' or ')' in IN list");
            return IDX_NONE;
        }
    }
    COND(a, ci).in_values_start = iv_start;
    COND(a, ci).in_values_count = iv_count;
    return ci;
}

/* col op ANY/ALL/SOME(ARRAY[...]) — caller has already set ci.op from the comparison token.
 * Consumes ANY/ALL/SOME, '(', values, ')'. Returns ci on success, IDX_NONE on error. */
static uint32_t parse_cond_any_all(struct lexer *l, struct query_arena *a, uint32_t ci)
{
    struct token peek_aas = lexer_peek(l);
    int is_all = sv_eq_ignorecase_cstr(peek_aas.value, "ALL");
    lexer_next(l); /* consume ANY/ALL/SOME */
    struct token lp = lexer_next(l);
    if (lp.type != TOK_LPAREN) {
        arena_set_error(a, "42601", "expected '(' after ANY/ALL/SOME");
        return IDX_NONE;
    }
    /* optional ARRAY keyword */
    struct token arr_peek = lexer_peek(l);
    if (arr_peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(arr_peek.value, "ARRAY")) {
        lexer_next(l); /* consume ARRAY */
        struct token lb = lexer_next(l);
        (void)lb;
    }
    COND(a, ci).is_any = is_all ? 0 : 1;
    COND(a, ci).is_all = is_all ? 1 : 0;
    /* check for subquery: ANY (SELECT ...) */
    {
        struct token sq_peek = lexer_peek(l);
        if (sq_peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(sq_peek.value, "SELECT")) {
            const char *sq_start = l->input + l->pos;
            int depth = 1;
            struct token st;
            while (depth > 0) {
                st = lexer_next(l);
                if (st.type == TOK_LPAREN) depth++;
                else if (st.type == TOK_RPAREN) depth--;
                else if (st.type == TOK_EOF) {
                    arena_set_error(a, "42601", "unterminated ANY/ALL subquery");
                    return IDX_NONE;
                }
            }
            const char *sq_end = st.value.data;
            while (sq_end > sq_start && (sq_end[-1] == ' ' || sq_end[-1] == '\n')) sq_end--;
            COND(a, ci).subquery_sql = arena_store_string(a, sq_start, (size_t)(sq_end - sq_start));
            return ci;
        }
    }
    uint32_t av_start = (uint32_t)a->cells.count;
    uint32_t av_count = 0;
    /* parse values until ) or ] */
    for (;;) {
        struct token vt = lexer_next(l);
        if (vt.type == TOK_RPAREN) break;
        /* skip ] before ) */
        if (vt.type == TOK_UNKNOWN && vt.value.len == 1 && vt.value.data[0] == ']') {
            struct token rp = lexer_next(l);
            if (rp.type == TOK_RPAREN) break;
            continue;
        }
        struct cell v = parse_literal_value_arena(vt, a);
        arena_push_cell(a, v);
        av_count++;
        struct token sep = lexer_peek(l);
        if (sep.type == TOK_COMMA) lexer_next(l);
    }
    COND(a, ci).array_values_start = av_start;
    COND(a, ci).array_values_count = av_count;
    return ci;
}

/* parse a single comparison or grouped/negated condition */
static uint32_t parse_single_cond(struct lexer *l, struct query_arena *a)
{
    struct token tok = lexer_peek(l);

    /* EXISTS (SELECT ...) */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "EXISTS"))
        return parse_cond_exists(l, a);

    /* NOT expr / NOT EXISTS */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "NOT")) {
        lexer_next(l); /* consume NOT */
        struct token next_peek = lexer_peek(l);
        if (next_peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next_peek.value, "EXISTS")) {
            uint32_t inner = parse_single_cond(l, a);
            if (inner == IDX_NONE) return IDX_NONE;
            COND(a, inner).op = CMP_NOT_EXISTS;
            return inner;
        }
        uint32_t inner = parse_single_cond(l, a);
        if (inner == IDX_NONE) return IDX_NONE;
        uint32_t ci = arena_alloc_cond(a);
        COND(a, ci).type = COND_NOT;
        COND(a, ci).left = inner;
        COND(a, ci).right = IDX_NONE;
        COND(a, ci).lhs_expr = IDX_NONE;
        COND(a, ci).subquery_sql = IDX_NONE;
        COND(a, ci).scalar_subquery_sql = IDX_NONE;
        return ci;
    }

    /* ( expr ) — parenthesized sub-expression, or (a, b) IN (...) multi-column IN */
    if (tok.type == TOK_LPAREN) {
        lexer_next(l); /* consume ( */
        size_t saved = l->pos; /* position right after ( */
        /* look ahead: is this (col, col, ...) IN (...) ? */
        int is_multi_in = 0;
        {
            struct lexer tmp = { .input = l->input, .pos = saved };
            struct token t1 = lexer_next(&tmp);
            if (t1.type == TOK_IDENTIFIER || t1.type == TOK_KEYWORD) {
                struct token t1p = lexer_peek(&tmp);
                if (t1p.type == TOK_DOT) { lexer_next(&tmp); lexer_next(&tmp); }
                struct token t2 = lexer_peek(&tmp);
                if (t2.type == TOK_COMMA) {
                    int depth = 1;
                    while (depth > 0) {
                        struct token tt = lexer_next(&tmp);
                        if (tt.type == TOK_LPAREN) depth++;
                        else if (tt.type == TOK_RPAREN) depth--;
                        else if (tt.type == TOK_EOF) break;
                    }
                    struct token after = lexer_peek(&tmp);
                    if (after.type == TOK_KEYWORD &&
                        (sv_eq_ignorecase_cstr(after.value, "IN") ||
                         sv_eq_ignorecase_cstr(after.value, "NOT")))
                        is_multi_in = 1;
                }
            }
        }
        if (is_multi_in)
            return parse_cond_multi_in(l, a);
        /* not multi-column IN — check if this is (expr) op value (e.g. (SELECT ...) > 0) */
        l->pos = saved;
        {
            /* peek: is the first token inside the paren SELECT? If so, it's a subquery expr LHS */
            struct token inner_peek = lexer_peek(l);
            int is_expr_lhs = 0;
            if (inner_peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(inner_peek.value, "SELECT"))
                is_expr_lhs = 1;
            if (is_expr_lhs) {
                /* back up to before the '(' and parse as expression */
                l->pos = saved - 1; /* re-position before '(' */
                /* find the actual '(' position */
                while (l->pos > 0 && l->input[l->pos] != '(') l->pos--;
                uint32_t lhs_expr = parse_expr(l, a);
                if (lhs_expr != IDX_NONE) {
                    struct token maybe_op = lexer_peek(l);
                    if (is_cmp_token(maybe_op.type)) {
                        uint32_t eci = arena_alloc_cond(a);
                        COND(a, eci).type = COND_COMPARE;
                        COND(a, eci).lhs_expr = lhs_expr;
                        COND(a, eci).left = IDX_NONE;
                        COND(a, eci).right = IDX_NONE;
                        COND(a, eci).subquery_sql = IDX_NONE;
                        COND(a, eci).scalar_subquery_sql = IDX_NONE;
                        COND(a, eci).in_values_start = 0;
                        COND(a, eci).in_values_count = 0;
                        COND(a, eci).array_values_start = 0;
                        COND(a, eci).array_values_count = 0;
                        COND(a, eci).multi_columns_start = 0;
                        COND(a, eci).multi_columns_count = 0;
                        COND(a, eci).multi_values_start = 0;
                        COND(a, eci).multi_values_count = 0;
                        COND(a, eci).column = sv_from(NULL, 0);
                        struct token eop = lexer_next(l);
                        /* parse RHS value */
                        struct token rhs_tok = lexer_next(l);
                        COND(a, eci).value = parse_literal_value_arena(rhs_tok, a);
                        COND(a, eci).op = cmp_from_token(eop.type);
                        return eci;
                    }
                }
                /* failed — restore */
                l->pos = saved;
            }
        }
        /* grouped condition fallback */
        uint32_t inner = parse_or_cond(l, a);
        if (inner == IDX_NONE) return IDX_NONE;
        tok = lexer_next(l);
        if (tok.type != TOK_RPAREN) {
            arena_set_error(a, "42601", "expected ')' after grouped condition");
            return IDX_NONE;
        }
        return inner;
    }

    tok = lexer_next(l);

    /* bare TRUE/FALSE as standalone condition */
    if (tok.type == TOK_KEYWORD &&
        (sv_eq_ignorecase_cstr(tok.value, "TRUE") ||
         sv_eq_ignorecase_cstr(tok.value, "FALSE"))) {
        int val = sv_eq_ignorecase_cstr(tok.value, "TRUE") ? 1 : 0;
        uint32_t bci = arena_alloc_cond(a);
        COND(a, bci).type = COND_COMPARE;
        COND(a, bci).lhs_expr = IDX_NONE;
        COND(a, bci).left = IDX_NONE;
        COND(a, bci).right = IDX_NONE;
        COND(a, bci).subquery_sql = IDX_NONE;
        COND(a, bci).scalar_subquery_sql = IDX_NONE;
        COND(a, bci).op = CMP_EQ;
        COND(a, bci).column = sv_from(NULL, 0);
        /* synthesize: 1 = 1 (TRUE) or 1 = 0 (FALSE) */
        uint32_t lhs = expr_alloc(a, EXPR_LITERAL);
        EXPR(a, lhs).literal.type = COLUMN_TYPE_INT;
        EXPR(a, lhs).literal.value.as_int = 1;
        COND(a, bci).lhs_expr = lhs;
        COND(a, bci).value.type = COLUMN_TYPE_INT;
        COND(a, bci).value.value.as_int = val;
        return bci;
    }

    /* accept identifiers and keywords as column names (e.g. sum, count, avg in HAVING) */
    if (tok.type != TOK_IDENTIFIER && tok.type != TOK_KEYWORD) {
        arena_set_error(a, "42601", "expected column name in WHERE/HAVING");
        return IDX_NONE;
    }

    uint32_t ci = arena_alloc_cond(a);
    COND(a, ci).type = COND_COMPARE;
    COND(a, ci).lhs_expr = IDX_NONE;
    COND(a, ci).left = IDX_NONE;
    COND(a, ci).right = IDX_NONE;
    COND(a, ci).subquery_sql = IDX_NONE;
    COND(a, ci).scalar_subquery_sql = IDX_NONE;
    COND(a, ci).in_values_start = 0;
    COND(a, ci).in_values_count = 0;
    COND(a, ci).array_values_start = 0;
    COND(a, ci).array_values_count = 0;
    COND(a, ci).multi_columns_start = 0;
    COND(a, ci).multi_columns_count = 0;
    COND(a, ci).multi_values_start = 0;
    COND(a, ci).multi_values_count = 0;

    struct token op_tok;

    /* check if LHS is an aggregate call in HAVING (e.g. COUNT(*) > 1) */
    if (is_agg_keyword(tok.value)) {
        struct token peek_lp = lexer_peek(l);
        if (peek_lp.type == TOK_LPAREN) {
            /* consume the aggregate call: FUNC( ... ) */
            lexer_next(l); /* ( */
            int depth = 1;
            while (depth > 0) {
                struct token t = lexer_next(l);
                if (t.type == TOK_LPAREN) depth++;
                else if (t.type == TOK_RPAREN) depth--;
                else if (t.type == TOK_EOF) break;
            }
            /* use the lowercase function name as the virtual column name */
            const char *agg_name = "?";
            if (sv_eq_ignorecase_cstr(tok.value, "COUNT")) agg_name = "count";
            else if (sv_eq_ignorecase_cstr(tok.value, "SUM")) agg_name = "sum";
            else if (sv_eq_ignorecase_cstr(tok.value, "AVG")) agg_name = "avg";
            else if (sv_eq_ignorecase_cstr(tok.value, "MIN")) agg_name = "min";
            else if (sv_eq_ignorecase_cstr(tok.value, "MAX")) agg_name = "max";
            COND(a, ci).column = sv_from(agg_name, strlen(agg_name));
            op_tok = lexer_next(l);
            goto parse_operator;
        }
    }

    /* check if LHS is a function call (e.g. COALESCE(val, 0) > 5) */
    if (is_expr_func_keyword(tok.value)) {
        struct token peek_lp = lexer_peek(l);
        if (peek_lp.type == TOK_LPAREN) {
            /* back up to before the function name and parse as expression */
            l->pos = tok.value.data - l->input;
            COND(a, ci).lhs_expr = parse_expr(l, a);
            COND(a, ci).column = sv_from(NULL, 0);
            op_tok = lexer_next(l);
            goto parse_operator;
        }
    }

    COND(a, ci).column = consume_identifier(l, tok);
    op_tok = lexer_next(l);

    /* If op_tok is '(' then the LHS is a function call (possibly schema-qualified).
     * Back up and re-parse as an expression. */
    if (op_tok.type == TOK_LPAREN) {
        l->pos = tok.value.data - l->input;
        COND(a, ci).lhs_expr = parse_expr(l, a);
        COND(a, ci).column = sv_from(NULL, 0);
        op_tok = lexer_next(l);
    }

    /* If op_tok is an arithmetic operator, the LHS is an expression (e.g. a + b > 11).
     * Back up to before the identifier and re-parse the whole LHS as an expression. */
    if (op_tok.type == TOK_PLUS || op_tok.type == TOK_MINUS ||
        op_tok.type == TOK_STAR || op_tok.type == TOK_SLASH ||
        op_tok.type == TOK_PERCENT || op_tok.type == TOK_PIPE_PIPE ||
        op_tok.type == TOK_DOUBLE_COLON) {
        l->pos = tok.value.data - l->input;
        COND(a, ci).lhs_expr = parse_expr(l, a);
        COND(a, ci).column = sv_from(NULL, 0);
        op_tok = lexer_next(l);
    }

    /* Bare boolean column: WHERE active ORDER BY ...
     * If op_tok is a structural keyword, EOF, semicolon, or ), treat as col = TRUE */
    if (op_tok.type == TOK_EOF || op_tok.type == TOK_SEMICOLON ||
        op_tok.type == TOK_RPAREN ||
        (op_tok.type == TOK_KEYWORD &&
         (sv_eq_ignorecase_cstr(op_tok.value, "ORDER") ||
          sv_eq_ignorecase_cstr(op_tok.value, "GROUP") ||
          sv_eq_ignorecase_cstr(op_tok.value, "HAVING") ||
          sv_eq_ignorecase_cstr(op_tok.value, "LIMIT") ||
          sv_eq_ignorecase_cstr(op_tok.value, "OFFSET") ||
          sv_eq_ignorecase_cstr(op_tok.value, "AND") ||
          sv_eq_ignorecase_cstr(op_tok.value, "OR") ||
          sv_eq_ignorecase_cstr(op_tok.value, "RETURNING")))) {
        /* push back the token we consumed */
        l->pos = op_tok.value.data - l->input;
        COND(a, ci).op = CMP_EQ;
        COND(a, ci).value.type = COLUMN_TYPE_BOOLEAN;
        COND(a, ci).value.value.as_bool = 1;
        return ci;
    }

parse_operator:

    /* IS NULL / IS NOT NULL / IS [NOT] DISTINCT FROM */
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "IS"))
        return parse_cond_is(l, a, ci);

    /* NOT IN / NOT BETWEEN / NOT LIKE / NOT ILIKE */
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "NOT")) {
        struct token next = lexer_next(l);
        if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "IN")) {
            COND(a, ci).op = CMP_NOT_IN;
            return parse_cond_in_list(l, a, ci);
        }
        if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "BETWEEN")) {
            COND(a, ci).op = CMP_BETWEEN;
            tok = lexer_next(l);
            COND(a, ci).value = parse_literal_value_arena(tok, a);
            tok = lexer_next(l); /* AND */
            if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "AND")) {
                arena_set_error(a, "42601", "expected AND in NOT BETWEEN");
                return IDX_NONE;
            }
            tok = lexer_next(l);
            COND(a, ci).between_high = parse_literal_value_arena(tok, a);
            /* wrap in COND_NOT to negate */
            uint32_t not_ci = arena_alloc_cond(a);
            COND(a, not_ci).type = COND_NOT;
            COND(a, not_ci).left = ci;
            COND(a, not_ci).right = IDX_NONE;
            COND(a, not_ci).lhs_expr = IDX_NONE;
            COND(a, not_ci).subquery_sql = IDX_NONE;
            COND(a, not_ci).scalar_subquery_sql = IDX_NONE;
            return not_ci;
        }
        if (next.type == TOK_KEYWORD && (sv_eq_ignorecase_cstr(next.value, "LIKE") ||
                                          sv_eq_ignorecase_cstr(next.value, "ILIKE"))) {
            COND(a, ci).op = sv_eq_ignorecase_cstr(next.value, "LIKE") ? CMP_LIKE : CMP_ILIKE;
            tok = lexer_next(l);
            COND(a, ci).value = parse_literal_value_arena(tok, a);
            /* wrap in COND_NOT to negate */
            uint32_t not_ci = arena_alloc_cond(a);
            COND(a, not_ci).type = COND_NOT;
            COND(a, not_ci).left = ci;
            COND(a, not_ci).right = IDX_NONE;
            COND(a, not_ci).lhs_expr = IDX_NONE;
            COND(a, not_ci).subquery_sql = IDX_NONE;
            COND(a, not_ci).scalar_subquery_sql = IDX_NONE;
            return not_ci;
        }
        arena_set_error(a, "42601", "expected IN, BETWEEN, LIKE, or ILIKE after NOT");
        return IDX_NONE;
    }

    /* IN (...) */
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "IN")) {
        COND(a, ci).op = CMP_IN;
        return parse_cond_in_list(l, a, ci);
    }

    /* BETWEEN low AND high */
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "BETWEEN")) {
        COND(a, ci).op = CMP_BETWEEN;
        tok = lexer_next(l);
        COND(a, ci).value = parse_literal_value_arena(tok, a);
        tok = lexer_next(l); /* AND */
        if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "AND")) {
            arena_set_error(a, "42601", "expected AND in BETWEEN");
            return IDX_NONE;
        }
        tok = lexer_next(l);
        COND(a, ci).between_high = parse_literal_value_arena(tok, a);
        return ci;
    }

    /* LIKE / ILIKE */
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "LIKE")) {
        COND(a, ci).op = CMP_LIKE;
        tok = lexer_next(l);
        COND(a, ci).value = parse_literal_value_arena(tok, a);
        return ci;
    }
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "ILIKE")) {
        COND(a, ci).op = CMP_ILIKE;
        tok = lexer_next(l);
        COND(a, ci).value = parse_literal_value_arena(tok, a);
        return ci;
    }

    /* Handle OPERATOR(schema.op) syntax: extract the actual operator */
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "OPERATOR")) {
        struct token lp = lexer_next(l); /* ( */
        if (lp.type == TOK_LPAREN) {
            /* skip schema. prefix */
            struct token ot = lexer_next(l);
            struct token peek_d = lexer_peek(l);
            if (peek_d.type == TOK_DOT) {
                lexer_next(l); /* consume . */
                ot = lexer_next(l); /* actual operator token */
            }
            lexer_next(l); /* consume ) */
            op_tok = ot;
        }
    }

    if (!is_cmp_token(op_tok.type)) {
        arena_set_error(a, "42601", "expected comparison operator in WHERE");
        return IDX_NONE;
    }
    COND(a, ci).op = cmp_from_token(op_tok.type);

    /* check for ANY/ALL/SOME: col op ANY(ARRAY[...]) or col op ANY(v1,v2,...) */
    {
        struct token peek_aas = lexer_peek(l);
        if (peek_aas.type == TOK_KEYWORD &&
            (sv_eq_ignorecase_cstr(peek_aas.value, "ANY") ||
             sv_eq_ignorecase_cstr(peek_aas.value, "ALL") ||
             sv_eq_ignorecase_cstr(peek_aas.value, "SOME")))
            return parse_cond_any_all(l, a, ci);
    }

    /* check for scalar subquery: col > (SELECT ...) */
    {
        struct token peek_sq = lexer_peek(l);
        if (peek_sq.type == TOK_LPAREN) {
            /* look ahead for SELECT */
            size_t saved = l->pos;
            lexer_next(l); /* consume ( */
            struct token maybe_sel = lexer_peek(l);
            if (maybe_sel.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(maybe_sel.value, "SELECT")) {
                /* capture subquery SQL */
                const char *sq_start = maybe_sel.value.data;
                int depth = 1;
                struct token st;
                while (depth > 0) {
                    st = lexer_next(l);
                    if (st.type == TOK_LPAREN) depth++;
                    else if (st.type == TOK_RPAREN) depth--;
                    else if (st.type == TOK_EOF) break;
                }
                const char *sq_end = st.value.data; /* points at closing ')' */
                while (sq_end > sq_start && (sq_end[-1] == ' ' || sq_end[-1] == '\n')) sq_end--;
                size_t sq_len = (size_t)(sq_end - sq_start);
                COND(a, ci).scalar_subquery_sql = arena_store_string(a, sq_start, sq_len);
                return ci;
            }
            /* not a subquery, restore */
            l->pos = saved;
        }
    }

    tok = lexer_next(l);
    if (tok.type == TOK_IDENTIFIER || (tok.type == TOK_KEYWORD &&
        !sv_eq_ignorecase_cstr(tok.value, "NULL") &&
        !sv_eq_ignorecase_cstr(tok.value, "TRUE") &&
        !sv_eq_ignorecase_cstr(tok.value, "FALSE"))) {
        /* check if next token is a dot (qualified column ref like t2.col) or
         * if this looks like a column reference rather than a literal */
        struct token peek_dot = lexer_peek(l);
        if (peek_dot.type == TOK_DOT || tok.type == TOK_IDENTIFIER) {
            /* store as RHS column reference for column-to-column comparison */
            COND(a, ci).rhs_column = consume_identifier(l, tok);
            return ci;
        }
    }
    COND(a, ci).value = parse_literal_value_arena(tok, a);

    /* skip optional COLLATE clause (e.g. COLLATE pg_catalog.default) */
    {
        struct token peek_col = lexer_peek(l);
        if (peek_col.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek_col.value, "COLLATE")) {
            lexer_next(l); /* consume COLLATE */
            struct token ct = lexer_next(l); /* schema or collation name */
            struct token cd = lexer_peek(l);
            if (cd.type == TOK_DOT) {
                lexer_next(l); /* consume . */
                lexer_next(l); /* consume collation name */
            }
            (void)ct;
        }
    }

    return ci;
}

/* parse WHERE condition with AND/OR support (left-to-right, AND binds tighter) */
static uint32_t parse_and_cond(struct lexer *l, struct query_arena *a)
{
    uint32_t left = parse_single_cond(l, a);
    if (left == IDX_NONE) return IDX_NONE;

    for (;;) {
        struct token peek = lexer_peek(l);
        if (peek.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(peek.value, "AND"))
            break;
        lexer_next(l); /* consume AND */
        uint32_t right = parse_single_cond(l, a);
        if (right == IDX_NONE) return IDX_NONE;
        uint32_t node = arena_alloc_cond(a);
        COND(a, node).type = COND_AND;
        COND(a, node).left = left;
        COND(a, node).right = right;
        COND(a, node).lhs_expr = IDX_NONE;
        COND(a, node).subquery_sql = IDX_NONE;
        COND(a, node).scalar_subquery_sql = IDX_NONE;
        left = node;
    }
    return left;
}

static uint32_t parse_or_cond(struct lexer *l, struct query_arena *a)
{
    uint32_t left = parse_and_cond(l, a);
    if (left == IDX_NONE) return IDX_NONE;

    for (;;) {
        struct token peek = lexer_peek(l);
        if (peek.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(peek.value, "OR"))
            break;
        lexer_next(l); /* consume OR */
        uint32_t right = parse_and_cond(l, a);
        if (right == IDX_NONE) return IDX_NONE;
        uint32_t node = arena_alloc_cond(a);
        COND(a, node).type = COND_OR;
        COND(a, node).left = left;
        COND(a, node).right = right;
        COND(a, node).lhs_expr = IDX_NONE;
        COND(a, node).subquery_sql = IDX_NONE;
        COND(a, node).scalar_subquery_sql = IDX_NONE;
        left = node;
    }
    return left;
}

static int parse_where_clause(struct lexer *l, struct query_arena *a, struct where_clause *w)
{
    w->has_where = 1;
    w->where_cond = parse_or_cond(l, a);
    if (w->where_cond == IDX_NONE) return -1;

    /* also fill legacy where_column/where_value for backward compat with index lookup */
    struct condition *wc = &COND(a, w->where_cond);
    if (wc->type == COND_COMPARE && wc->op == CMP_EQ) {
        w->where_column = wc->column;
        w->where_value = wc->value;
    }
    return 0;
}

/* parse optional GROUP BY col HAVING ... ORDER BY col [ASC|DESC] LIMIT n OFFSET n */
static void parse_order_limit(struct lexer *l, struct query_arena *a, struct query_select *s)
{
    struct token peek = lexer_peek(l);

    /* GROUP BY col [, col ...] | GROUP BY ROLLUP(...) | GROUP BY CUBE(...) */
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "GROUP")) {
        lexer_next(l);
        struct token by = lexer_next(l);
        if (by.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(by.value, "BY")) {
            s->has_group_by = 1;
            uint32_t gb_start = (uint32_t)a->svs.count;
            uint32_t gb_count = 0;
            /* check for ROLLUP(...) or CUBE(...) */
            peek = lexer_peek(l);
            if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "ROLLUP")) {
                lexer_next(l); /* consume ROLLUP */
                s->group_by_rollup = 1;
                lexer_next(l); /* consume ( */
                for (;;) {
                    struct token col = lexer_next(l);
                    if (col.type == TOK_RPAREN) break;
                    if (col.type == TOK_COMMA) continue;
                    if (col.type == TOK_IDENTIFIER || col.type == TOK_KEYWORD) {
                        sv colsv = consume_identifier(l, col);
                        arena_push_sv(a, colsv);
                        gb_count++;
                    }
                }
            } else if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "CUBE")) {
                lexer_next(l); /* consume CUBE */
                s->group_by_cube = 1;
                lexer_next(l); /* consume ( */
                for (;;) {
                    struct token col = lexer_next(l);
                    if (col.type == TOK_RPAREN) break;
                    if (col.type == TOK_COMMA) continue;
                    if (col.type == TOK_IDENTIFIER || col.type == TOK_KEYWORD) {
                        sv colsv = consume_identifier(l, col);
                        arena_push_sv(a, colsv);
                        gb_count++;
                    }
                }
            } else {
                /* Parse GROUP BY items into a temporary buffer first, then
                 * push all expression indices into arg_indices consecutively.
                 * This avoids interleaving with function-call arg indices
                 * that parse_expr may push during expression parsing. */
                uint32_t tmp_expr[32];
                sv tmp_sv[32];
                uint32_t tmp_count = 0;
                for (;;) {
                    struct token col = lexer_peek(l);
                    if (col.type == TOK_NUMBER) {
                        /* positional GROUP BY: GROUP BY 1, 2 */
                        lexer_next(l); /* consume number */
                        long long pos = 0;
                        for (size_t k = 0; k < col.value.len; k++)
                            pos = pos * 10 + (col.value.data[k] - '0');
                        int resolved = 0;
                        /* try parsed_columns first */
                        if (pos >= 1 && s->parsed_columns_count > 0 &&
                            (uint32_t)pos <= s->parsed_columns_count) {
                            struct select_column *sc = &a->select_cols.items[s->parsed_columns_start + (uint32_t)(pos - 1)];
                            if (sc->expr_idx != IDX_NONE) {
                                struct expr *e = &EXPR(a, sc->expr_idx);
                                if (e->type == EXPR_COLUMN_REF && e->column_ref.table.len == 0) {
                                    if (tmp_count < 32) { tmp_sv[tmp_count] = e->column_ref.column; tmp_expr[tmp_count] = IDX_NONE; tmp_count++; }
                                } else {
                                    sv placeholder = sv_from("?", 1);
                                    if (tmp_count < 32) { tmp_sv[tmp_count] = placeholder; tmp_expr[tmp_count] = sc->expr_idx; tmp_count++; }
                                }
                            } else if (sc->alias.len > 0) {
                                if (tmp_count < 32) { tmp_sv[tmp_count] = sc->alias; tmp_expr[tmp_count] = IDX_NONE; tmp_count++; }
                            } else {
                                if (tmp_count < 32) { tmp_sv[tmp_count] = col.value; tmp_expr[tmp_count] = IDX_NONE; tmp_count++; }
                            }
                            resolved = 1;
                        }
                        /* fallback: resolve from legacy s->columns (comma-separated) */
                        if (!resolved && pos >= 1 && s->columns.len > 0) {
                            sv cols = s->columns;
                            long long cur = 1;
                            const char *p = cols.data;
                            const char *end = cols.data + cols.len;
                            while (p < end && cur < pos) {
                                if (*p == ',') cur++;
                                p++;
                            }
                            /* skip whitespace */
                            while (p < end && (*p == ' ' || *p == '\t')) p++;
                            const char *cstart = p;
                            while (p < end && *p != ',' && *p != ' ' && *p != '\t') p++;
                            if (cur == pos && cstart < p) {
                                if (tmp_count < 32) { tmp_sv[tmp_count] = sv_from(cstart, (size_t)(p - cstart)); tmp_expr[tmp_count] = IDX_NONE; tmp_count++; }
                                resolved = 1;
                            }
                        }
                        if (!resolved) {
                            if (tmp_count < 32) { tmp_sv[tmp_count] = col.value; tmp_expr[tmp_count] = IDX_NONE; tmp_count++; }
                        }
                        gb_count++;
                    } else {
                        col = lexer_peek(l);
                        if ((col.type == TOK_IDENTIFIER || col.type == TOK_KEYWORD) &&
                            col.value.len > 0) {
                            /* check if this is a function call (identifier followed by '(') */
                            struct lexer saved = *l;
                            struct token id_tok = lexer_next(l);
                            struct token next = lexer_peek(l);
                            if (next.type == TOK_LPAREN ||
                                next.type == TOK_PLUS || next.type == TOK_MINUS ||
                                next.type == TOK_STAR || next.type == TOK_SLASH ||
                                next.type == TOK_PERCENT || next.type == TOK_PIPE_PIPE ||
                                next.type == TOK_DOUBLE_COLON) {
                                /* expression — restore and parse as full expression */
                                *l = saved;
                                uint32_t expr_idx = parse_expr(l, a);
                                if (expr_idx != IDX_NONE) {
                                    sv placeholder = sv_from("?", 1);
                                    if (tmp_count < 32) { tmp_sv[tmp_count] = placeholder; tmp_expr[tmp_count] = expr_idx; tmp_count++; }
                                    gb_count++;
                                }
                            } else {
                                /* simple identifier (possibly table.column) */
                                sv colsv = consume_identifier(l, id_tok);
                                if (tmp_count < 32) { tmp_sv[tmp_count] = colsv; tmp_expr[tmp_count] = IDX_NONE; tmp_count++; }
                                gb_count++;
                            }
                        }
                    }
                    peek = lexer_peek(l);
                    if (peek.type != TOK_COMMA) break;
                    lexer_next(l); /* consume comma */
                }
                /* Now push all GROUP BY items consecutively into svs and arg_indices */
                gb_start = (uint32_t)a->svs.count;
                uint32_t gb_expr_start = (uint32_t)a->arg_indices.count;
                for (uint32_t gi = 0; gi < tmp_count; gi++) {
                    arena_push_sv(a, tmp_sv[gi]);
                    da_push(&a->arg_indices, tmp_expr[gi]);
                }
                s->group_by_exprs_start = gb_expr_start;
            }
            s->group_by_start = gb_start;
            s->group_by_count = gb_count;
            /* backward compat: populate single group_by_col from first item */
            if (gb_count > 0)
                s->group_by_col = ASV(a, gb_start);
        }
        peek = lexer_peek(l);
    }

    /* HAVING condition */
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "HAVING")) {
        lexer_next(l);
        s->has_having = 1;
        s->having_cond = parse_or_cond(l, a);
        peek = lexer_peek(l);
    }

    /* ORDER BY col [ASC|DESC] [, col [ASC|DESC] ...] */
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "ORDER")) {
        lexer_next(l);
        struct token by = lexer_next(l);
        if (by.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(by.value, "BY")) return;
        s->has_order_by = 1;
        uint32_t ob_start = (uint32_t)a->order_items.count;
        uint32_t ob_count = 0;
        for (;;) {
            struct order_by_item item;
            memset(&item, 0, sizeof(item));
            item.expr_idx = IDX_NONE;
            item.desc = 0;
            item.nulls_first = -1; /* unspecified — use default */
            /* try parsing as expression to handle ORDER BY a + b */
            uint32_t ei = parse_expr(l, a);
            if (ei == IDX_NONE) return;
            if (EXPR(a, ei).type == EXPR_COLUMN_REF) {
                /* simple column reference — store as column name for backward compat */
                if (EXPR(a, ei).column_ref.table.len > 0) {
                    /* reconstruct "table.column" from the two parts */
                    item.column = sv_from(EXPR(a, ei).column_ref.table.data,
                        (size_t)(EXPR(a, ei).column_ref.column.data + EXPR(a, ei).column_ref.column.len
                                 - EXPR(a, ei).column_ref.table.data));
                } else {
                    item.column = EXPR(a, ei).column_ref.column;
                }
            } else {
                /* complex expression — store expr index */
                item.expr_idx = ei;
                item.column = sv_from(NULL, 0);
            }
            peek = lexer_peek(l);
            if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "DESC")) {
                lexer_next(l);
                item.desc = 1;
            } else if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "ASC")) {
                lexer_next(l);
            }
            /* optional NULLS FIRST / NULLS LAST */
            peek = lexer_peek(l);
            if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "NULLS")) {
                lexer_next(l); /* consume NULLS */
                struct token nf = lexer_next(l);
                if (nf.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(nf.value, "FIRST"))
                    item.nulls_first = 1;
                else
                    item.nulls_first = 0; /* LAST */
            }
            arena_push_order_item(a, item);
            ob_count++;
            peek = lexer_peek(l);
            if (peek.type != TOK_COMMA) break;
            lexer_next(l); /* consume comma */
        }
        s->order_by_start = ob_start;
        s->order_by_count = ob_count;
        /* backward compat: populate single-column fields from first item */
        if (ob_count > 0) {
            s->order_by_col = a->order_items.items[ob_start].column;
            s->order_desc = a->order_items.items[ob_start].desc;
        }
        peek = lexer_peek(l);
    }

    /* LIMIT n */
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "LIMIT")) {
        lexer_next(l);
        struct token n = lexer_next(l);
        if (n.type == TOK_NUMBER) {
            s->has_limit = 1;
            int lv = sv_atoi(n.value);
            s->limit_count = lv < 0 ? 0 : lv;
        }
        peek = lexer_peek(l);
    }

    /* OFFSET n */
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "OFFSET")) {
        lexer_next(l);
        struct token n = lexer_next(l);
        if (n.type == TOK_NUMBER) {
            s->has_offset = 1;
            int ov = sv_atoi(n.value);
            s->offset_count = ov < 0 ? 0 : ov;
        }
    }
}

/* check if a function call at current position has OVER after closing paren */
static int peek_has_over(struct lexer *l)
{
    size_t saved = l->pos;
    struct token t1 = lexer_next(l); /* ( */
    if (t1.type != TOK_LPAREN) { l->pos = saved; return 0; }
    /* skip past balanced parens to find closing ) */
    int depth = 1;
    while (depth > 0) {
        struct token t = lexer_next(l);
        if (t.type == TOK_LPAREN) depth++;
        else if (t.type == TOK_RPAREN) depth--;
        else if (t.type == TOK_EOF) { l->pos = saved; return 0; }
    }
    struct token after = lexer_peek(l);
    l->pos = saved;
    return after.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(after.value, "OVER");
}

/* parse a single aggregate: FUNC(col) or FUNC(expr) or FUNC(*), returns 0 on success */
static int parse_single_agg(struct lexer *l, struct query_arena *a, sv func_name, struct agg_expr *agg)
{
    agg->func = agg_from_keyword(func_name);
    agg->expr_idx = IDX_NONE;
    struct token tok = lexer_next(l); /* ( */
    if (tok.type != TOK_LPAREN) {
        arena_set_error(a, "42601", "expected '(' after aggregate function");
        return -1;
    }
    tok = lexer_peek(l);
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "DISTINCT")) {
        lexer_next(l); /* consume DISTINCT */
        agg->has_distinct = 1;
        tok = lexer_peek(l);
    }
    if (tok.type == TOK_STAR) {
        lexer_next(l); /* consume * */
        agg->column = tok.value;
    } else {
        /* parse a full expression — may be a simple column or a complex expr */
        uint32_t eidx = parse_expr(l, a);
        if (eidx == IDX_NONE) {
            arena_set_error(a, "42601", "expected expression in aggregate");
            return -1;
        }
        /* if it's a simple column ref, set column sv for backward compat */
        struct expr *e = &EXPR(a, eidx);
        if (e->type == EXPR_COLUMN_REF) {
            if (e->column_ref.table.len == 0) {
                agg->column = e->column_ref.column;
            } else {
                /* qualified ref like t.col — reconstruct "t.col" sv spanning from table start to column end */
                const char *start = e->column_ref.table.data;
                const char *end = e->column_ref.column.data + e->column_ref.column.len;
                agg->column = sv_from(start, (size_t)(end - start));
            }
        } else {
            agg->expr_idx = eidx;
            agg->column = sv_from("*", 1); /* placeholder — expr_idx takes precedence */
        }
    }
    /* STRING_AGG(col, separator) — parse the separator argument */
    tok = lexer_peek(l);
    if (tok.type == TOK_COMMA && agg->func == AGG_STRING_AGG) {
        lexer_next(l); /* consume comma */
        struct token sep_tok = lexer_next(l);
        if (sep_tok.type == TOK_STRING) {
            agg->separator = sep_tok.value;
        } else {
            arena_set_error(a, "42601", "expected string literal separator in STRING_AGG");
            return -1;
        }
        tok = lexer_peek(l);
    }
    tok = lexer_next(l);
    if (tok.type != TOK_RPAREN) {
        arena_set_error(a, "42601", "expected ')' after aggregate expression");
        return -1;
    }
    return 0;
}

/* parse a window function call: FUNC(...) OVER (...) */
static int parse_win_call(struct lexer *l, struct query_arena *a, sv func_name, struct win_expr *w)
{
    w->func = win_from_keyword(func_name);
    w->arg_column = sv_from(NULL, 0);
    w->offset = 1; /* default offset for LAG/LEAD */

    struct token tok = lexer_next(l); /* ( */
    if (tok.type != TOK_LPAREN) {
        arena_set_error(a, "42601", "expected '(' after window function");
        return -1;
    }
    tok = lexer_next(l);
    if (tok.type == TOK_STAR) {
        w->arg_column = tok.value;
    } else if (tok.type == TOK_IDENTIFIER || tok.type == TOK_KEYWORD) {
        w->arg_column = consume_identifier(l, tok);
    } else if (tok.type == TOK_NUMBER) {
        /* NTILE(n) — first arg is a number */
        long long v = 0;
        for (size_t k = 0; k < tok.value.len; k++) {
            int d = tok.value.data[k] - '0';
            if (v > (2147483647LL - d) / 10) { v = 2147483647LL; break; }
            v = v * 10 + d;
        }
        w->offset = (int)v;
    } else if (tok.type == TOK_RPAREN) {
        /* no args, e.g. ROW_NUMBER() */
        goto after_rparen;
    } else {
        arena_set_error(a, "42601", "unexpected token in window function args");
        return -1;
    }
    /* check for comma-separated second argument (offset for LAG/LEAD/NTH_VALUE) */
    tok = lexer_peek(l);
    if (tok.type == TOK_COMMA) {
        lexer_next(l); /* consume comma */
        tok = lexer_next(l); /* second arg (offset number) */
        if (tok.type == TOK_NUMBER) {
            long long v = 0;
            for (size_t k = 0; k < tok.value.len; k++) {
                int d = tok.value.data[k] - '0';
                if (v > (2147483647LL - d) / 10) { v = 2147483647LL; break; }
                v = v * 10 + d;
            }
            w->offset = (int)v;
        }
        /* skip any further args (e.g. default value — not yet supported) */
        tok = lexer_peek(l);
        if (tok.type == TOK_COMMA) {
            lexer_next(l); /* consume comma */
            /* skip third arg */
            int depth = 0;
            for (;;) {
                tok = lexer_peek(l);
                if (tok.type == TOK_LPAREN) { depth++; lexer_next(l); }
                else if (tok.type == TOK_RPAREN) { if (depth == 0) break; depth--; lexer_next(l); }
                else if (tok.type == TOK_EOF) break;
                else lexer_next(l);
            }
        }
    }
    tok = lexer_next(l); /* ) */
    if (tok.type != TOK_RPAREN) {
        arena_set_error(a, "42601", "expected ')' in window function");
        return -1;
    }
after_rparen:
    /* expect OVER */
    tok = lexer_peek(l);
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "OVER")) {
        arena_set_error(a, "42601", "expected OVER after window function");
        return -1;
    }
    return parse_over_clause(l, a, w);
}

static int parse_agg_list(struct lexer *l, struct query_arena *a, struct query_select *s, struct token first)
{
    uint32_t agg_start = (uint32_t)a->aggregates.count;
    uint32_t agg_count = 0;

    /* parse first aggregate: we already have the keyword token */
    struct agg_expr agg;
    memset(&agg, 0, sizeof(agg));
    agg.expr_idx = IDX_NONE;
    if (parse_single_agg(l, a, first.value, &agg) != 0) return -1;
    /* store optional AS alias */
    {
        struct token pa = lexer_peek(l);
        if (pa.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(pa.value, "AS")) {
            lexer_next(l); /* AS */
            struct token alias_tok = lexer_next(l); /* alias */
            agg.alias = alias_tok.value;
        }
    }
    arena_push_agg(a, agg);
    agg_count++;

    /* check for more comma-separated aggregates or plain columns */
    const char *col_start = NULL;
    const char *col_end = NULL;
    for (;;) {
        struct token peek = lexer_peek(l);
        if (peek.type != TOK_COMMA) break;
        lexer_next(l); /* consume comma */

        struct token tok = lexer_next(l);
        if (tok.type == TOK_KEYWORD && is_agg_keyword(tok.value)) {
            struct agg_expr a2;
            memset(&a2, 0, sizeof(a2));
            if (parse_single_agg(l, a, tok.value, &a2) != 0) return -1;
            /* store optional AS alias */
            {
                struct token pa = lexer_peek(l);
                if (pa.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(pa.value, "AS")) {
                    lexer_next(l); /* AS */
                    struct token alias_tok = lexer_next(l); /* alias */
                    a2.alias = alias_tok.value;
                }
            }
            arena_push_agg(a, a2);
            agg_count++;
        } else if (tok.type == TOK_IDENTIFIER || tok.type == TOK_KEYWORD) {
            sv id = consume_identifier(l, tok);
            if (!col_start) col_start = id.data;
            col_end = id.data + id.len;
        } else {
            arena_set_error(a, "42601", "expected aggregate or column after ','");
            return -1;
        }
    }
    if (col_start)
        s->columns = sv_from(col_start, (size_t)(col_end - col_start));

    s->aggregates_start = agg_start;
    s->aggregates_count = agg_count;
    if (col_start) s->agg_before_cols = 1;
    return 0;
}

/* --- helpers extracted from parse_select --- */

/* DISTINCT / DISTINCT ON (...) — consumes the DISTINCT keyword and optional ON clause */
static void parse_select_distinct(struct lexer *l, struct query_arena *a, struct query_select *s)
{
    struct token peek_dist = lexer_peek(l);
    if (peek_dist.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(peek_dist.value, "DISTINCT"))
        return;
    lexer_next(l);
    /* check for DISTINCT ON (...) */
    struct token peek_on = lexer_peek(l);
    if (peek_on.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek_on.value, "ON")) {
        lexer_next(l); /* consume ON */
        struct token lp = lexer_next(l); /* consume ( */
        if (lp.type == TOK_LPAREN) {
            uint32_t don_start = (uint32_t)a->svs.count;
            uint32_t don_count = 0;
            for (;;) {
                struct token col_tok = lexer_next(l);
                if (col_tok.type == TOK_RPAREN) break;
                if (col_tok.type == TOK_COMMA) continue;
                if (col_tok.type == TOK_IDENTIFIER || col_tok.type == TOK_KEYWORD || col_tok.type == TOK_STRING) {
                    arena_push_sv(a, col_tok.value);
                    don_count++;
                }
            }
            s->has_distinct_on = 1;
            s->distinct_on_start = don_start;
            s->distinct_on_count = don_count;
        }
    } else {
        s->has_distinct = 1;
    }
}

/* Parse one or more JOIN clauses after the FROM table/alias.
 * Fills s->joins_start, joins_count, and backward-compat single-join fields. */
static int parse_join_list(struct lexer *l, struct query_arena *a, struct query_select *s)
{
    uint32_t joins_start = (uint32_t)a->joins.count;
    uint32_t joins_count = 0;
    for (;;) {
        struct token peek = lexer_peek(l);
        int jtype = 0; /* 0=INNER */
        int is_natural = 0;

        /* NATURAL prefix */
        if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "NATURAL")) {
            is_natural = 1;
            lexer_next(l);
            peek = lexer_peek(l);
        }

        if (peek.type == TOK_KEYWORD && (sv_eq_ignorecase_cstr(peek.value, "LEFT") ||
                                          sv_eq_ignorecase_cstr(peek.value, "RIGHT") ||
                                          sv_eq_ignorecase_cstr(peek.value, "FULL") ||
                                          sv_eq_ignorecase_cstr(peek.value, "INNER") ||
                                          sv_eq_ignorecase_cstr(peek.value, "CROSS"))) {
            if (sv_eq_ignorecase_cstr(peek.value, "LEFT"))  jtype = 1;
            if (sv_eq_ignorecase_cstr(peek.value, "RIGHT")) jtype = 2;
            if (sv_eq_ignorecase_cstr(peek.value, "FULL"))  jtype = 3;
            if (sv_eq_ignorecase_cstr(peek.value, "CROSS")) jtype = 4;
            /* INNER leaves jtype = 0 */
            lexer_next(l);
            peek = lexer_peek(l);
            if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "OUTER")) {
                lexer_next(l);
                peek = lexer_peek(l);
            }
        }
        if (!(peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "JOIN")))
            break;
        lexer_next(l); /* consume JOIN */

        struct join_info ji = {0};
        ji.join_type = jtype;
        ji.is_natural = is_natural;
        ji.join_on_cond = IDX_NONE;
        ji.lateral_subquery_sql = IDX_NONE;

        /* check for LATERAL (SELECT ...) */
        struct token tok = lexer_next(l);
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "LATERAL")) {
            ji.is_lateral = 1;
            tok = lexer_next(l); /* should be ( */
            if (tok.type != TOK_LPAREN) {
                arena_set_error(a, "42601", "expected '(' after LATERAL");
                return -1;
            }
            const char *sq_start = l->input + l->pos;
            int depth = 1;
            struct token st;
            while (depth > 0) {
                st = lexer_next(l);
                if (st.type == TOK_LPAREN) depth++;
                else if (st.type == TOK_RPAREN) depth--;
                else if (st.type == TOK_EOF) {
                    arena_set_error(a, "42601", "unterminated LATERAL subquery");
                    return -1;
                }
            }
            const char *sq_end = st.value.data; /* points at closing ')' */
            while (sq_end > sq_start && (sq_end[-1] == ' ' || sq_end[-1] == '\n')) sq_end--;
            size_t sq_len = (size_t)(sq_end - sq_start);
            ji.lateral_subquery_sql = arena_store_string(a, sq_start, sq_len);
            /* require AS alias */
            peek = lexer_peek(l);
            if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "AS")) {
                lexer_next(l);
                tok = lexer_next(l);
                ji.join_alias = tok.value;
                ji.join_table = tok.value;
            } else if (peek.type == TOK_IDENTIFIER) {
                tok = lexer_next(l);
                ji.join_alias = tok.value;
                ji.join_table = tok.value;
            } else {
                arena_set_error(a, "42601", "expected alias after LATERAL subquery");
                return -1;
            }
        } else if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
            ji.join_table = tok.value;

            /* check for schema-qualified name: schema.table */
            peek = lexer_peek(l);
            if (peek.type == TOK_DOT) {
                lexer_next(l); /* consume dot */
                struct token tbl_tok = lexer_next(l);
                sv schema = tok.value;
                sv tbl = tbl_tok.value;
                if (sv_eq_ignorecase_cstr(schema, "pg_catalog")) {
                    ji.join_table = tbl;
                } else if (sv_eq_ignorecase_cstr(schema, "information_schema")) {
                    char resolved[256];
                    snprintf(resolved, sizeof(resolved), "information_schema_%.*s",
                             (int)tbl.len, tbl.data);
                    uint32_t si = arena_store_string(a, resolved, strlen(resolved));
                    const char *stored = a->strings.items[si];
                    ji.join_table = sv_from(stored, strlen(stored));
                } else if (sv_eq_ignorecase_cstr(schema, "public")) {
                    ji.join_table = tbl;
                } else {
                    arena_set_error(a, "3F000", "schema '%.*s' does not exist",
                                    (int)schema.len, schema.data);
                    return -1;
                }
            }

            /* optional alias */
            peek = lexer_peek(l);
            if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "AS")) {
                lexer_next(l); /* consume AS */
                tok = lexer_next(l);
                ji.join_alias = tok.value;
            } else if (peek.type == TOK_IDENTIFIER) {
                /* bare alias — but only if not a keyword we expect next */
                if (!sv_eq_ignorecase_cstr(peek.value, "ON") &&
                    !sv_eq_ignorecase_cstr(peek.value, "USING") &&
                    !sv_eq_ignorecase_cstr(peek.value, "WHERE") &&
                    !sv_eq_ignorecase_cstr(peek.value, "ORDER") &&
                    !sv_eq_ignorecase_cstr(peek.value, "GROUP") &&
                    !sv_eq_ignorecase_cstr(peek.value, "LIMIT")) {
                    tok = lexer_next(l);
                    ji.join_alias = tok.value;
                }
            }
        } else {
            arena_set_error(a, "42601", "expected table name after JOIN");
            return -1;
        }

        if (ji.is_lateral) {
            /* LATERAL JOIN — ON TRUE or no ON clause */
            peek = lexer_peek(l);
            if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "ON")) {
                lexer_next(l); /* consume ON */
                tok = lexer_next(l); /* TRUE or condition */
                /* just consume ON TRUE for now */
            }
        } else if (jtype == 4 && !is_natural) {
            /* CROSS JOIN — no ON/USING clause */
        } else if (is_natural) {
            /* NATURAL JOIN — no ON/USING clause, resolved at execution time */
        } else {
            /* ON or USING */
            peek = lexer_peek(l);
            if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "USING")) {
                lexer_next(l); /* consume USING */
                tok = lexer_next(l); /* ( */
                if (tok.type != TOK_LPAREN) {
                    arena_set_error(a, "42601", "expected '(' after USING");
                    return -1;
                }
                tok = lexer_next(l);
                ji.has_using = 1;
                ji.using_col = tok.value;
                tok = lexer_next(l); /* ) */
                if (tok.type != TOK_RPAREN) {
                    arena_set_error(a, "42601", "expected ')' after USING column");
                    return -1;
                }
            } else {
                /* ON */
                tok = lexer_next(l);
                if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "ON")) {
                    arena_set_error(a, "42601", "expected ON after JOIN table");
                    return -1;
                }

                /* parse full ON condition tree (supports AND/OR/compound) */
                ji.join_on_cond = parse_or_cond(l, a);
                if (ji.join_on_cond == IDX_NONE) {
                    arena_set_error(a, "42601", "invalid ON condition");
                    return -1;
                }

                /* backward compat: populate legacy fields from first simple condition */
                struct condition *jc = &COND(a, ji.join_on_cond);
                if (jc->type == COND_COMPARE) {
                    ji.join_left_col = jc->column;
                    ji.join_op = jc->op;
                    /* extract RHS column name from value if it's text, or from the condition */
                    if (jc->value.type == COLUMN_TYPE_TEXT && jc->value.value.as_text)
                        ji.join_right_col = sv_from(jc->value.value.as_text, strlen(jc->value.value.as_text));
                } else if (jc->type == COND_AND) {
                    /* first child of AND */
                    struct condition *first = &COND(a, jc->left);
                    if (first->type == COND_COMPARE) {
                        ji.join_left_col = first->column;
                        ji.join_op = first->op;
                    }
                }
            }
        }

        arena_push_join(a, ji);
        joins_count++;
    }
    if (joins_count > 0) {
        s->joins_start = joins_start;
        s->joins_count = joins_count;
        /* backwards compat: populate single-join fields from first join */
        s->has_join = 1;
        s->join_type = a->joins.items[joins_start].join_type;
        s->join_table = a->joins.items[joins_start].join_table;
        s->join_left_col = a->joins.items[joins_start].join_left_col;
        s->join_right_col = a->joins.items[joins_start].join_right_col;
    } else if (!s->has_join) {
        s->joins_start = joins_start;
        s->joins_count = 0;
    }
    return 0;
}

/* UNION / INTERSECT / EXCEPT [ALL] — parse set operation and capture RHS SQL.
 * Also extracts trailing ORDER BY for the combined result. */
static void parse_set_ops(struct lexer *l, struct query_arena *a, struct query_select *s)
{
    struct token peek = lexer_peek(l);
    if (!(peek.type == TOK_KEYWORD &&
        (sv_eq_ignorecase_cstr(peek.value, "UNION") ||
         sv_eq_ignorecase_cstr(peek.value, "INTERSECT") ||
         sv_eq_ignorecase_cstr(peek.value, "EXCEPT"))))
        return;

    if (sv_eq_ignorecase_cstr(peek.value, "UNION"))     s->set_op = 0;
    if (sv_eq_ignorecase_cstr(peek.value, "INTERSECT")) s->set_op = 1;
    if (sv_eq_ignorecase_cstr(peek.value, "EXCEPT"))    s->set_op = 2;
    s->has_set_op = 1;
    lexer_next(l); /* consume UNION/INTERSECT/EXCEPT */
    peek = lexer_peek(l);
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "ALL")) {
        s->set_all = 1;
        lexer_next(l);
    }
    /* capture rest of input as the RHS SQL */
    while (l->pos < strlen(l->input) && (l->input[l->pos] == ' ' || l->input[l->pos] == '\t'))
        l->pos++;
    const char *rhs_start = l->input + l->pos;
    size_t rhs_len = strlen(l->input) - l->pos;
    /* trim trailing semicolons and whitespace */
    while (rhs_len > 0 && (rhs_start[rhs_len-1] == ';' || rhs_start[rhs_len-1] == ' '
                            || rhs_start[rhs_len-1] == '\n'))
        rhs_len--;
    /* find trailing ORDER BY (not inside parens) using the lexer */
    size_t last_order = (size_t)-1;
    {
        /* make a NUL-terminated copy for the temporary lexer */
        char *tmp_sql = malloc(rhs_len + 1);
        memcpy(tmp_sql, rhs_start, rhs_len);
        tmp_sql[rhs_len] = '\0';
        struct lexer tmp_l;
        lexer_init(&tmp_l, tmp_sql);
        int pd = 0;
        for (;;) {
            size_t tok_pos = tmp_l.pos;
            struct token tk = lexer_next(&tmp_l);
            if (tk.type == TOK_EOF) break;
            if (tk.type == TOK_LPAREN) { pd++; continue; }
            if (tk.type == TOK_RPAREN) { pd--; continue; }
            if (pd == 0 && tk.type == TOK_KEYWORD &&
                sv_eq_ignorecase_cstr(tk.value, "ORDER")) {
                struct token nxt = lexer_peek(&tmp_l);
                if (nxt.type == TOK_KEYWORD &&
                    sv_eq_ignorecase_cstr(nxt.value, "BY")) {
                    last_order = tok_pos;
                }
            }
        }
        free(tmp_sql);
    }
    if (last_order != (size_t)-1) {
        /* parse the ORDER BY for the combined result */
        const char *ob_str = rhs_start + last_order;
        size_t ob_len = rhs_len - last_order;
        /* store the ORDER BY text for the combined result */
        {
            char *tmp_ob = malloc(ob_len + 1);
            memcpy(tmp_ob, ob_str, ob_len);
            tmp_ob[ob_len] = '\0';
            s->set_order_by = arena_own_string(a, tmp_ob);
        }
        rhs_len = last_order;
        /* trim whitespace before ORDER BY */
        while (rhs_len > 0 && rhs_start[rhs_len-1] == ' ') rhs_len--;
    }
    s->set_rhs_sql = arena_store_string(a, rhs_start, rhs_len);
}

static int parse_select(struct lexer *l, struct query *out, struct query_arena *a)
{
    out->query_type = QUERY_TYPE_SELECT;
    struct query_select *s = &out->select;
    /* init all uint32_t index fields to IDX_NONE */
    s->where.where_cond = IDX_NONE;
    s->having_cond = IDX_NONE;
    s->set_rhs_sql = IDX_NONE;
    s->set_order_by = IDX_NONE;
    s->from_subquery_sql = IDX_NONE;
    s->cte_name = IDX_NONE;
    s->cte_sql = IDX_NONE;
    s->gs_start_expr = IDX_NONE;
    s->gs_stop_expr = IDX_NONE;
    s->gs_step_expr = IDX_NONE;
    s->group_by_exprs_start = 0;

    /* optional DISTINCT [ON (expr, ...)] */
    parse_select_distinct(l, a, s);

    /* columns: *, aggregates, window functions, literal values, or identifiers */
    size_t col_start_pos = l->pos; /* save for parse_expr fallback */
    struct token tok = lexer_next(l);

    /* pure aggregate functions (no OVER): SUM(...), COUNT(...), AVG(...) */
    if (tok.type == TOK_KEYWORD && is_agg_keyword(tok.value) && !is_win_only_keyword(tok.value)) {
        struct token peek = lexer_peek(l);
        if (peek.type == TOK_LPAREN) {
            /* look further ahead: save lexer pos, parse func(...), check for OVER */
            size_t saved_pos = l->pos;
            struct token t1 = lexer_next(l); /* ( */
            (void)t1;
            struct token t2 = lexer_next(l); /* arg or DISTINCT */
            /* skip DISTINCT keyword if present */
            if (t2.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(t2.value, "DISTINCT"))
                t2 = lexer_next(l); /* actual arg */
            (void)t2;
            struct token t3 = lexer_next(l); /* ) or more */
            /* if t2 was ), t3 might be OVER; if t3 is ), next might be OVER */
            struct token maybe_over;
            if (t2.type == TOK_RPAREN) {
                maybe_over = t3;
            } else {
                maybe_over = lexer_peek(l);
            }
            l->pos = saved_pos; /* restore */

            int has_over = (maybe_over.type == TOK_KEYWORD &&
                            sv_eq_ignorecase_cstr(maybe_over.value, "OVER"));
            if (!has_over) {
                if (parse_agg_list(l, a, s, tok) != 0) return -1;
                tok = lexer_next(l);
                if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "FROM")) {
                    arena_set_error(a, "42601", "expected FROM after aggregates");
                    return -1;
                }
                goto parse_table_name;
            }
            /* fall through to mixed select expr parsing below */
        }
    }

    /* window function or mixed column+window list */
    if (tok.type == TOK_KEYWORD && is_win_keyword(tok.value)) {
        struct token peek = lexer_peek(l);
        if (peek.type == TOK_LPAREN) {
            int has_over = peek_has_over(l);
            if (has_over) {
                /* parse mixed select expression list starting with window func */
                uint32_t se_start = (uint32_t)a->select_exprs.count;
                uint32_t se_count = 0;
                struct select_expr se = {0};
                se.kind = SEL_WINDOW;
                if (parse_win_call(l, a, tok.value, &se.win) != 0) return -1;
                arena_push_select_expr(a, se);
                se_count++;

                for (;;) {
                    peek = lexer_peek(l);
                    if (peek.type != TOK_COMMA) break;
                    lexer_next(l); /* consume comma */
                    tok = lexer_next(l);
                    if (tok.type == TOK_KEYWORD && is_win_keyword(tok.value)
                        && peek_has_over(l)) {
                        struct select_expr se2 = {0};
                        se2.kind = SEL_WINDOW;
                        if (parse_win_call(l, a, tok.value, &se2.win) != 0) return -1;
                        /* optional AS alias */
                        struct token pa = lexer_peek(l);
                        if (pa.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(pa.value, "AS")) {
                            lexer_next(l); /* consume AS */
                            struct token alias_tok = lexer_next(l);
                            se2.alias = alias_tok.value;
                        }
                        arena_push_select_expr(a, se2);
                        se_count++;
                    } else if (tok.type == TOK_IDENTIFIER || tok.type == TOK_KEYWORD) {
                        struct select_expr se2 = {0};
                        se2.kind = SEL_COLUMN;
                        se2.column = consume_identifier(l, tok);
                        arena_push_select_expr(a, se2);
                        se_count++;
                    } else {
                        arena_set_error(a, "42601", "expected column or window function");
                        return -1;
                    }
                }
                s->select_exprs_start = se_start;
                s->select_exprs_count = se_count;
                tok = lexer_next(l);
                if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "FROM")) {
                    arena_set_error(a, "42601", "expected FROM");
                    return -1;
                }
                goto parse_table_name;
            }
            /* no OVER — fall through, will be handled as aggregate */
        }
    }

    /* identifier followed by comma then function → mixed list (window or agg) */
    if (tok.type == TOK_IDENTIFIER) {
        /* consume qualified name (e.g. t1.name) before checking for comma */
        size_t pre_qual_pos = l->pos;
        sv first_col = consume_identifier(l, tok);
        struct token peek = lexer_peek(l);
        /* skip optional AS alias to check for comma */
        if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "AS")) {
            size_t saved_as = l->pos;
            lexer_next(l); /* consume AS */
            lexer_next(l); /* consume alias */
            peek = lexer_peek(l);
            if (peek.type != TOK_COMMA) {
                /* no comma after alias — restore and fall through */
                l->pos = saved_as;
                peek = lexer_peek(l);
            }
        }
        if (peek.type == TOK_COMMA) {
            /* scan ahead to find if any aggregate/window function appears in the select list */
            int found_agg = 0, found_win = 0;
            {
                size_t scan = l->pos;
                struct lexer tmp_l = { .input = l->input, .pos = scan };
                int scan_depth = 0;
                for (;;) {
                    struct token st = lexer_next(&tmp_l);
                    if (st.type == TOK_EOF) break;
                    if (st.type == TOK_LPAREN) { scan_depth++; continue; }
                    if (st.type == TOK_RPAREN) { scan_depth--; continue; }
                    if (scan_depth == 0 && st.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(st.value, "FROM")) break;
                    if (scan_depth == 0 && st.type == TOK_KEYWORD && is_agg_keyword(st.value)) {
                        struct token sp = lexer_peek(&tmp_l);
                        if (sp.type == TOK_LPAREN) {
                            int ho = peek_has_over(&tmp_l);
                            if (ho) found_win = 1;
                            else found_agg = 1;
                        }
                    } else if (scan_depth == 0 && st.type == TOK_KEYWORD && is_win_only_keyword(st.value)) {
                        struct token sp = lexer_peek(&tmp_l);
                        if (sp.type == TOK_LPAREN) found_win = 1;
                    }
                }
            }

            if (found_win) {
                /* mixed column + window function list (may also contain plain aggregates) */
                uint32_t se_start = (uint32_t)a->select_exprs.count;
                uint32_t se_count = 0;
                uint32_t agg_start = (uint32_t)a->aggregates.count;
                uint32_t agg_count = 0;
                struct select_expr se = {0};
                se.kind = SEL_COLUMN;
                se.column = first_col;
                arena_push_select_expr(a, se);
                se_count++;

                for (;;) {
                    peek = lexer_peek(l);
                    if (peek.type != TOK_COMMA) break;
                    lexer_next(l); /* comma */
                    tok = lexer_next(l);
                    if (tok.type == TOK_KEYWORD && is_win_keyword(tok.value)
                        && peek_has_over(l)) {
                        struct select_expr se2 = {0};
                        se2.kind = SEL_WINDOW;
                        if (parse_win_call(l, a, tok.value, &se2.win) != 0) return -1;
                        /* optional AS alias */
                        struct token pa = lexer_peek(l);
                        if (pa.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(pa.value, "AS")) {
                            lexer_next(l); /* consume AS */
                            struct token alias_tok = lexer_next(l);
                            se2.alias = alias_tok.value;
                        }
                        arena_push_select_expr(a, se2);
                        se_count++;
                    } else if (tok.type == TOK_KEYWORD && is_agg_keyword(tok.value)) {
                        /* plain aggregate (no OVER) in mixed win+agg list */
                        struct agg_expr agg;
                        memset(&agg, 0, sizeof(agg));
                        if (parse_single_agg(l, a, tok.value, &agg) != 0) return -1;
                        struct token pa = lexer_peek(l);
                        if (pa.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(pa.value, "AS")) {
                            lexer_next(l); /* AS */
                            struct token alias_tok = lexer_next(l);
                            agg.alias = alias_tok.value;
                        }
                        arena_push_agg(a, agg);
                        agg_count++;
                        /* placeholder in select_exprs */
                        struct select_expr se2 = {0};
                        se2.kind = SEL_COLUMN;
                        se2.column = sv_from(NULL, 0);
                        arena_push_select_expr(a, se2);
                        se_count++;
                    } else if (tok.type == TOK_IDENTIFIER || tok.type == TOK_KEYWORD) {
                        struct select_expr se2 = {0};
                        se2.kind = SEL_COLUMN;
                        se2.column = consume_identifier(l, tok);
                        /* skip optional AS alias on plain column */
                        struct token pa = lexer_peek(l);
                        if (pa.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(pa.value, "AS")) {
                            lexer_next(l); /* AS */
                            struct token alias_tok = lexer_next(l);
                            se2.alias = alias_tok.value;
                        }
                        arena_push_select_expr(a, se2);
                        se_count++;
                    } else {
                        arena_set_error(a, "42601", "expected column or window function");
                        return -1;
                    }
                }
                s->select_exprs_start = se_start;
                s->select_exprs_count = se_count;
                s->aggregates_start = agg_start;
                s->aggregates_count = agg_count;
                tok = lexer_next(l);
                if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "FROM")) {
                    arena_set_error(a, "42601", "expected FROM");
                    return -1;
                }
                goto parse_table_name;
            } else if (found_agg) {
                /* mixed column(s) + plain aggregate list (GROUP BY case) */
                /* collect all plain columns into s->columns sv, aggregates into s->aggregates */
                const char *col_start = first_col.data;
                const char *col_end = first_col.data + first_col.len;
                uint32_t agg_start = (uint32_t)a->aggregates.count;
                uint32_t agg_count = 0;

                for (;;) {
                    peek = lexer_peek(l);
                    if (peek.type != TOK_COMMA) break;
                    lexer_next(l); /* comma */
                    tok = lexer_next(l);
                    if (tok.type == TOK_KEYWORD && is_agg_keyword(tok.value)) {
                        struct agg_expr agg;
                        memset(&agg, 0, sizeof(agg));
                        if (parse_single_agg(l, a, tok.value, &agg) != 0) return -1;
                        /* store optional AS alias */
                        struct token pa = lexer_peek(l);
                        if (pa.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(pa.value, "AS")) {
                            lexer_next(l); /* AS */
                            struct token alias_tok = lexer_next(l); /* alias */
                            agg.alias = alias_tok.value;
                        }
                        arena_push_agg(a, agg);
                        agg_count++;
                    } else if (tok.type == TOK_IDENTIFIER || tok.type == TOK_KEYWORD) {
                        sv id = consume_identifier(l, tok);
                        col_end = id.data + id.len;
                        /* if followed by (, it's a function call — skip args */
                        struct token fp = lexer_peek(l);
                        if (fp.type == TOK_LPAREN) {
                            int depth = 0;
                            for (;;) {
                                struct token ft = lexer_peek(l);
                                if (ft.type == TOK_LPAREN) { depth++; lexer_next(l); }
                                else if (ft.type == TOK_RPAREN) {
                                    lexer_next(l);
                                    if (--depth <= 0) {
                                        col_end = ft.value.data + ft.value.len;
                                        break;
                                    }
                                }
                                else if (ft.type == TOK_EOF) break;
                                else lexer_next(l);
                            }
                        }
                        /* skip optional AS alias on plain column */
                        struct token pa = lexer_peek(l);
                        if (pa.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(pa.value, "AS")) {
                            lexer_next(l); /* AS */
                            struct token alias_tok = lexer_next(l); /* alias */
                            col_end = alias_tok.value.data + alias_tok.value.len;
                        }
                    } else {
                        arena_set_error(a, "42601", "expected column or aggregate function");
                        return -1;
                    }
                }
                s->aggregates_start = agg_start;
                s->aggregates_count = agg_count;
                s->columns = sv_from(col_start, (size_t)(col_end - col_start));
                tok = lexer_next(l);
                if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "FROM")) {
                    arena_set_error(a, "42601", "expected FROM");
                    return -1;
                }
                goto parse_table_name;
            }
        } else {
            /* no agg/win found — restore lexer so parse_expr fallback works */
            l->pos = pre_qual_pos;
        }
    }

    if (tok.type == TOK_STAR) {
        s->columns = tok.value;
    } else {
        /* general column list: restore lexer and parse each column as an
         * expression AST with optional AS alias */
        l->pos = col_start_pos;
        uint32_t pc_start = (uint32_t)a->select_cols.count;
        uint32_t pc_count = 0;
        uint32_t gc_agg_start = (uint32_t)a->aggregates.count;
        uint32_t gc_agg_count = 0;
        const char *raw_col_start = l->input + l->pos;
        skip_whitespace(l);
        raw_col_start = l->input + l->pos;
        const char *raw_col_end = raw_col_start;
        for (;;) {
            /* check if next token is an aggregate keyword */
            struct token agg_peek = lexer_peek(l);
            if (agg_peek.type == TOK_KEYWORD && is_agg_keyword(agg_peek.value)) {
                struct token agg_peek2;
                size_t saved_agg = l->pos;
                lexer_next(l); /* consume agg name */
                agg_peek2 = lexer_peek(l);
                if (agg_peek2.type == TOK_LPAREN) {
                    /* it's an aggregate call — parse it */
                    struct agg_expr agg;
                    memset(&agg, 0, sizeof(agg));
                    if (parse_single_agg(l, a, agg_peek.value, &agg) != 0) return -1;
                    struct token pa = lexer_peek(l);
                    if (pa.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(pa.value, "AS")) {
                        lexer_next(l); /* AS */
                        struct token alias_tok = lexer_next(l);
                        agg.alias = alias_tok.value;
                    }
                    arena_push_agg(a, agg);
                    gc_agg_count++;
                    raw_col_end = l->input + l->pos;
                    /* placeholder in parsed_columns */
                    struct select_column sc = {0};
                    sc.expr_idx = IDX_NONE;
                    arena_push_select_col(a, sc);
                    pc_count++;
                    struct token np = lexer_peek(l);
                    if (np.type != TOK_COMMA) break;
                    lexer_next(l); /* consume comma */
                    continue;
                }
                /* not followed by ( — restore and fall through to parse_expr */
                l->pos = saved_agg;
            }
            struct select_column sc = {0};
            sc.expr_idx = parse_expr(l, a);
            if (sc.expr_idx == IDX_NONE) {
                arena_set_error(a, "42601", "expected expression in SELECT column list");
                return -1;
            }
            raw_col_end = l->input + l->pos;
            /* optional AS alias */
            struct token peek = lexer_peek(l);
            if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "AS")) {
                lexer_next(l); /* consume AS */
                struct token alias_tok = lexer_next(l);
                sc.alias = alias_tok.value;
                raw_col_end = l->input + l->pos;
            }
            arena_push_select_col(a, sc);
            pc_count++;
            peek = lexer_peek(l);
            if (peek.type != TOK_COMMA) break;
            lexer_next(l); /* consume comma */
        }
        s->parsed_columns_start = pc_start;
        s->parsed_columns_count = pc_count;
        if (gc_agg_count > 0) {
            s->aggregates_start = gc_agg_start;
            s->aggregates_count = gc_agg_count;
        }
        /* also set raw columns text for backward compat */
        while (raw_col_end > raw_col_start &&
               (raw_col_end[-1] == ' ' || raw_col_end[-1] == '\t'))
            raw_col_end--;
        s->columns = sv_from(raw_col_start, (size_t)(raw_col_end - raw_col_start));
        /* check for FROM or end of statement */
        struct token next = lexer_peek(l);
        if (next.type == TOK_EOF || next.type == TOK_SEMICOLON) {
            /* SELECT <expr>, ... with no FROM (literal select) */
            return 0;
        }
    }

    /* FROM */
    tok = lexer_next(l);
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "FROM")) {
        arena_set_error(a, "42601", "expected FROM, got '" SV_FMT "'", (int)tok.value.len, tok.value.data);
        return -1;
    }

parse_table_name:
    /* table name: identifier, quoted string, or subquery (SELECT ...) AS alias */
    tok = lexer_next(l);
    if (tok.type == TOK_LPAREN) {
        /* FROM (SELECT ...) AS alias — capture subquery SQL */
        const char *sq_start = l->input + l->pos;
        int depth = 1;
        struct token st;
        while (depth > 0) {
            st = lexer_next(l);
            if (st.type == TOK_LPAREN) depth++;
            else if (st.type == TOK_RPAREN) depth--;
            else if (st.type == TOK_EOF) {
                arena_set_error(a, "42601", "unterminated FROM subquery");
                return -1;
            }
        }
        const char *sq_end = st.value.data; /* points at closing ')' */
        while (sq_end > sq_start && (sq_end[-1] == ' ' || sq_end[-1] == '\n')) sq_end--;
        size_t sq_len = (size_t)(sq_end - sq_start);
        s->from_subquery_sql = arena_store_string(a, sq_start, sq_len);
        /* require AS alias */
        struct token as_tok = lexer_next(l);
        if (as_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(as_tok.value, "AS")) {
            tok = lexer_next(l);
            s->from_subquery_alias = tok.value;
            /* use alias as table name so downstream code can find it */
            s->table = tok.value;
        } else {
            arena_set_error(a, "42601", "expected AS alias after FROM subquery");
            // NOTE: from_subquery_sql was just malloc'd above. Safe as long as the
            // caller always calls query_free on failure (query_type is QUERY_TYPE_SELECT
            // here, so query_select_free will free it). All current callers do this.
            return -1;
        }
    } else if ((tok.type == TOK_IDENTIFIER || tok.type == TOK_KEYWORD) &&
               sv_eq_ignorecase_cstr(tok.value, "generate_series")) {
        /* FROM generate_series(start, stop [, step]) [AS alias [(col_alias)]] */
        struct token lp = lexer_next(l);
        if (lp.type != TOK_LPAREN) {
            arena_set_error(a, "42601", "expected '(' after generate_series");
            return -1;
        }
        s->has_generate_series = 1;
        s->gs_start_expr = parse_expr(l, a);
        if (s->gs_start_expr == IDX_NONE) {
            arena_set_error(a, "42601", "expected start expression in generate_series");
            return -1;
        }
        struct token comma1 = lexer_next(l);
        if (comma1.type != TOK_COMMA) {
            arena_set_error(a, "42601", "expected ',' after start in generate_series");
            return -1;
        }
        s->gs_stop_expr = parse_expr(l, a);
        if (s->gs_stop_expr == IDX_NONE) {
            arena_set_error(a, "42601", "expected stop expression in generate_series");
            return -1;
        }
        /* optional step argument */
        struct token maybe_comma = lexer_peek(l);
        if (maybe_comma.type == TOK_COMMA) {
            lexer_next(l); /* consume comma */
            s->gs_step_expr = parse_expr(l, a);
            if (s->gs_step_expr == IDX_NONE) {
                arena_set_error(a, "42601", "expected step expression in generate_series");
                return -1;
            }
        }
        struct token rp = lexer_next(l);
        if (rp.type != TOK_RPAREN) {
            arena_set_error(a, "42601", "expected ')' after generate_series arguments");
            return -1;
        }
        /* use a synthetic table name so downstream code has something */
        s->table = sv_from("generate_series", 15);
        /* optional AS alias [(col_alias)] */
        struct token pa = lexer_peek(l);
        if (pa.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(pa.value, "AS")) {
            lexer_next(l); /* consume AS */
            tok = lexer_next(l); /* alias name */
            s->gs_alias = tok.value;
            s->table = tok.value;
            /* optional (col_alias) */
            struct token lp2 = lexer_peek(l);
            if (lp2.type == TOK_LPAREN) {
                lexer_next(l); /* consume ( */
                struct token col_alias_tok = lexer_next(l);
                s->gs_col_alias = col_alias_tok.value;
                lexer_next(l); /* consume ) */
            }
        } else if (pa.type == TOK_IDENTIFIER) {
            /* bare alias */
            if (!sv_eq_ignorecase_cstr(pa.value, "WHERE") &&
                !sv_eq_ignorecase_cstr(pa.value, "ORDER") &&
                !sv_eq_ignorecase_cstr(pa.value, "GROUP") &&
                !sv_eq_ignorecase_cstr(pa.value, "LIMIT") &&
                !sv_eq_ignorecase_cstr(pa.value, "JOIN") &&
                !sv_eq_ignorecase_cstr(pa.value, "CROSS")) {
                tok = lexer_next(l);
                s->gs_alias = tok.value;
                s->table = tok.value;
            }
        }
        goto after_table_alias;
    } else if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
        s->table = tok.value;
        /* check for schema-qualified name: schema.table */
        struct token dot_peek = lexer_peek(l);
        if (dot_peek.type == TOK_DOT) {
            lexer_next(l); /* consume dot */
            struct token tbl_tok = lexer_next(l);
            /* resolve schema.table -> internal name */
            sv schema = tok.value;
            sv tbl = tbl_tok.value;
            if (sv_eq_ignorecase_cstr(schema, "pg_catalog")) {
                s->table = tbl; /* pg_catalog.pg_class -> pg_class */
            } else if (sv_eq_ignorecase_cstr(schema, "information_schema")) {
                /* information_schema.tables -> information_schema_tables */
                s->table = sv_from(schema.data, (size_t)((tbl.data + tbl.len) - schema.data));
                /* rewrite the sv to use underscore instead of dot */
                /* we store the resolved name in the arena */
                char resolved[256];
                snprintf(resolved, sizeof(resolved), "information_schema_%.*s",
                         (int)tbl.len, tbl.data);
                uint32_t si = arena_store_string(a, resolved, strlen(resolved));
                const char *stored = a->strings.items[si];
                s->table = sv_from(stored, strlen(stored));
            } else if (sv_eq_ignorecase_cstr(schema, "public")) {
                s->table = tbl; /* public.t -> t */
            } else {
                arena_set_error(a, "3F000", "schema '%.*s' does not exist",
                                (int)schema.len, schema.data);
                return -1;
            }
        }
    } else {
        arena_set_error(a, "42601", "expected table name, got '" SV_FMT "'", (int)tok.value.len, tok.value.data);
        return -1;
    }

    /* optional table alias: AS alias or just alias (identifier) */
    {
        struct token pa = lexer_peek(l);
        if (pa.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(pa.value, "AS")) {
            lexer_next(l); /* consume AS */
            tok = lexer_next(l); /* consume alias name */
            s->table_alias = tok.value;
        } else if (pa.type == TOK_IDENTIFIER) {
            /* bare alias — but only if it's not a keyword we expect next */
            if (!sv_eq_ignorecase_cstr(pa.value, "WHERE") &&
                !sv_eq_ignorecase_cstr(pa.value, "ORDER") &&
                !sv_eq_ignorecase_cstr(pa.value, "GROUP") &&
                !sv_eq_ignorecase_cstr(pa.value, "LIMIT") &&
                !sv_eq_ignorecase_cstr(pa.value, "JOIN") &&
                !sv_eq_ignorecase_cstr(pa.value, "LEFT") &&
                !sv_eq_ignorecase_cstr(pa.value, "RIGHT") &&
                !sv_eq_ignorecase_cstr(pa.value, "FULL") &&
                !sv_eq_ignorecase_cstr(pa.value, "INNER") &&
                !sv_eq_ignorecase_cstr(pa.value, "CROSS") &&
                !sv_eq_ignorecase_cstr(pa.value, "NATURAL") &&
                !sv_eq_ignorecase_cstr(pa.value, "ON") &&
                !sv_eq_ignorecase_cstr(pa.value, "UNION") &&
                !sv_eq_ignorecase_cstr(pa.value, "INTERSECT") &&
                !sv_eq_ignorecase_cstr(pa.value, "EXCEPT")) {
                tok = lexer_next(l);
                s->table_alias = tok.value;
            }
        }
    }

after_table_alias:
    /* handle comma-LATERAL syntax: FROM t1, LATERAL (SELECT ...) alias
     * Convert to implicit CROSS JOIN LATERAL before parse_join_list */
    {
        struct token comma_peek = lexer_peek(l);
        if (comma_peek.type == TOK_COMMA) {
            size_t saved_pos = l->pos;
            lexer_next(l); /* consume comma */
            struct token lat_peek = lexer_peek(l);
            if (lat_peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(lat_peek.value, "LATERAL")) {
                lexer_next(l); /* consume LATERAL */
                /* synthesize a CROSS JOIN LATERAL entry */
                struct join_info ji = {0};
                ji.join_type = 4; /* CROSS */
                ji.is_lateral = 1;
                ji.join_on_cond = IDX_NONE;
                ji.lateral_subquery_sql = IDX_NONE;
                struct token lp = lexer_next(l); /* ( */
                if (lp.type != TOK_LPAREN) {
                    arena_set_error(a, "42601", "expected '(' after LATERAL");
                    return -1;
                }
                const char *sq_start = l->input + l->pos;
                int depth = 1;
                struct token st;
                while (depth > 0) {
                    st = lexer_next(l);
                    if (st.type == TOK_LPAREN) depth++;
                    else if (st.type == TOK_RPAREN) depth--;
                    else if (st.type == TOK_EOF) {
                        arena_set_error(a, "42601", "unterminated LATERAL subquery");
                        return -1;
                    }
                }
                const char *sq_end = st.value.data;
                while (sq_end > sq_start && (sq_end[-1] == ' ' || sq_end[-1] == '\n')) sq_end--;
                ji.lateral_subquery_sql = arena_store_string(a, sq_start, (size_t)(sq_end - sq_start));
                /* require alias */
                struct token ap = lexer_peek(l);
                if (ap.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(ap.value, "AS")) {
                    lexer_next(l);
                    struct token al = lexer_next(l);
                    ji.join_alias = al.value;
                    ji.join_table = al.value;
                } else if (ap.type == TOK_IDENTIFIER) {
                    struct token al = lexer_next(l);
                    ji.join_alias = al.value;
                    ji.join_table = al.value;
                } else {
                    arena_set_error(a, "42601", "expected alias after LATERAL subquery");
                    return -1;
                }
                arena_push_join(a, ji);
                s->has_join = 1;
                s->joins_start = (uint32_t)(a->joins.count - 1);
                s->joins_count = 1;
                s->join_type = 4; /* CROSS */
            } else {
                /* not LATERAL after comma — restore position */
                l->pos = saved_pos;
            }
        }
    }

    /* optional: one or more JOIN clauses */
    if (parse_join_list(l, a, s) != 0) return -1;

    /* optional: WHERE condition */
    {
        struct token peek = lexer_peek(l);
        if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "WHERE")) {
            lexer_next(l); /* consume WHERE */
            if (parse_where_clause(l, a, &s->where) != 0) return -1;
        }
    }

    /* optional: GROUP BY / HAVING / ORDER BY / LIMIT / OFFSET */
    parse_order_limit(l, a, s);

    /* optional: UNION / INTERSECT / EXCEPT */
    parse_set_ops(l, a, s);

    return 0;
}

static int parse_value_tuple(struct lexer *l, struct row *r, struct query_arena *a)
{
    struct token tok = lexer_next(l);
    if (tok.type != TOK_LPAREN) {
        arena_set_error(a, "42601", "expected '('");
        return -1;
    }

    da_init(&r->cells);
    for (;;) {
        tok = lexer_next(l);
        struct cell c = {0};
        /* function call in VALUES: nextval('seq'), currval('seq'), etc.
         * Parse as expression, store expr index in sentinel cell (is_null=2). */
        if ((tok.type == TOK_KEYWORD || tok.type == TOK_IDENTIFIER) &&
            is_expr_func_keyword(tok.value)) {
            struct token peek_lp = lexer_peek(l);
            if (peek_lp.type == TOK_LPAREN) {
                /* back up to before the function name and parse as expression */
                size_t saved = l->pos;
                l->pos = tok.value.data - l->input;
                uint32_t ei = parse_expr(l, a);
                if (ei == IDX_NONE) {
                    l->pos = saved;
                    arena_set_error(a, "42601", "failed to parse expression in VALUES");
                    return -1;
                }
                c.type = COLUMN_TYPE_INT;
                c.is_null = 2; /* sentinel: value.as_int is an expr index */
                c.value.as_int = (int)ei;
                da_push(&r->cells, c);
                tok = lexer_next(l);
                if (tok.type == TOK_RPAREN) break;
                if (tok.type != TOK_COMMA) {
                    arena_set_error(a, "42601", "expected ',' or ')'");
                    return -1;
                }
                continue;
            }
        }
        if (tok.type == TOK_NUMBER) {
            if (sv_contains_char(tok.value, '.')) {
                c.type = COLUMN_TYPE_FLOAT;
                c.value.as_float = sv_atof(tok.value);
            } else {
                long long v = 0;
                size_t k = 0;
                int neg = 0;
                if (tok.value.len > 0 && tok.value.data[0] == '-') { neg = 1; k = 1; }
                for (; k < tok.value.len; k++)
                    v = v * 10 + (tok.value.data[k] - '0');
                if (neg) v = -v;
                if (v > 2147483647LL || v < -2147483648LL) {
                    c.type = COLUMN_TYPE_BIGINT;
                    c.value.as_bigint = v;
                } else {
                    c.type = COLUMN_TYPE_INT;
                    c.value.as_int = (int)v;
                }
            }
        } else if (tok.type == TOK_STRING) {
            c.type = COLUMN_TYPE_TEXT;
            c.value.as_text = bump_strndup(&a->bump, tok.value.data, tok.value.len);
        } else if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "NULL")) {
            c.type = COLUMN_TYPE_TEXT;
            c.is_null = 1;
            c.value.as_text = NULL;
        } else if (tok.type == TOK_KEYWORD &&
                   (sv_eq_ignorecase_cstr(tok.value, "TRUE") ||
                    sv_eq_ignorecase_cstr(tok.value, "FALSE"))) {
            c.type = COLUMN_TYPE_BOOLEAN;
            c.value.as_bool = sv_eq_ignorecase_cstr(tok.value, "TRUE") ? 1 : 0;
        } else {
            arena_set_error(a, "42601", "unexpected token '" SV_FMT "' in VALUES", (int)tok.value.len, tok.value.data);
            return -1;
        }
        da_push(&r->cells, c);

        tok = lexer_next(l);
        if (tok.type == TOK_RPAREN) break;
        if (tok.type != TOK_COMMA) {
            arena_set_error(a, "42601", "expected ',' or ')'");
            return -1;
        }
    }
    return 0;
}

static void parse_returning_clause(struct lexer *l, int *has_returning, sv *returning_columns)
{
    struct token peek = lexer_peek(l);
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "RETURNING")) {
        lexer_next(l); /* consume RETURNING */
        *has_returning = 1;
        struct token tok = lexer_next(l);
        if (tok.type == TOK_STAR) {
            *returning_columns = tok.value;
        } else {
            const char *start = tok.value.data;
            const char *end = tok.value.data + tok.value.len;
            for (;;) {
                struct token p = lexer_peek(l);
                if (p.type == TOK_COMMA) {
                    lexer_next(l);
                    tok = lexer_next(l);
                    end = tok.value.data + tok.value.len;
                } else {
                    break;
                }
            }
            *returning_columns = sv_from(start, (size_t)(end - start));
        }
    }
}

static int parse_insert(struct lexer *l, struct query *out)
{
    out->query_type = QUERY_TYPE_INSERT;
    struct query_insert *ins = &out->insert;
    ins->insert_select_sql = IDX_NONE;
    ins->cte_name = IDX_NONE;
    ins->cte_sql = IDX_NONE;
    ins->ctes_start = 0;
    ins->ctes_count = 0;

    /* INTO */
    struct token tok = lexer_next(l);
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "INTO")) {
        arena_set_error(&out->arena, "42601", "expected INTO after INSERT");
        return -1;
    }

    /* table name */
    tok = lexer_next(l);
    if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
        ins->table = tok.value;
    } else {
        arena_set_error(&out->arena, "42601", "expected table name");
        return -1;
    }

    /* optional (col, col, ...) then VALUES */
    tok = lexer_next(l);
    if (tok.type == TOK_LPAREN) {
        /* parse column list and store names */
        uint32_t ic_start = (uint32_t)out->arena.svs.count;
        uint32_t ic_count = 0;
        for (;;) {
            tok = lexer_next(l);
            if (tok.type == TOK_RPAREN) break;
            if (tok.type == TOK_EOF) {
                arena_set_error(&out->arena, "42601", "unexpected end in column list");
                return -1;
            }
            if (tok.type == TOK_COMMA) continue;
            if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING || tok.type == TOK_KEYWORD) {
                arena_push_sv(&out->arena, tok.value);
                ic_count++;
            }
        }
        ins->insert_columns_start = ic_start;
        ins->insert_columns_count = ic_count;
        tok = lexer_next(l);
    }
    /* INSERT ... SELECT ... */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "SELECT")) {
        /* capture everything from SELECT to end as insert_select_sql */
        const char *sel_start = tok.value.data;
        size_t sel_len = strlen(sel_start);
        while (sel_len > 0 && (sel_start[sel_len-1] == ';' || sel_start[sel_len-1] == ' '
                                || sel_start[sel_len-1] == '\n'))
            sel_len--;
        ins->insert_select_sql = arena_store_string(&out->arena, sel_start, sel_len);
        return 0;
    }

    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "VALUES")) {
        arena_set_error(&out->arena, "42601", "expected VALUES or SELECT");
        return -1;
    }

    /* parse one or more value tuples: (v1, v2), (v3, v4), ... */
    uint32_t ir_start = (uint32_t)out->arena.rows.count;
    uint32_t ir_count = 0;
    for (;;) {
        struct row r = {0};
        if (parse_value_tuple(l, &r, &out->arena) != 0) {
            row_free(&r);
            return -1;
        }
        arena_push_row(&out->arena, r);
        ir_count++;

        struct token peek = lexer_peek(l);
        if (peek.type == TOK_COMMA) {
            lexer_next(l); /* consume comma between tuples */
        } else {
            break;
        }
    }
    ins->insert_rows_start = ir_start;
    ins->insert_rows_count = ir_count;

    /* optional ON CONFLICT ... DO NOTHING */
    struct token peek = lexer_peek(l);
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "ON")) {
        lexer_next(l); /* consume ON */
        tok = lexer_next(l); /* CONFLICT */
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "CONFLICT")) {
            ins->has_on_conflict = 1;
            peek = lexer_peek(l);
            /* optional (column) */
            if (peek.type == TOK_LPAREN) {
                lexer_next(l); /* ( */
                tok = lexer_next(l);
                ins->conflict_column = tok.value;
                lexer_next(l); /* ) */
            }
            tok = lexer_next(l); /* DO */
            tok = lexer_next(l); /* NOTHING or UPDATE */
            if (sv_eq_ignorecase_cstr(tok.value, "NOTHING")) {
                ins->on_conflict_do_nothing = 1;
            } else if (sv_eq_ignorecase_cstr(tok.value, "UPDATE")) {
                ins->on_conflict_do_update = 1;
                tok = lexer_next(l); /* SET */
                struct query_arena *ca = &out->arena;
                uint32_t sc_start = (uint32_t)ca->set_clauses.count;
                uint32_t sc_count = 0;
                for (;;) {
                    tok = lexer_next(l); /* column name */
                    struct set_clause sc = {0};
                    sc.column = tok.value;
                    sc.expr_idx = IDX_NONE;
                    tok = lexer_next(l); /* = */
                    sc.expr_idx = parse_expr(l, ca);
                    if (sc.expr_idx == IDX_NONE) {
                        arena_set_error(&out->arena, "42601", "expected expression in ON CONFLICT SET");
                        return -1;
                    }
                    arena_push_set_clause(ca, sc);
                    sc_count++;
                    peek = lexer_peek(l);
                    if (peek.type != TOK_COMMA) break;
                    lexer_next(l); /* consume comma */
                }
                ins->conflict_set_start = sc_start;
                ins->conflict_set_count = sc_count;
            }
        }
        peek = lexer_peek(l);
    }

    parse_returning_clause(l, &ins->has_returning, &ins->returning_columns);

    return 0;
}

static enum column_type parse_column_type(sv type_name)
{
    if (sv_eq_ignorecase_cstr(type_name, "SMALLINT") ||
        sv_eq_ignorecase_cstr(type_name, "INT2") ||
        sv_eq_ignorecase_cstr(type_name, "SMALLSERIAL") ||
        sv_eq_ignorecase_cstr(type_name, "SERIAL2"))
        return COLUMN_TYPE_SMALLINT;
    if (sv_eq_ignorecase_cstr(type_name, "INT") ||
        sv_eq_ignorecase_cstr(type_name, "INTEGER") ||
        sv_eq_ignorecase_cstr(type_name, "INT4") ||
        sv_eq_ignorecase_cstr(type_name, "SERIAL"))
        return COLUMN_TYPE_INT;
    if (sv_eq_ignorecase_cstr(type_name, "FLOAT") ||
        sv_eq_ignorecase_cstr(type_name, "FLOAT8") ||
        sv_eq_ignorecase_cstr(type_name, "DOUBLE") ||
        sv_eq_ignorecase_cstr(type_name, "REAL"))
        return COLUMN_TYPE_FLOAT;
    if (sv_eq_ignorecase_cstr(type_name, "TEXT") ||
        sv_eq_ignorecase_cstr(type_name, "VARCHAR") ||
        sv_eq_ignorecase_cstr(type_name, "CHAR") ||
        sv_eq_ignorecase_cstr(type_name, "CHARACTER"))
        return COLUMN_TYPE_TEXT;
    if (sv_eq_ignorecase_cstr(type_name, "BOOLEAN") ||
        sv_eq_ignorecase_cstr(type_name, "BOOL"))
        return COLUMN_TYPE_BOOLEAN;
    if (sv_eq_ignorecase_cstr(type_name, "BIGINT") ||
        sv_eq_ignorecase_cstr(type_name, "INT8") ||
        sv_eq_ignorecase_cstr(type_name, "BIGSERIAL"))
        return COLUMN_TYPE_BIGINT;
    if (sv_eq_ignorecase_cstr(type_name, "NUMERIC") ||
        sv_eq_ignorecase_cstr(type_name, "DECIMAL"))
        return COLUMN_TYPE_NUMERIC;
    if (sv_eq_ignorecase_cstr(type_name, "DATE"))
        return COLUMN_TYPE_DATE;
    if (sv_eq_ignorecase_cstr(type_name, "TIME"))
        return COLUMN_TYPE_TIME;
    if (sv_eq_ignorecase_cstr(type_name, "TIMESTAMP"))
        return COLUMN_TYPE_TIMESTAMP;
    if (sv_eq_ignorecase_cstr(type_name, "TIMESTAMPTZ"))
        return COLUMN_TYPE_TIMESTAMPTZ;
    if (sv_eq_ignorecase_cstr(type_name, "INTERVAL"))
        return COLUMN_TYPE_INTERVAL;
    if (sv_eq_ignorecase_cstr(type_name, "UUID"))
        return COLUMN_TYPE_UUID;
    return COLUMN_TYPE_ENUM;
}

/* --- helpers extracted from parse_create --- */

/* CREATE SEQUENCE name [START WITH n] [INCREMENT BY n] [MINVALUE n] [MAXVALUE n] */
static int parse_create_sequence(struct lexer *l, struct query *out)
{
    out->query_type = QUERY_TYPE_CREATE_SEQUENCE;
    struct query_create_sequence *cs = &out->create_seq;
    cs->start_value = 1;
    cs->increment = 1;
    cs->min_value = 1;
    cs->max_value = 9223372036854775807LL;

    struct token tok = lexer_next(l);
    if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
        arena_set_error(&out->arena, "42601", "expected sequence name");
        return -1;
    }
    cs->name = tok.value;

    /* parse optional clauses */
    for (;;) {
        struct token peek = lexer_peek(l);
        if (peek.type == TOK_EOF || peek.type == TOK_SEMICOLON) break;
        if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "START")) {
            lexer_next(l); /* consume START */
            struct token w = lexer_peek(l);
            if (w.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(w.value, "WITH"))
                lexer_next(l); /* consume WITH */
            tok = lexer_next(l);
            cs->start_value = sv_atoi(tok.value);
        } else if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "INCREMENT")) {
            lexer_next(l); /* consume INCREMENT */
            struct token by = lexer_peek(l);
            if (by.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(by.value, "BY"))
                lexer_next(l); /* consume BY */
            tok = lexer_next(l);
            cs->increment = sv_atoi(tok.value);
        } else if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "MINVALUE")) {
            lexer_next(l);
            tok = lexer_next(l);
            cs->min_value = sv_atoi(tok.value);
        } else if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "MAXVALUE")) {
            lexer_next(l);
            tok = lexer_next(l);
            cs->max_value = sv_atoi(tok.value);
        } else {
            /* skip unknown clause words (e.g. NO MINVALUE, CACHE, CYCLE) */
            lexer_next(l);
        }
    }
    return 0;
}

/* CREATE [OR REPLACE] VIEW name AS SELECT ...
 * tok is the first keyword after CREATE (VIEW or OR). */
static int parse_create_view(struct lexer *l, struct query *out, struct token tok)
{
    /* handle CREATE OR REPLACE VIEW */
    if (sv_eq_ignorecase_cstr(tok.value, "OR")) {
        lexer_next(l); /* consume REPLACE */
        tok = lexer_next(l); /* consume VIEW */
    }
    out->query_type = QUERY_TYPE_CREATE_VIEW;
    struct query_create_view *cv = &out->create_view;
    cv->sql_idx = IDX_NONE;

    tok = lexer_next(l);
    if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
        arena_set_error(&out->arena, "42601", "expected view name");
        return -1;
    }
    cv->name = tok.value;

    tok = lexer_next(l); /* AS */
    /* capture everything after AS as the view SQL */
    struct token sel = lexer_peek(l);
    if (sel.type != TOK_EOF) {
        const char *sql_start = sel.value.data;
        size_t sql_len = strlen(sql_start);
        while (sql_len > 0 && (sql_start[sql_len-1] == ';' || sql_start[sql_len-1] == ' '
                                || sql_start[sql_len-1] == '\n'))
            sql_len--;
        cv->sql_idx = arena_store_string(&out->arena, sql_start, sql_len);
    }
    /* consume remaining tokens */
    while (lexer_peek(l).type != TOK_EOF) lexer_next(l);
    return 0;
}

/* CREATE TABLE name (col_name TYPE [constraints], ...) */
static int parse_create_table(struct lexer *l, struct query *out)
{
    out->query_type = QUERY_TYPE_CREATE;
    struct query_create_table *crt = &out->create_table;

    /* table name */
    struct token tok = lexer_next(l);
    if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
        crt->table = tok.value;
    } else {
        arena_set_error(&out->arena, "42601", "expected table name");
        return -1;
    }

    /* ( col_name TYPE, ... ) */
    tok = lexer_next(l);
    if (tok.type != TOK_LPAREN) {
        arena_set_error(&out->arena, "42601", "expected '(' after table name");
        return -1;
    }

    uint32_t col_start_idx = (uint32_t)out->arena.columns.count;
    uint32_t col_count = 0;
    for (;;) {
        /* column name */
        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_KEYWORD) {
            arena_set_error(&out->arena, "42601", "expected column name");
            return -1;
        }
        char *col_name = bump_strndup(&out->arena.bump, tok.value.data, tok.value.len);

        /* column type — keyword (INT/FLOAT/TEXT) or identifier (enum type name) */
        tok = lexer_next(l);
        if (tok.type != TOK_KEYWORD && tok.type != TOK_IDENTIFIER) {
            arena_set_error(&out->arena, "42601", "expected column type");
            return -1;
        }
        int is_serial = sv_eq_ignorecase_cstr(tok.value, "SERIAL") ||
                        sv_eq_ignorecase_cstr(tok.value, "BIGSERIAL") ||
                        sv_eq_ignorecase_cstr(tok.value, "SMALLSERIAL") ||
                        sv_eq_ignorecase_cstr(tok.value, "SERIAL2");
        struct column col = {
            .name = col_name,
            .type = parse_column_type(tok.value),
            .enum_type_name = NULL,
            .is_serial = is_serial,
            .serial_next = is_serial ? 1 : 0,
            .fk_table = NULL,
            .fk_column = NULL
        };
        if (is_serial) {
            col.not_null = 1;
        }
        if (col.type == COLUMN_TYPE_ENUM) {
            col.enum_type_name = bump_strndup(&out->arena.bump, tok.value.data, tok.value.len);
        }
        /* skip optional (n) or (p,s) after type name (e.g. VARCHAR(255), NUMERIC(10,2)) */
        {
            struct token peek = lexer_peek(l);
            if (peek.type == TOK_LPAREN) {
                lexer_next(l); /* consume ( */
                for (;;) {
                    tok = lexer_next(l);
                    if (tok.type == TOK_RPAREN || tok.type == TOK_EOF) break;
                }
            }
        }
        /* parse optional column constraints */
        for (;;) {
            struct token peek = lexer_peek(l);
            if (peek.type == TOK_COMMA || peek.type == TOK_RPAREN || peek.type == TOK_EOF) break;
            if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "NOT")) {
                lexer_next(l); /* consume NOT */
                struct token null_tok = lexer_next(l);
                if (null_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(null_tok.value, "NULL")) {
                    col.not_null = 1;
                }
            } else if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "DEFAULT")) {
                lexer_next(l); /* consume DEFAULT */
                struct token val_tok = lexer_next(l);
                col.has_default = 1;
                col.default_value = calloc(1, sizeof(struct cell));
                *col.default_value = parse_literal_value_arena(val_tok, &out->arena);
            } else if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "UNIQUE")) {
                lexer_next(l);
                col.is_unique = 1;
            } else if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "PRIMARY")) {
                lexer_next(l); /* consume PRIMARY */
                struct token key_tok = lexer_peek(l);
                if (key_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(key_tok.value, "KEY")) {
                    lexer_next(l); /* consume KEY */
                }
                col.is_primary_key = 1;
                col.not_null = 1;
                col.is_unique = 1;
            } else if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "REFERENCES")) {
                lexer_next(l); /* consume REFERENCES */
                struct token ref_table = lexer_next(l);
                col.fk_table = bump_strndup(&out->arena.bump, ref_table.value.data, ref_table.value.len);
                struct token lp = lexer_peek(l);
                if (lp.type == TOK_LPAREN) {
                    lexer_next(l); /* consume ( */
                    struct token ref_col = lexer_next(l);
                    col.fk_column = bump_strndup(&out->arena.bump, ref_col.value.data, ref_col.value.len);
                    lexer_next(l); /* consume ) */
                }
                /* parse optional ON DELETE/UPDATE actions */
                for (;;) {
                    struct token on_peek = lexer_peek(l);
                    if (on_peek.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(on_peek.value, "ON"))
                        break;
                    lexer_next(l); /* consume ON */
                    struct token action = lexer_next(l); /* DELETE or UPDATE */
                    int is_delete = sv_eq_ignorecase_cstr(action.value, "DELETE");
                    int is_update = sv_eq_ignorecase_cstr(action.value, "UPDATE");
                    struct token act_tok = lexer_next(l);
                    enum fk_action fka = FK_NO_ACTION;
                    if (sv_eq_ignorecase_cstr(act_tok.value, "CASCADE")) {
                        fka = FK_CASCADE;
                    } else if (sv_eq_ignorecase_cstr(act_tok.value, "RESTRICT")) {
                        fka = FK_RESTRICT;
                    } else if (sv_eq_ignorecase_cstr(act_tok.value, "SET")) {
                        struct token set_what = lexer_next(l);
                        if (sv_eq_ignorecase_cstr(set_what.value, "NULL"))
                            fka = FK_SET_NULL;
                        else if (sv_eq_ignorecase_cstr(set_what.value, "DEFAULT"))
                            fka = FK_SET_DEFAULT;
                    } else if (sv_eq_ignorecase_cstr(act_tok.value, "NO")) {
                        lexer_next(l); /* consume ACTION */
                        fka = FK_NO_ACTION;
                    }
                    if (is_delete) col.fk_on_delete = fka;
                    else if (is_update) col.fk_on_update = fka;
                }
            } else if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "CHECK")) {
                lexer_next(l); /* consume CHECK */
                /* capture CHECK(...) expression text */
                struct token lp = lexer_next(l);
                if (lp.type == TOK_LPAREN) {
                    const char *expr_start = l->input + l->pos;
                    int depth = 1;
                    while (depth > 0) {
                        tok = lexer_next(l);
                        if (tok.type == TOK_LPAREN) depth++;
                        else if (tok.type == TOK_RPAREN) depth--;
                        else if (tok.type == TOK_EOF) break;
                    }
                    /* tok is the closing ')' — expression text is between expr_start and just before it */
                    const char *expr_end = tok.value.data; /* points at ')' */
                    size_t expr_len = (size_t)(expr_end - expr_start);
                    /* trim trailing whitespace */
                    while (expr_len > 0 && (expr_start[expr_len-1] == ' ' || expr_start[expr_len-1] == '\t'))
                        expr_len--;
                    col.check_expr_sql = bump_strndup(&out->arena.bump, expr_start, expr_len);
                }
            } else {
                /* unknown constraint keyword, skip it */
                lexer_next(l);
            }
        }
        arena_push_column(&out->arena, col);
        col_count++;

        tok = lexer_next(l);
        if (tok.type == TOK_RPAREN) break;
        if (tok.type != TOK_COMMA) {
            arena_set_error(&out->arena, "42601", "expected ',' or ')' in column list");
            return -1;
        }
    }
    crt->columns_start = col_start_idx;
    crt->columns_count = col_count;

    return 0;
}

static int parse_create(struct lexer *l, struct query *out)
{
    struct token tok = lexer_next(l);

    /* CREATE SEQUENCE name [START WITH n] [INCREMENT BY n] */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "SEQUENCE"))
        return parse_create_sequence(l, out);

    /* CREATE [OR REPLACE] VIEW name AS SELECT ... */
    if (tok.type == TOK_KEYWORD &&
        (sv_eq_ignorecase_cstr(tok.value, "VIEW") ||
         sv_eq_ignorecase_cstr(tok.value, "OR")))
        return parse_create_view(l, out, tok);

    /* CREATE TYPE name AS ENUM ('val1', 'val2', ...) */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "TYPE")) {
        out->query_type = QUERY_TYPE_CREATE_TYPE;
        struct query_create_type *ct = &out->create_type;

        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            arena_set_error(&out->arena, "42601", "expected type name after CREATE TYPE");
            return -1;
        }
        ct->type_name = tok.value;

        tok = lexer_next(l);
        if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "AS")) {
            arena_set_error(&out->arena, "42601", "expected AS after type name");
            return -1;
        }

        tok = lexer_next(l);
        if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "ENUM")) {
            arena_set_error(&out->arena, "42601", "expected ENUM after AS");
            return -1;
        }

        tok = lexer_next(l);
        if (tok.type != TOK_LPAREN) {
            arena_set_error(&out->arena, "42601", "expected '(' after ENUM");
            return -1;
        }

        uint32_t ev_start = (uint32_t)out->arena.strings.count;
        uint32_t ev_count = 0;
        for (;;) {
            tok = lexer_next(l);
            if (tok.type != TOK_STRING) {
                arena_set_error(&out->arena, "42601", "expected string value in ENUM list");
                return -1;
            }
            arena_own_string(&out->arena, sv_to_cstr(tok.value));
            ev_count++;

            tok = lexer_next(l);
            if (tok.type == TOK_RPAREN) break;
            if (tok.type != TOK_COMMA) {
                arena_set_error(&out->arena, "42601", "expected ',' or ')' in ENUM list");
                return -1;
            }
        }
        ct->enum_values_start = ev_start;
        ct->enum_values_count = ev_count;

        return 0;
    }

    /* CREATE INDEX [IF NOT EXISTS] name ON table (column) */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "INDEX")) {
        out->query_type = QUERY_TYPE_CREATE_INDEX;
        struct query_create_index *ci = &out->create_index;
        ci->if_not_exists = 0;

        /* optional IF NOT EXISTS */
        tok = lexer_peek(l);
        if ((tok.type == TOK_IDENTIFIER || tok.type == TOK_KEYWORD) &&
            sv_eq_ignorecase_cstr(tok.value, "IF")) {
            lexer_next(l); /* consume IF */
            struct token not_tok = lexer_next(l);
            if ((not_tok.type == TOK_IDENTIFIER || not_tok.type == TOK_KEYWORD) &&
                sv_eq_ignorecase_cstr(not_tok.value, "NOT")) {
                struct token exists_tok = lexer_next(l);
                if ((exists_tok.type == TOK_IDENTIFIER || exists_tok.type == TOK_KEYWORD) &&
                    sv_eq_ignorecase_cstr(exists_tok.value, "EXISTS")) {
                    ci->if_not_exists = 1;
                } else {
                    arena_set_error(&out->arena, "42601", "expected EXISTS after IF NOT");
                    return -1;
                }
            } else {
                arena_set_error(&out->arena, "42601", "expected NOT after IF");
                return -1;
            }
        }

        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            arena_set_error(&out->arena, "42601", "expected index name");
            return -1;
        }
        ci->index_name = tok.value;

        tok = lexer_next(l);
        if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "ON")) {
            arena_set_error(&out->arena, "42601", "expected ON after index name");
            return -1;
        }

        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            arena_set_error(&out->arena, "42601", "expected table name after ON");
            return -1;
        }
        ci->table = tok.value;

        tok = lexer_next(l);
        if (tok.type != TOK_LPAREN) {
            arena_set_error(&out->arena, "42601", "expected '(' after table name");
            return -1;
        }

        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER) {
            arena_set_error(&out->arena, "42601", "expected column name in index");
            return -1;
        }
        ci->index_column = tok.value;

        tok = lexer_next(l);
        if (tok.type != TOK_RPAREN) {
            arena_set_error(&out->arena, "42601", "expected ')' after column name");
            return -1;
        }

        return 0;
    }

    /* CREATE TABLE ... */
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "TABLE")) {
        arena_set_error(&out->arena, "42601", "expected TABLE or INDEX after CREATE");
        return -1;
    }

    /* optional IF NOT EXISTS */
    int if_not_exists = 0;
    {
        struct token peek = lexer_peek(l);
        if ((peek.type == TOK_IDENTIFIER || peek.type == TOK_KEYWORD) &&
            sv_eq_ignorecase_cstr(peek.value, "IF")) {
            lexer_next(l); /* consume IF */
            struct token not_tok = lexer_next(l);
            if ((not_tok.type == TOK_IDENTIFIER || not_tok.type == TOK_KEYWORD) &&
                sv_eq_ignorecase_cstr(not_tok.value, "NOT")) {
                struct token exists_tok = lexer_next(l);
                if ((exists_tok.type == TOK_IDENTIFIER || exists_tok.type == TOK_KEYWORD) &&
                    sv_eq_ignorecase_cstr(exists_tok.value, "EXISTS")) {
                    if_not_exists = 1;
                } else {
                    arena_set_error(&out->arena, "42601", "expected EXISTS after IF NOT");
                    return -1;
                }
            } else {
                arena_set_error(&out->arena, "42601", "expected NOT after IF");
                return -1;
            }
        }
    }

    int rc = parse_create_table(l, out);
    if (rc == 0) out->create_table.if_not_exists = if_not_exists;
    return rc;
}

static int parse_drop(struct lexer *l, struct query *out)
{
    struct token tok = lexer_next(l);

    /* DROP TYPE name */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "TYPE")) {
        out->query_type = QUERY_TYPE_DROP_TYPE;
        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            arena_set_error(&out->arena, "42601", "expected type name after DROP TYPE");
            return -1;
        }
        out->drop_type.type_name = tok.value;
        return 0;
    }

    /* DROP SEQUENCE name */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "SEQUENCE")) {
        out->query_type = QUERY_TYPE_DROP_SEQUENCE;
        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            arena_set_error(&out->arena, "42601", "expected sequence name after DROP SEQUENCE");
            return -1;
        }
        out->drop_seq.name = tok.value;
        return 0;
    }

    /* DROP VIEW name */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "VIEW")) {
        out->query_type = QUERY_TYPE_DROP_VIEW;
        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            arena_set_error(&out->arena, "42601", "expected view name after DROP VIEW");
            return -1;
        }
        out->drop_view.name = tok.value;
        return 0;
    }

    /* DROP INDEX name */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "INDEX")) {
        out->query_type = QUERY_TYPE_DROP_INDEX;
        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            arena_set_error(&out->arena, "42601", "expected index name after DROP INDEX");
            return -1;
        }
        out->drop_index.index_name = tok.value;
        return 0;
    }

    /* DROP TABLE name */
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "TABLE")) {
        arena_set_error(&out->arena, "42601", "expected TABLE, INDEX, or TYPE after DROP");
        return -1;
    }

    out->query_type = QUERY_TYPE_DROP;
    out->drop_table.if_exists = 0;
    tok = lexer_next(l);
    /* handle IF EXISTS */
    if ((tok.type == TOK_IDENTIFIER || tok.type == TOK_KEYWORD) &&
        sv_eq_ignorecase_cstr(tok.value, "IF")) {
        struct token exists_tok = lexer_next(l);
        if ((exists_tok.type == TOK_IDENTIFIER || exists_tok.type == TOK_KEYWORD) &&
            sv_eq_ignorecase_cstr(exists_tok.value, "EXISTS")) {
            out->drop_table.if_exists = 1;
            tok = lexer_next(l);
        } else {
            arena_set_error(&out->arena, "42601", "expected EXISTS after IF");
            return -1;
        }
    }
    if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
        out->drop_table.table = tok.value;
    } else {
        arena_set_error(&out->arena, "42601", "expected table name after DROP TABLE");
        return -1;
    }

    return 0;
}

static int parse_delete(struct lexer *l, struct query *out)
{
    out->query_type = QUERY_TYPE_DELETE;
    struct query_delete *d = &out->del;
    d->where.where_cond = IDX_NONE;

    /* FROM */
    struct token tok = lexer_next(l);
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "FROM")) {
        arena_set_error(&out->arena, "42601", "expected FROM after DELETE");
        return -1;
    }

    /* table name */
    tok = lexer_next(l);
    if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
        d->table = tok.value;
    } else {
        arena_set_error(&out->arena, "42601", "expected table name");
        return -1;
    }

    /* optional WHERE */
    struct token peek = lexer_peek(l);
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "WHERE")) {
        lexer_next(l);
        if (parse_where_clause(l, &out->arena, &d->where) != 0) return -1;
    }

    parse_returning_clause(l, &d->has_returning, &d->returning_columns);

    return 0;
}

static int parse_update(struct lexer *l, struct query *out)
{
    out->query_type = QUERY_TYPE_UPDATE;
    struct query_update *u = &out->update;
    u->where.where_cond = IDX_NONE;

    /* table name */
    struct token tok = lexer_next(l);
    if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
        arena_set_error(&out->arena, "42601", "expected table name after UPDATE");
        return -1;
    }
    u->table = tok.value;

    /* SET */
    tok = lexer_next(l);
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "SET")) {
        arena_set_error(&out->arena, "42601", "expected SET after table name");
        return -1;
    }

    /* col = val [, col = val ...] */
    uint32_t sc_start = (uint32_t)out->arena.set_clauses.count;
    uint32_t sc_count = 0;
    for (;;) {
        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER) {
            arena_set_error(&out->arena, "42601", "expected column name in SET");
            return -1;
        }
        struct set_clause sc;
        memset(&sc, 0, sizeof(sc));
        sc.column = tok.value;

        tok = lexer_next(l);
        if (tok.type != TOK_EQUALS) {
            arena_set_error(&out->arena, "42601", "expected '=' in SET clause");
            return -1;
        }

        /* parse the value as an expression AST */
        sc.expr_idx = parse_expr(l, &out->arena);
        sc.value = (struct cell){0};
        arena_push_set_clause(&out->arena, sc);
        sc_count++;

        {
            struct token peek = lexer_peek(l);
            if (peek.type != TOK_COMMA) break;
            lexer_next(l); /* consume comma */
        }
    }
    u->set_clauses_start = sc_start;
    u->set_clauses_count = sc_count;

    /* optional FROM table */
    {
        struct token peek2 = lexer_peek(l);
        if (peek2.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek2.value, "FROM")) {
            lexer_next(l); /* consume FROM */
            tok = lexer_next(l);
            u->has_update_from = 1;
            u->update_from_table = tok.value;
        }
    }

    /* optional WHERE */
    {
        struct token peek2 = lexer_peek(l);
        if (peek2.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek2.value, "WHERE")) {
            lexer_next(l);
            if (u->has_update_from) {
                /* UPDATE ... FROM ... WHERE t1.col = t2.col — parse as join condition */
                tok = lexer_next(l);
                u->update_from_join_left = consume_identifier(l, tok);
                tok = lexer_next(l); /* = */
                tok = lexer_next(l);
                u->update_from_join_right = consume_identifier(l, tok);
            } else {
                if (parse_where_clause(l, &out->arena, &u->where) != 0) return -1;
            }
        }
    }

    parse_returning_clause(l, &u->has_returning, &u->returning_columns);

    return 0;
}

static int parse_alter(struct lexer *l, struct query *out)
{
    out->query_type = QUERY_TYPE_ALTER;
    struct query_alter *a = &out->alter;

    struct token tok = lexer_next(l);
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "TABLE")) {
        arena_set_error(&out->arena, "42601", "expected TABLE after ALTER");
        return -1;
    }

    tok = lexer_next(l);
    if (tok.type != TOK_IDENTIFIER && tok.type != TOK_KEYWORD && tok.type != TOK_STRING) {
        arena_set_error(&out->arena, "42601", "expected table name after ALTER TABLE");
        return -1;
    }
    a->table = tok.value;

    tok = lexer_next(l);
    if (tok.type != TOK_KEYWORD) {
        arena_set_error(&out->arena, "42601", "expected ADD/DROP/RENAME/ALTER after table name");
        return -1;
    }

    if (sv_eq_ignorecase_cstr(tok.value, "ADD")) {
        /* ALTER TABLE t ADD [COLUMN] col_name col_type [constraints...] */
        a->alter_action = ALTER_ADD_COLUMN;
        tok = lexer_next(l);
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "COLUMN"))
            tok = lexer_next(l); /* skip optional COLUMN keyword */
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_KEYWORD) {
            arena_set_error(&out->arena, "42601", "expected column name in ADD COLUMN");
            return -1;
        }
        a->alter_new_col.name = bump_strndup(&out->arena.bump, tok.value.data, tok.value.len);
        tok = lexer_next(l);
        a->alter_new_col.type = parse_column_type(tok.value);
        if (a->alter_new_col.type == COLUMN_TYPE_ENUM)
            a->alter_new_col.enum_type_name = bump_strndup(&out->arena.bump, tok.value.data, tok.value.len);
        /* skip optional (n) */
        {
            struct token peek = lexer_peek(l);
            if (peek.type == TOK_LPAREN) {
                lexer_next(l);
                for (;;) { tok = lexer_next(l); if (tok.type == TOK_RPAREN || tok.type == TOK_EOF) break; }
            }
        }
        /* optional DEFAULT value */
        {
            struct token peek = lexer_peek(l);
            if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "DEFAULT")) {
                lexer_next(l); /* consume DEFAULT */
                struct token val_tok = lexer_next(l);
                a->alter_new_col.has_default = 1;
                a->alter_new_col.default_value = calloc(1, sizeof(struct cell));
                *a->alter_new_col.default_value = parse_literal_value_arena(val_tok, &out->arena);
            }
        }
        return 0;
    }

    if (sv_eq_ignorecase_cstr(tok.value, "DROP")) {
        /* ALTER TABLE t DROP [COLUMN] col_name */
        a->alter_action = ALTER_DROP_COLUMN;
        tok = lexer_next(l);
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "COLUMN"))
            tok = lexer_next(l);
        a->alter_column = tok.value;
        return 0;
    }

    if (sv_eq_ignorecase_cstr(tok.value, "RENAME")) {
        /* ALTER TABLE t RENAME [COLUMN] old_name TO new_name */
        a->alter_action = ALTER_RENAME_COLUMN;
        tok = lexer_next(l);
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "COLUMN"))
            tok = lexer_next(l);
        a->alter_column = tok.value;
        tok = lexer_next(l); /* TO */
        if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "TO")) {
            arena_set_error(&out->arena, "42601", "expected TO in RENAME COLUMN");
            return -1;
        }
        tok = lexer_next(l);
        a->alter_new_name = tok.value;
        return 0;
    }

    if (sv_eq_ignorecase_cstr(tok.value, "ALTER")) {
        /* ALTER TABLE t ALTER [COLUMN] col_name TYPE new_type */
        a->alter_action = ALTER_COLUMN_TYPE;
        tok = lexer_next(l);
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "COLUMN"))
            tok = lexer_next(l);
        a->alter_column = tok.value;
        tok = lexer_next(l); /* TYPE or SET */
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "TYPE")) {
            tok = lexer_next(l);
            a->alter_new_col.type = parse_column_type(tok.value);
            /* skip optional (n) */
            struct token peek = lexer_peek(l);
            if (peek.type == TOK_LPAREN) {
                lexer_next(l);
                for (;;) { tok = lexer_next(l); if (tok.type == TOK_RPAREN || tok.type == TOK_EOF) break; }
            }
        }
        return 0;
    }

    arena_set_error(&out->arena, "42601", "unsupported ALTER TABLE action");
    return -1;
}

static int query_parse_internal(const char *sql, struct query *out);

int query_parse(const char *sql, struct query *out)
{
    memset(out, 0, sizeof(*out));
    query_arena_init(&out->arena);
    return query_parse_internal(sql, out);
}

int query_parse_into(const char *sql, struct query *out, struct query_arena *arena)
{
    memset(out, 0, sizeof(*out));
    query_arena_reset(arena);
    out->arena = *arena;  /* shallow copy — pointers transfer */
    int rc = query_parse_internal(sql, out);
    *arena = out->arena;  /* copy back any capacity growth */
    return rc;
}

static int query_parse_internal(const char *sql, struct query *out)
{
    struct lexer l;
    lexer_init(&l, sql);

    struct token tok = lexer_next(&l);
    if (tok.type != TOK_KEYWORD) {
        arena_set_error(&out->arena, "42601", "expected SQL keyword, got '" SV_FMT "'", (int)tok.value.len, tok.value.data);
        return -1;
    }

    /* With [RECURSIVE] name AS (...) [, name AS (...) ...] SELECT ... (CTE) */
    if (sv_eq_ignorecase_cstr(tok.value, "WITH")) {
        struct query_select *s = &out->select;
        s->cte_name = IDX_NONE;
        s->cte_sql = IDX_NONE;
        struct query_arena *a = &out->arena;
        tok = lexer_next(&l);

        /* optional RECURSIVE keyword */
        int is_recursive = 0;
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "RECURSIVE")) {
            is_recursive = 1;
            s->has_recursive_cte = 1;
            tok = lexer_next(&l);
        }

        uint32_t ctes_start = (uint32_t)a->ctes.count;
        uint32_t ctes_count = 0;

        /* set query_type early so query_free takes the SELECT branch on error */
        out->query_type = QUERY_TYPE_SELECT;

        /* parse one or more CTE definitions: name AS (...) [, ...] */
        for (;;) {
            if (tok.type != TOK_IDENTIFIER && tok.type != TOK_KEYWORD) {
                arena_set_error(&out->arena, "42601", "expected CTE name after WITH");
                return -1;
            }
            struct cte_def cte = {0};
            char *name_str = sv_to_cstr(tok.value);
            cte.name_idx = arena_own_string(a, name_str);
            cte.is_recursive = is_recursive;

            tok = lexer_next(&l); /* AS */
            if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "AS")) {
                arena_set_error(&out->arena, "42601", "expected AS after CTE name");
                return -1;
            }
            tok = lexer_next(&l); /* ( */
            if (tok.type != TOK_LPAREN) {
                arena_set_error(&out->arena, "42601", "expected '(' after AS in CTE");
                return -1;
            }
            const char *cte_start = l.input + l.pos;
            int depth = 1;
            while (depth > 0) {
                tok = lexer_next(&l);
                if (tok.type == TOK_LPAREN) depth++;
                else if (tok.type == TOK_RPAREN) depth--;
                else if (tok.type == TOK_EOF) {
                    arena_set_error(&out->arena, "42601", "unterminated CTE");
                    return -1;
                }
            }
            const char *cte_end = tok.value.data; /* points at ')' */
            size_t cte_len = (size_t)(cte_end - cte_start);
            while (cte_len > 0 && (cte_start[cte_len-1] == ' ' || cte_start[cte_len-1] == '\n'))
                cte_len--;
            cte.sql_idx = arena_store_string(a, cte_start, cte_len);

            arena_push_cte(a, cte);
            ctes_count++;

            /* backward compat: also populate legacy single-CTE fields for first CTE */
            if (ctes_count == 1) {
                s->cte_name = arena_store_string(a, ASTRING(a, cte.name_idx), strlen(ASTRING(a, cte.name_idx)));
                s->cte_sql = arena_store_string(a, ASTRING(a, cte.sql_idx), strlen(ASTRING(a, cte.sql_idx)));
            }

            /* check for comma (more CTEs) or SELECT */
            tok = lexer_next(&l);
            if (tok.type == TOK_COMMA) {
                tok = lexer_next(&l); /* next CTE name */
                continue;
            }
            break;
        }
        /* now parse the main statement: SELECT or INSERT */
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "INSERT")) {
            /* WITH ... INSERT INTO t2 SELECT ... FROM src
             * Save CTE values before parse_insert overwrites the union. */
            uint32_t saved_cte_name = s->cte_name;
            uint32_t saved_cte_sql = s->cte_sql;
            int rc = parse_insert(&l, out);
            if (rc != 0) return rc;
            /* attach CTE info to the insert query */
            out->insert.cte_name = saved_cte_name;
            out->insert.cte_sql = saved_cte_sql;
            out->insert.ctes_start = ctes_start;
            out->insert.ctes_count = ctes_count;
            return 0;
        }
        if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "SELECT")) {
            arena_set_error(&out->arena, "42601", "expected SELECT or INSERT after CTE");
            return -1;
        }
        /* save CTE values that parse_select will overwrite */
        uint32_t saved_cte_name = s->cte_name;
        uint32_t saved_cte_sql = s->cte_sql;
        int saved_has_recursive = s->has_recursive_cte;
        int rc = parse_select(&l, out, &out->arena);
        /* restore CTE fields */
        s->ctes_start = ctes_start;
        s->ctes_count = ctes_count;
        s->cte_name = saved_cte_name;
        s->cte_sql = saved_cte_sql;
        s->has_recursive_cte = saved_has_recursive;
        return rc;
    }

    if (sv_eq_ignorecase_cstr(tok.value, "CREATE"))
        return parse_create(&l, out);
    if (sv_eq_ignorecase_cstr(tok.value, "DROP"))
        return parse_drop(&l, out);
    if (sv_eq_ignorecase_cstr(tok.value, "SELECT"))
        return parse_select(&l, out, &out->arena);
    if (sv_eq_ignorecase_cstr(tok.value, "INSERT"))
        return parse_insert(&l, out);
    if (sv_eq_ignorecase_cstr(tok.value, "DELETE"))
        return parse_delete(&l, out);
    if (sv_eq_ignorecase_cstr(tok.value, "UPDATE"))
        return parse_update(&l, out);
    if (sv_eq_ignorecase_cstr(tok.value, "ALTER"))
        return parse_alter(&l, out);
    if (sv_eq_ignorecase_cstr(tok.value, "BEGIN")) {
        out->query_type = QUERY_TYPE_BEGIN;
        return 0;
    }
    if (sv_eq_ignorecase_cstr(tok.value, "COMMIT")) {
        out->query_type = QUERY_TYPE_COMMIT;
        return 0;
    }
    if (sv_eq_ignorecase_cstr(tok.value, "ROLLBACK")) {
        out->query_type = QUERY_TYPE_ROLLBACK;
        return 0;
    }
    if (sv_eq_ignorecase_cstr(tok.value, "TRUNCATE")) {
        out->query_type = QUERY_TYPE_TRUNCATE;
        /* optional TABLE keyword */
        tok = lexer_peek(&l);
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "TABLE"))
            lexer_next(&l);
        tok = lexer_next(&l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING && tok.type != TOK_KEYWORD) {
            arena_set_error(&out->arena, "42601", "expected table name after TRUNCATE");
            return -1;
        }
        out->del.table = tok.value;
        out->del.where.has_where = 0;
        out->del.where.where_cond = IDX_NONE;
        return 0;
    }

    if (sv_eq_ignorecase_cstr(tok.value, "COPY")) {
        out->query_type = QUERY_TYPE_COPY;
        /* COPY table_name FROM/TO STDIN/STDOUT [WITH CSV [HEADER]] */
        tok = lexer_next(&l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_KEYWORD) {
            arena_set_error(&out->arena, "42601", "expected table name after COPY");
            return -1;
        }
        out->copy.table = tok.value;
        tok = lexer_next(&l);
        if (tok.type != TOK_KEYWORD) {
            arena_set_error(&out->arena, "42601", "expected FROM or TO after table name");
            return -1;
        }
        if (sv_eq_ignorecase_cstr(tok.value, "FROM"))
            out->copy.is_from = 1;
        else if (sv_eq_ignorecase_cstr(tok.value, "TO"))
            out->copy.is_from = 0;
        else {
            arena_set_error(&out->arena, "42601", "expected FROM or TO after table name");
            return -1;
        }
        tok = lexer_next(&l);
        /* STDIN or STDOUT */
        if (tok.type != TOK_KEYWORD ||
            (!sv_eq_ignorecase_cstr(tok.value, "STDIN") &&
             !sv_eq_ignorecase_cstr(tok.value, "STDOUT"))) {
            arena_set_error(&out->arena, "42601", "expected STDIN or STDOUT");
            return -1;
        }
        /* optional WITH CSV [HEADER] */
        out->copy.is_csv = 0;
        out->copy.has_header = 0;
        tok = lexer_peek(&l);
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "WITH")) {
            lexer_next(&l);
            tok = lexer_peek(&l);
        }
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "CSV")) {
            lexer_next(&l);
            out->copy.is_csv = 1;
            tok = lexer_peek(&l);
            if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "HEADER")) {
                lexer_next(&l);
                out->copy.has_header = 1;
            }
        }
        return 0;
    }

    if (sv_eq_ignorecase_cstr(tok.value, "EXPLAIN")) {
        out->query_type = QUERY_TYPE_EXPLAIN;
        out->explain.has_analyze = 0;
        /* optional ANALYZE keyword */
        tok = lexer_peek(&l);
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "ANALYZE")) {
            lexer_next(&l);
            out->explain.has_analyze = 1;
        }
        /* capture remaining SQL text as inner_sql */
        const char *rest = l.input + l.pos;
        while (*rest == ' ' || *rest == '\t' || *rest == '\n' || *rest == '\r') rest++;
        size_t rest_len = strlen(rest);
        /* strip trailing semicolons and whitespace */
        while (rest_len > 0 && (rest[rest_len - 1] == ';' || rest[rest_len - 1] == ' ' ||
               rest[rest_len - 1] == '\n' || rest[rest_len - 1] == '\r'))
            rest_len--;
        out->explain.inner_sql = sv_from(rest, rest_len);
        return 0;
    }

    /* SET ... / RESET ... / DISCARD ... — silently accept as no-op */
    if (sv_eq_ignorecase_cstr(tok.value, "SET") ||
        sv_eq_ignorecase_cstr(tok.value, "RESET") ||
        sv_eq_ignorecase_cstr(tok.value, "DISCARD") ||
        sv_eq_ignorecase_cstr(tok.value, "DEALLOCATE")) {
        out->query_type = QUERY_TYPE_SET;
        /* consume all remaining tokens */
        while (lexer_next(&l).type != TOK_EOF) {}
        return 0;
    }

    /* SHOW parameter */
    if (sv_eq_ignorecase_cstr(tok.value, "SHOW")) {
        out->query_type = QUERY_TYPE_SHOW;
        tok = lexer_next(&l);
        if (tok.type == TOK_EOF) {
            arena_set_error(&out->arena, "42601", "expected parameter name after SHOW");
            return -1;
        }
        out->show.parameter = tok.value;
        return 0;
    }

    arena_set_error(&out->arena, "42601", "unsupported statement '" SV_FMT "'", (int)tok.value.len, tok.value.data);
    return -1;
}

/* ---------------------------------------------------------------------------
 * Arena-based destroy — replaces all recursive free functions.
 * All parser-allocated memory lives in the arena; a single destroy call
 * frees everything.
 * ------------------------------------------------------------------------- */

void query_free(struct query *q)
{
    /* ALTER ADD COLUMN default_value is heap-allocated (calloc) — free before arena */
    if (q->query_type == QUERY_TYPE_ALTER && q->alter.alter_new_col.default_value) {
        cell_free_text(q->alter.alter_new_col.default_value);
        free(q->alter.alter_new_col.default_value);
        q->alter.alter_new_col.default_value = NULL;
    }
    /* CREATE TABLE column default_values are heap-allocated (calloc) — free before arena */
    for (size_t i = 0; i < q->arena.columns.count; i++) {
        if (q->arena.columns.items[i].default_value) {
            cell_free_text(q->arena.columns.items[i].default_value);
            free(q->arena.columns.items[i].default_value);
            q->arena.columns.items[i].default_value = NULL;
        }
    }
    query_arena_destroy(&q->arena);
}
