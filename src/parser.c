#include "parser.h"
#include "stringview.h"
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
        "SUM", "COUNT", "AVG", "MIN", "MAX",
        "ROW_NUMBER", "RANK", "OVER", "PARTITION", "BY", "ORDER",
        "UPDATE", "SET", "AND", "OR", "NOT", "NULL", "IS",
        "LIMIT", "OFFSET", "ASC", "DESC", "GROUP", "HAVING",
        "INT", "INTEGER", "INT4", "SERIAL", "FLOAT", "FLOAT8", "DOUBLE", "REAL", "TEXT",
        "VARCHAR", "CHAR", "CHARACTER", "BOOLEAN", "BOOL",
        "BIGINT", "INT8", "BIGSERIAL", "NUMERIC", "DECIMAL",
        "DATE", "TIMESTAMP", "TIMESTAMPTZ", "UUID",
        "DISTINCT", "IN", "BETWEEN", "LIKE", "ILIKE",
        "LEFT", "RIGHT", "FULL", "OUTER", "COALESCE", "CASE",
        "WHEN", "THEN", "ELSE", "END", "TRUE", "FALSE",
        "PRIMARY", "KEY", "DEFAULT", "CHECK", "UNIQUE",
        "ALTER", "ADD", "RENAME", "COLUMN", "TO",
        "BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION",
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
    if (c == '=' ) {
        tok.type = TOK_EQUALS;
        tok.value = sv_from(&l->input[l->pos], 1);
        l->pos++;
        return tok;
    }
    if (c == '!' && l->input[l->pos + 1] == '=') {
        tok.type = TOK_NOT_EQUALS;
        tok.value = sv_from(&l->input[l->pos], 2);
        l->pos += 2;
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

    /* quoted string: "..." or '...' */
    if (c == '"' || c == '\'') {
        char quote = c;
        l->pos++;
        size_t start = l->pos;
        while (l->input[l->pos] && l->input[l->pos] != quote)
            l->pos++;
        tok.value = sv_from(l->input + start, l->pos - start);
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
        || sv_eq_ignorecase_cstr(word, "MAX");
}

static int is_win_keyword(sv word)
{
    return sv_eq_ignorecase_cstr(word, "ROW_NUMBER")
        || sv_eq_ignorecase_cstr(word, "RANK")
        || is_agg_keyword(word);
}

static int is_win_only_keyword(sv word)
{
    return sv_eq_ignorecase_cstr(word, "ROW_NUMBER")
        || sv_eq_ignorecase_cstr(word, "RANK");
}

static enum agg_func agg_from_keyword(sv word)
{
    if (sv_eq_ignorecase_cstr(word, "SUM"))   return AGG_SUM;
    if (sv_eq_ignorecase_cstr(word, "COUNT")) return AGG_COUNT;
    if (sv_eq_ignorecase_cstr(word, "AVG"))   return AGG_AVG;
    if (sv_eq_ignorecase_cstr(word, "MIN"))   return AGG_MIN;
    if (sv_eq_ignorecase_cstr(word, "MAX"))   return AGG_MAX;
    return AGG_NONE;
}

static enum win_func win_from_keyword(sv word)
{
    if (sv_eq_ignorecase_cstr(word, "ROW_NUMBER")) return WIN_ROW_NUMBER;
    if (sv_eq_ignorecase_cstr(word, "RANK"))       return WIN_RANK;
    if (sv_eq_ignorecase_cstr(word, "SUM"))        return WIN_SUM;
    if (sv_eq_ignorecase_cstr(word, "COUNT"))      return WIN_COUNT;
    if (sv_eq_ignorecase_cstr(word, "AVG"))        return WIN_AVG;
    return WIN_ROW_NUMBER;
}

/* parse OVER (PARTITION BY col ORDER BY col) */
static int parse_over_clause(struct lexer *l, struct win_expr *w)
{
    struct token tok = lexer_next(l); /* consume OVER */
    tok = lexer_next(l); /* ( */
    if (tok.type != TOK_LPAREN) {
        fprintf(stderr, "parse error: expected '(' after OVER\n");
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
                fprintf(stderr, "parse error: expected BY after PARTITION\n");
                return -1;
            }
            tok = lexer_next(l);
            if (tok.type != TOK_IDENTIFIER) {
                fprintf(stderr, "parse error: expected column after PARTITION BY\n");
                return -1;
            }
            w->has_partition = 1;
            w->partition_col = tok.value;
        } else if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "ORDER")) {
            lexer_next(l); /* ORDER */
            tok = lexer_next(l); /* BY */
            if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "BY")) {
                fprintf(stderr, "parse error: expected BY after ORDER\n");
                return -1;
            }
            tok = lexer_next(l);
            if (tok.type != TOK_IDENTIFIER) {
                fprintf(stderr, "parse error: expected column after ORDER BY\n");
                return -1;
            }
            w->has_order = 1;
            w->order_col = tok.value;
        } else {
            fprintf(stderr, "parse error: unexpected token in OVER clause\n");
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
           t == TOK_LESS_EQ || t == TOK_GREATER_EQ;
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
        case TOK_EOF:
        case TOK_UNKNOWN:
            return CMP_EQ;
    }
}

static struct cell parse_literal_value(struct token tok)
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
        c.value.as_text = sv_to_cstr(tok.value);
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

/* forward declaration for recursive descent */
static struct condition *parse_or_cond(struct lexer *l);

/* parse a single comparison or grouped/negated condition */
static struct condition *parse_single_cond(struct lexer *l)
{
    struct token tok = lexer_peek(l);

    /* NOT expr */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "NOT")) {
        lexer_next(l); /* consume NOT */
        struct condition *inner = parse_single_cond(l);
        if (!inner) return NULL;
        struct condition *node = calloc(1, sizeof(*node));
        node->type = COND_NOT;
        node->left = inner;
        return node;
    }

    /* ( expr ) — parenthesized sub-expression */
    if (tok.type == TOK_LPAREN) {
        lexer_next(l); /* consume ( */
        struct condition *inner = parse_or_cond(l);
        if (!inner) return NULL;
        tok = lexer_next(l);
        if (tok.type != TOK_RPAREN) {
            fprintf(stderr, "parse error: expected ')' after grouped condition\n");
            condition_free(inner);
            return NULL;
        }
        return inner;
    }

    tok = lexer_next(l);
    /* accept identifiers and keywords as column names (e.g. sum, count, avg in HAVING) */
    if (tok.type != TOK_IDENTIFIER && tok.type != TOK_KEYWORD) {
        fprintf(stderr, "parse error: expected column name in WHERE/HAVING\n");
        return NULL;
    }

    struct condition *c = calloc(1, sizeof(*c));
    c->type = COND_COMPARE;
    c->column = consume_identifier(l, tok);

    struct token op_tok = lexer_next(l);

    /* IS NULL / IS NOT NULL / IS [NOT] DISTINCT FROM */
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "IS")) {
        struct token next = lexer_next(l);
        if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "NOT")) {
            struct token next2 = lexer_peek(l);
            if (next2.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next2.value, "DISTINCT")) {
                lexer_next(l); /* consume DISTINCT */
                struct token from_tok = lexer_next(l);
                if (from_tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(from_tok.value, "FROM")) {
                    fprintf(stderr, "parse error: expected FROM after IS NOT DISTINCT\n");
                    free(c); return NULL;
                }
                c->op = CMP_IS_NOT_DISTINCT;
                struct token val_tok = lexer_next(l);
                c->value = parse_literal_value(val_tok);
                return c;
            }
            next = lexer_next(l);
            if (next.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(next.value, "NULL")) {
                fprintf(stderr, "parse error: expected NULL after IS NOT\n");
                free(c); return NULL;
            }
            c->op = CMP_IS_NOT_NULL;
        } else if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "DISTINCT")) {
            struct token from_tok = lexer_next(l);
            if (from_tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(from_tok.value, "FROM")) {
                fprintf(stderr, "parse error: expected FROM after IS DISTINCT\n");
                free(c); return NULL;
            }
            c->op = CMP_IS_DISTINCT;
            struct token val_tok = lexer_next(l);
            c->value = parse_literal_value(val_tok);
            return c;
        } else if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "NULL")) {
            c->op = CMP_IS_NULL;
        } else {
            fprintf(stderr, "parse error: expected NULL, NOT, or DISTINCT after IS\n");
            free(c); return NULL;
        }
        return c;
    }

    /* NOT IN (...) */
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "NOT")) {
        struct token next = lexer_next(l);
        if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "IN")) {
            c->op = CMP_NOT_IN;
            goto parse_in_list;
        }
        fprintf(stderr, "parse error: expected IN after NOT\n");
        free(c); return NULL;
    }

    /* IN (...) */
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "IN")) {
        c->op = CMP_IN;
parse_in_list:
        tok = lexer_next(l);
        if (tok.type != TOK_LPAREN) {
            fprintf(stderr, "parse error: expected '(' after IN\n");
            free(c); return NULL;
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
                        fprintf(stderr, "parse error: unterminated subquery\n");
                        free(c); return NULL;
                    }
                }
                /* tok is now the closing ')' */
                const char *sq_end = tok.value.data; /* points at ')' */
                size_t sq_len = (size_t)(sq_end - sq_start);
                c->subquery_sql = malloc(sq_len + 1);
                memcpy(c->subquery_sql, sq_start, sq_len);
                c->subquery_sql[sq_len] = '\0';
                /* trim trailing whitespace */
                while (sq_len > 0 && (c->subquery_sql[sq_len-1] == ' ' || c->subquery_sql[sq_len-1] == '\t'))
                    c->subquery_sql[--sq_len] = '\0';
                da_init(&c->in_values);
                return c;
            }
        }
        da_init(&c->in_values);
        for (;;) {
            tok = lexer_next(l);
            struct cell v = parse_literal_value(tok);
            da_push(&c->in_values, v);
            tok = lexer_next(l);
            if (tok.type == TOK_RPAREN) break;
            if (tok.type != TOK_COMMA) {
                fprintf(stderr, "parse error: expected ',' or ')' in IN list\n");
                for (size_t iv = 0; iv < c->in_values.count; iv++) {
                    if ((c->in_values.items[iv].type == COLUMN_TYPE_TEXT ||
                         c->in_values.items[iv].type == COLUMN_TYPE_ENUM) &&
                        c->in_values.items[iv].value.as_text)
                        free(c->in_values.items[iv].value.as_text);
                }
                da_free(&c->in_values);
                free(c); return NULL;
            }
        }
        return c;
    }

    /* BETWEEN low AND high */
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "BETWEEN")) {
        c->op = CMP_BETWEEN;
        tok = lexer_next(l);
        c->value = parse_literal_value(tok);
        tok = lexer_next(l); /* AND */
        if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "AND")) {
            fprintf(stderr, "parse error: expected AND in BETWEEN\n");
            free(c); return NULL;
        }
        tok = lexer_next(l);
        c->between_high = parse_literal_value(tok);
        return c;
    }

    /* LIKE / ILIKE */
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "LIKE")) {
        c->op = CMP_LIKE;
        tok = lexer_next(l);
        c->value = parse_literal_value(tok);
        return c;
    }
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "ILIKE")) {
        c->op = CMP_ILIKE;
        tok = lexer_next(l);
        c->value = parse_literal_value(tok);
        return c;
    }

    if (!is_cmp_token(op_tok.type)) {
        fprintf(stderr, "parse error: expected comparison operator in WHERE\n");
        free(c); return NULL;
    }
    c->op = cmp_from_token(op_tok.type);

    tok = lexer_next(l);
    c->value = parse_literal_value(tok);
    return c;
}

/* parse WHERE condition with AND/OR support (left-to-right, AND binds tighter) */
static struct condition *parse_and_cond(struct lexer *l)
{
    struct condition *left = parse_single_cond(l);
    if (!left) return NULL;

    for (;;) {
        struct token peek = lexer_peek(l);
        if (peek.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(peek.value, "AND"))
            break;
        lexer_next(l); /* consume AND */
        struct condition *right = parse_single_cond(l);
        if (!right) { condition_free(left); return NULL; }
        struct condition *node = calloc(1, sizeof(*node));
        node->type = COND_AND;
        node->left = left;
        node->right = right;
        left = node;
    }
    return left;
}

static struct condition *parse_or_cond(struct lexer *l)
{
    struct condition *left = parse_and_cond(l);
    if (!left) return NULL;

    for (;;) {
        struct token peek = lexer_peek(l);
        if (peek.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(peek.value, "OR"))
            break;
        lexer_next(l); /* consume OR */
        struct condition *right = parse_and_cond(l);
        if (!right) { condition_free(left); return NULL; }
        struct condition *node = calloc(1, sizeof(*node));
        node->type = COND_OR;
        node->left = left;
        node->right = right;
        left = node;
    }
    return left;
}

static int parse_where_clause(struct lexer *l, struct query *out)
{
    out->has_where = 1;
    out->where_cond = parse_or_cond(l);
    if (!out->where_cond) return -1;

    /* also fill legacy where_column/where_value for backward compat with index lookup */
    if (out->where_cond->type == COND_COMPARE && out->where_cond->op == CMP_EQ) {
        out->where_column = out->where_cond->column;
        out->where_value = out->where_cond->value;
    }
    return 0;
}

/* parse optional GROUP BY col HAVING ... ORDER BY col [ASC|DESC] LIMIT n OFFSET n */
static void parse_order_limit(struct lexer *l, struct query *out)
{
    struct token peek = lexer_peek(l);

    /* GROUP BY col [, col ...] */
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "GROUP")) {
        lexer_next(l);
        struct token by = lexer_next(l);
        if (by.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(by.value, "BY")) {
            out->has_group_by = 1;
            da_init(&out->group_by_cols);
            for (;;) {
                struct token col = lexer_next(l);
                if (col.type == TOK_IDENTIFIER || col.type == TOK_KEYWORD) {
                    sv colsv = consume_identifier(l, col);
                    da_push(&out->group_by_cols, colsv);
                }
                peek = lexer_peek(l);
                if (peek.type != TOK_COMMA) break;
                lexer_next(l); /* consume comma */
            }
            /* backward compat: populate single group_by_col from first item */
            if (out->group_by_cols.count > 0)
                out->group_by_col = out->group_by_cols.items[0];
        }
        peek = lexer_peek(l);
    }

    /* HAVING condition */
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "HAVING")) {
        lexer_next(l);
        out->has_having = 1;
        out->having_cond = parse_or_cond(l);
        peek = lexer_peek(l);
    }

    /* ORDER BY col [ASC|DESC] [, col [ASC|DESC] ...] */
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "ORDER")) {
        lexer_next(l);
        struct token by = lexer_next(l);
        if (by.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(by.value, "BY")) return;
        out->has_order_by = 1;
        da_init(&out->order_by_items);
        for (;;) {
            struct token col = lexer_next(l);
            if (col.type != TOK_IDENTIFIER && col.type != TOK_KEYWORD) return;
            struct order_by_item item;
            item.column = consume_identifier(l, col);
            item.desc = 0;
            peek = lexer_peek(l);
            if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "DESC")) {
                lexer_next(l);
                item.desc = 1;
            } else if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "ASC")) {
                lexer_next(l);
            }
            da_push(&out->order_by_items, item);
            peek = lexer_peek(l);
            if (peek.type != TOK_COMMA) break;
            lexer_next(l); /* consume comma */
        }
        /* backward compat: populate single-column fields from first item */
        if (out->order_by_items.count > 0) {
            out->order_by_col = out->order_by_items.items[0].column;
            out->order_desc = out->order_by_items.items[0].desc;
        }
        peek = lexer_peek(l);
    }

    /* LIMIT n */
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "LIMIT")) {
        lexer_next(l);
        struct token n = lexer_next(l);
        if (n.type == TOK_NUMBER) {
            out->has_limit = 1;
            out->limit_count = sv_atoi(n.value);
        }
        peek = lexer_peek(l);
    }

    /* OFFSET n */
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "OFFSET")) {
        lexer_next(l);
        struct token n = lexer_next(l);
        if (n.type == TOK_NUMBER) {
            out->has_offset = 1;
            out->offset_count = sv_atoi(n.value);
        }
    }
}

/* check if a function call at current position has OVER after closing paren */
static int peek_has_over(struct lexer *l)
{
    size_t saved = l->pos;
    struct token t1 = lexer_next(l); /* ( */
    if (t1.type != TOK_LPAREN) { l->pos = saved; return 0; }
    struct token t2 = lexer_next(l); /* arg or ) */
    if (t2.type == TOK_RPAREN) {
        struct token t3 = lexer_peek(l);
        l->pos = saved;
        return t3.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(t3.value, "OVER");
    }
    struct token t3 = lexer_next(l); /* ) */
    (void)t3;
    struct token t4 = lexer_peek(l);
    l->pos = saved;
    return t4.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(t4.value, "OVER");
}

/* parse a single aggregate: FUNC(col) or FUNC(*), returns 0 on success */
static int parse_single_agg(struct lexer *l, sv func_name, struct agg_expr *agg)
{
    agg->func = agg_from_keyword(func_name);
    struct token tok = lexer_next(l); /* ( */
    if (tok.type != TOK_LPAREN) {
        fprintf(stderr, "parse error: expected '(' after aggregate function\n");
        return -1;
    }
    tok = lexer_next(l);
    if (tok.type == TOK_STAR) {
        agg->column = tok.value;
    } else if (tok.type == TOK_IDENTIFIER) {
        agg->column = tok.value;
    } else {
        fprintf(stderr, "parse error: expected column or * in aggregate\n");
        return -1;
    }
    tok = lexer_next(l);
    if (tok.type != TOK_RPAREN) {
        fprintf(stderr, "parse error: expected ')' after aggregate column\n");
        return -1;
    }
    return 0;
}

/* parse a window function call: FUNC(...) OVER (...) */
static int parse_win_call(struct lexer *l, sv func_name, struct win_expr *w)
{
    w->func = win_from_keyword(func_name);
    w->arg_column = sv_from(NULL, 0);

    struct token tok = lexer_next(l); /* ( */
    if (tok.type != TOK_LPAREN) {
        fprintf(stderr, "parse error: expected '(' after window function\n");
        return -1;
    }
    tok = lexer_next(l);
    if (tok.type == TOK_STAR) {
        w->arg_column = tok.value;
    } else if (tok.type == TOK_IDENTIFIER) {
        w->arg_column = tok.value;
    } else if (tok.type == TOK_RPAREN) {
        /* no args, e.g. ROW_NUMBER() */
        goto after_rparen;
    } else {
        fprintf(stderr, "parse error: unexpected token in window function args\n");
        return -1;
    }
    tok = lexer_next(l); /* ) */
    if (tok.type != TOK_RPAREN) {
        fprintf(stderr, "parse error: expected ')' in window function\n");
        return -1;
    }
after_rparen:
    /* expect OVER */
    tok = lexer_peek(l);
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "OVER")) {
        fprintf(stderr, "parse error: expected OVER after window function\n");
        return -1;
    }
    return parse_over_clause(l, w);
}

static int parse_agg_list(struct lexer *l, struct query *out, struct token first)
{
    da_init(&out->aggregates);

    /* parse first aggregate: we already have the keyword token */
    struct agg_expr agg;
    agg.func = agg_from_keyword(first.value);

    struct token tok = lexer_next(l); /* ( */
    if (tok.type != TOK_LPAREN) {
        fprintf(stderr, "parse error: expected '(' after aggregate function\n");
        return -1;
    }
    tok = lexer_next(l); /* column or * */
    if (tok.type == TOK_STAR) {
        agg.column = tok.value;
    } else if (tok.type == TOK_IDENTIFIER) {
        agg.column = tok.value;
    } else {
        fprintf(stderr, "parse error: expected column name or * in aggregate\n");
        return -1;
    }
    tok = lexer_next(l); /* ) */
    if (tok.type != TOK_RPAREN) {
        fprintf(stderr, "parse error: expected ')' after aggregate column\n");
        return -1;
    }
    da_push(&out->aggregates, agg);

    /* check for more comma-separated aggregates */
    for (;;) {
        struct token peek = lexer_peek(l);
        if (peek.type != TOK_COMMA) break;
        lexer_next(l); /* consume comma */

        tok = lexer_next(l);
        if (tok.type != TOK_KEYWORD || !is_agg_keyword(tok.value)) {
            fprintf(stderr, "parse error: expected aggregate function after ','\n");
            return -1;
        }
        struct agg_expr a2;
        a2.func = agg_from_keyword(tok.value);

        tok = lexer_next(l);
        if (tok.type != TOK_LPAREN) {
            fprintf(stderr, "parse error: expected '(' after aggregate function\n");
            return -1;
        }
        tok = lexer_next(l);
        if (tok.type == TOK_STAR) {
            a2.column = tok.value;
        } else if (tok.type == TOK_IDENTIFIER) {
            a2.column = tok.value;
        } else {
            fprintf(stderr, "parse error: expected column name or * in aggregate\n");
            return -1;
        }
        tok = lexer_next(l);
        if (tok.type != TOK_RPAREN) {
            fprintf(stderr, "parse error: expected ')' after aggregate column\n");
            return -1;
        }
        da_push(&out->aggregates, a2);
    }

    return 0;
}

static int parse_select(struct lexer *l, struct query *out)
{
    out->query_type = QUERY_TYPE_SELECT;

    /* optional DISTINCT */
    struct token peek_dist = lexer_peek(l);
    if (peek_dist.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek_dist.value, "DISTINCT")) {
        lexer_next(l);
        out->has_distinct = 1;
    }

    /* columns: *, aggregates, window functions, literal values, or identifiers */
    struct token tok = lexer_next(l);

    /* pure aggregate functions (no OVER): SUM(...), COUNT(...), AVG(...) */
    if (tok.type == TOK_KEYWORD && is_agg_keyword(tok.value) && !is_win_only_keyword(tok.value)) {
        struct token peek = lexer_peek(l);
        if (peek.type == TOK_LPAREN) {
            /* look further ahead: save lexer pos, parse func(...), check for OVER */
            size_t saved_pos = l->pos;
            struct token t1 = lexer_next(l); /* ( */
            (void)t1;
            struct token t2 = lexer_next(l); /* arg */
            (void)t2;
            struct token t3 = lexer_next(l); /* ) or more */
            /* if t2 was ), t3 might be OVER; if t3 is ), next might be OVER */
            struct token maybe_over;
            if (t2.type == TOK_RPAREN) {
                maybe_over = lexer_peek(l);
                /* actually t3 is already the next token */
                maybe_over = t3;
            } else {
                maybe_over = lexer_peek(l);
            }
            l->pos = saved_pos; /* restore */

            int has_over = (maybe_over.type == TOK_KEYWORD &&
                            sv_eq_ignorecase_cstr(maybe_over.value, "OVER"));
            if (!has_over) {
                if (parse_agg_list(l, out, tok) != 0) return -1;
                tok = lexer_next(l);
                if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "FROM")) {
                    fprintf(stderr, "parse error: expected FROM after aggregates\n");
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
                da_init(&out->select_exprs);
                struct select_expr se;
                se.kind = SEL_WINDOW;
                if (parse_win_call(l, tok.value, &se.win) != 0) return -1;
                da_push(&out->select_exprs, se);

                for (;;) {
                    peek = lexer_peek(l);
                    if (peek.type != TOK_COMMA) break;
                    lexer_next(l); /* consume comma */
                    tok = lexer_next(l);
                    if (tok.type == TOK_KEYWORD && is_win_keyword(tok.value)
                        && peek_has_over(l)) {
                        struct select_expr se2;
                        se2.kind = SEL_WINDOW;
                        if (parse_win_call(l, tok.value, &se2.win) != 0) return -1;
                        da_push(&out->select_exprs, se2);
                    } else if (tok.type == TOK_IDENTIFIER) {
                        struct select_expr se2;
                        se2.kind = SEL_COLUMN;
                        se2.column = tok.value;
                        da_push(&out->select_exprs, se2);
                    } else {
                        fprintf(stderr, "parse error: expected column or window function\n");
                        return -1;
                    }
                }
                tok = lexer_next(l);
                if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "FROM")) {
                    fprintf(stderr, "parse error: expected FROM\n");
                    return -1;
                }
                goto parse_table_name;
            }
            /* no OVER — fall through, will be handled as aggregate */
        }
    }

    /* identifier followed by comma then function → mixed list (window or agg) */
    if (tok.type == TOK_IDENTIFIER) {
        struct token peek = lexer_peek(l);
        if (peek.type == TOK_COMMA) {
            /* scan ahead to find if any aggregate/window function appears in the select list */
            int found_agg = 0, found_win = 0;
            {
                size_t scan = l->pos;
                struct lexer tmp_l = { .input = l->input, .pos = scan };
                for (;;) {
                    struct token st = lexer_next(&tmp_l);
                    if (st.type == TOK_EOF) break;
                    if (st.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(st.value, "FROM")) break;
                    if (st.type == TOK_KEYWORD && is_agg_keyword(st.value)) {
                        struct token sp = lexer_peek(&tmp_l);
                        if (sp.type == TOK_LPAREN) {
                            int ho = peek_has_over(&tmp_l);
                            if (ho) found_win = 1;
                            else found_agg = 1;
                        }
                    } else if (st.type == TOK_KEYWORD && is_win_only_keyword(st.value)) {
                        struct token sp = lexer_peek(&tmp_l);
                        if (sp.type == TOK_LPAREN) found_win = 1;
                    }
                }
            }

            if (found_win) {
                /* mixed column + window function list */
                da_init(&out->select_exprs);
                struct select_expr se;
                se.kind = SEL_COLUMN;
                se.column = tok.value;
                da_push(&out->select_exprs, se);

                for (;;) {
                    peek = lexer_peek(l);
                    if (peek.type != TOK_COMMA) break;
                    lexer_next(l); /* comma */
                    tok = lexer_next(l);
                    if (tok.type == TOK_KEYWORD && is_win_keyword(tok.value)
                        && peek_has_over(l)) {
                        struct select_expr se2;
                        se2.kind = SEL_WINDOW;
                        if (parse_win_call(l, tok.value, &se2.win) != 0) return -1;
                        da_push(&out->select_exprs, se2);
                    } else if (tok.type == TOK_IDENTIFIER) {
                        struct select_expr se2;
                        se2.kind = SEL_COLUMN;
                        se2.column = tok.value;
                        da_push(&out->select_exprs, se2);
                    } else {
                        fprintf(stderr, "parse error: expected column or window function\n");
                        return -1;
                    }
                }
                tok = lexer_next(l);
                if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "FROM")) {
                    fprintf(stderr, "parse error: expected FROM\n");
                    return -1;
                }
                goto parse_table_name;
            } else if (found_agg) {
                /* mixed column(s) + plain aggregate list (GROUP BY case) */
                /* collect all plain columns into out->columns sv, aggregates into out->aggregates */
                const char *col_start = tok.value.data;
                const char *col_end = tok.value.data + tok.value.len;
                da_init(&out->aggregates);

                for (;;) {
                    peek = lexer_peek(l);
                    if (peek.type != TOK_COMMA) break;
                    lexer_next(l); /* comma */
                    tok = lexer_next(l);
                    if (tok.type == TOK_KEYWORD && is_agg_keyword(tok.value)) {
                        struct agg_expr agg;
                        if (parse_single_agg(l, tok.value, &agg) != 0) return -1;
                        da_push(&out->aggregates, agg);
                    } else if (tok.type == TOK_IDENTIFIER || tok.type == TOK_KEYWORD) {
                        sv id = consume_identifier(l, tok);
                        col_end = id.data + id.len;
                    } else {
                        fprintf(stderr, "parse error: expected column or aggregate function\n");
                        return -1;
                    }
                }
                out->columns = sv_from(col_start, (size_t)(col_end - col_start));
                tok = lexer_next(l);
                if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "FROM")) {
                    fprintf(stderr, "parse error: expected FROM\n");
                    return -1;
                }
                goto parse_table_name;
            }
        }
    }

    if (tok.type == TOK_STAR) {
        out->columns = tok.value;
    } else if (tok.type == TOK_NUMBER || tok.type == TOK_STRING) {
        /* SELECT <literal> — no FROM needed */
        out->columns = tok.value;
        out->insert_row = calloc(1, sizeof(struct row));
        da_init(&out->insert_row->cells);
        struct cell c = {0};
        if (tok.type == TOK_NUMBER) {
            c.type = COLUMN_TYPE_INT;
            c.value.as_int = sv_atoi(tok.value);
        } else {
            c.type = COLUMN_TYPE_TEXT;
            c.value.as_text = sv_to_cstr(tok.value);
        }
        da_push(&out->insert_row->cells, c);

        /* check for more comma-separated literals */
        for (;;) {
            struct token peek = lexer_peek(l);
            if (peek.type == TOK_COMMA) {
                lexer_next(l);
                tok = lexer_next(l);
                struct cell c2 = {0};
                if (tok.type == TOK_NUMBER) {
                    c2.type = COLUMN_TYPE_INT;
                    c2.value.as_int = sv_atoi(tok.value);
                } else if (tok.type == TOK_STRING) {
                    c2.type = COLUMN_TYPE_TEXT;
                    c2.value.as_text = sv_to_cstr(tok.value);
                } else {
                    break;
                }
                da_push(&out->insert_row->cells, c2);
            } else {
                break;
            }
        }
        return 0;
    } else if (tok.type == TOK_IDENTIFIER || tok.type == TOK_KEYWORD) {
        const char *col_start = tok.value.data;
        sv last = consume_identifier(l, tok);
        const char *col_end = last.data + last.len;
        /* if this is CASE, consume until END */
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "CASE")) {
            int depth = 1;
            while (depth > 0) {
                struct token ct = lexer_next(l);
                if (ct.type == TOK_EOF) break;
                if (ct.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(ct.value, "CASE")) depth++;
                if (ct.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(ct.value, "END")) depth--;
                col_end = ct.value.data + ct.value.len;
            }
        }
        for (;;) {
            struct token peek = lexer_peek(l);
            /* consume parenthesized expressions: COALESCE(...), func(...) */
            if (peek.type == TOK_LPAREN) {
                int depth = 1;
                lexer_next(l); /* consume ( */
                while (depth > 0) {
                    struct token pt = lexer_next(l);
                    if (pt.type == TOK_EOF) break;
                    if (pt.type == TOK_LPAREN) depth++;
                    if (pt.type == TOK_RPAREN) depth--;
                    col_end = pt.value.data + pt.value.len;
                }
                continue;
            }
            /* consume arithmetic operators and operands as part of expression */
            if (peek.type == TOK_STAR || peek.type == TOK_PLUS ||
                peek.type == TOK_MINUS || peek.type == TOK_SLASH) {
                lexer_next(l); /* consume operator */
                struct token operand = lexer_next(l); /* consume operand */
                col_end = operand.value.data + operand.value.len;
                continue;
            }
            /* skip optional column alias: AS alias */
            if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "AS")) {
                lexer_next(l); /* consume AS */
                lexer_next(l); /* consume alias name */
            }
            peek = lexer_next(l);
            if (peek.type == TOK_COMMA) {
                tok = lexer_next(l);
                if (tok.type == TOK_IDENTIFIER || tok.type == TOK_KEYWORD) {
                    /* CASE ... END */
                    if (sv_eq_ignorecase_cstr(tok.value, "CASE")) {
                        int depth = 1;
                        col_end = tok.value.data + tok.value.len;
                        while (depth > 0) {
                            struct token ct = lexer_next(l);
                            if (ct.type == TOK_EOF) break;
                            if (ct.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(ct.value, "CASE")) depth++;
                            if (ct.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(ct.value, "END")) depth--;
                            col_end = ct.value.data + ct.value.len;
                        }
                    } else {
                        last = consume_identifier(l, tok);
                        col_end = last.data + last.len;
                    }
                } else if (tok.type == TOK_NUMBER) {
                    col_end = tok.value.data + tok.value.len;
                } else {
                    fprintf(stderr, "parse error: expected column name or expression after ','\n");
                    return -1;
                }
            } else {
                if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "FROM")) {
                    out->columns = sv_from(col_start, (size_t)(col_end - col_start));
                    goto parse_table_name;
                }
                fprintf(stderr, "parse error: expected FROM, got '" SV_FMT "'\n", SV_ARG(peek.value));
                return -1;
            }
        }
    } else {
        fprintf(stderr, "parse error: expected column list after SELECT\n");
        return -1;
    }

    /* FROM */
    tok = lexer_next(l);
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "FROM")) {
        fprintf(stderr, "parse error: expected FROM, got '" SV_FMT "'\n", SV_ARG(tok.value));
        return -1;
    }

parse_table_name:
    /* table name: identifier or quoted string */
    tok = lexer_next(l);
    if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
        out->table = tok.value;
    } else {
        fprintf(stderr, "parse error: expected table name, got '" SV_FMT "'\n", SV_ARG(tok.value));
        return -1;
    }

    /* optional table alias: AS alias or just alias (identifier) */
    {
        struct token pa = lexer_peek(l);
        if (pa.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(pa.value, "AS")) {
            lexer_next(l); /* consume AS */
            lexer_next(l); /* consume alias name */
        } else if (pa.type == TOK_IDENTIFIER) {
            /* bare alias — but only if it's not a keyword we expect next */
            /* skip: it could be confused with WHERE, JOIN, ORDER, etc. */
        }
    }

    /* optional: one or more [LEFT|RIGHT|FULL] [OUTER] JOIN table ON cond */
    da_init(&out->joins);
    for (;;) {
        struct token peek = lexer_peek(l);
        int jtype = 0; /* 0=INNER */
        if (peek.type == TOK_KEYWORD && (sv_eq_ignorecase_cstr(peek.value, "LEFT") ||
                                          sv_eq_ignorecase_cstr(peek.value, "RIGHT") ||
                                          sv_eq_ignorecase_cstr(peek.value, "FULL"))) {
            if (sv_eq_ignorecase_cstr(peek.value, "LEFT"))  jtype = 1;
            if (sv_eq_ignorecase_cstr(peek.value, "RIGHT")) jtype = 2;
            if (sv_eq_ignorecase_cstr(peek.value, "FULL"))  jtype = 3;
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

        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            fprintf(stderr, "parse error: expected table name after JOIN\n");
            return -1;
        }
        ji.join_table = tok.value;

        /* ON */
        tok = lexer_next(l);
        if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "ON")) {
            fprintf(stderr, "parse error: expected ON after JOIN table\n");
            return -1;
        }

        /* left: table.col */
        tok = lexer_next(l);
        ji.join_left_col = consume_identifier(l, tok);

        /* = */
        tok = lexer_next(l);
        if (tok.type != TOK_EQUALS) {
            fprintf(stderr, "parse error: expected '=' in ON clause\n");
            return -1;
        }

        /* right: table.col */
        tok = lexer_next(l);
        ji.join_right_col = consume_identifier(l, tok);

        da_push(&out->joins, ji);
    }

    /* backwards compat: populate single-join fields from first join */
    if (out->joins.count > 0) {
        out->has_join = 1;
        out->join_type = out->joins.items[0].join_type;
        out->join_table = out->joins.items[0].join_table;
        out->join_left_col = out->joins.items[0].join_left_col;
        out->join_right_col = out->joins.items[0].join_right_col;
    }

    /* optional: WHERE condition */
    {
        struct token peek = lexer_peek(l);
        if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "WHERE")) {
            lexer_next(l); /* consume WHERE */
            if (parse_where_clause(l, out) != 0) return -1;
        }
    }

    /* optional: ORDER BY / LIMIT / OFFSET */
    parse_order_limit(l, out);

    return 0;
}

static int parse_value_tuple(struct lexer *l, struct row *r)
{
    struct token tok = lexer_next(l);
    if (tok.type != TOK_LPAREN) {
        fprintf(stderr, "parse error: expected '('\n");
        return -1;
    }

    da_init(&r->cells);
    for (;;) {
        tok = lexer_next(l);
        struct cell c = {0};
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
            c.value.as_text = sv_to_cstr(tok.value);
        } else if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "NULL")) {
            c.type = COLUMN_TYPE_TEXT;
            c.value.as_text = NULL;
        } else if (tok.type == TOK_KEYWORD &&
                   (sv_eq_ignorecase_cstr(tok.value, "TRUE") ||
                    sv_eq_ignorecase_cstr(tok.value, "FALSE"))) {
            c.type = COLUMN_TYPE_BOOLEAN;
            c.value.as_bool = sv_eq_ignorecase_cstr(tok.value, "TRUE") ? 1 : 0;
        } else {
            fprintf(stderr, "parse error: unexpected token '" SV_FMT "' in VALUES\n", SV_ARG(tok.value));
            return -1;
        }
        da_push(&r->cells, c);

        tok = lexer_next(l);
        if (tok.type == TOK_RPAREN) break;
        if (tok.type != TOK_COMMA) {
            fprintf(stderr, "parse error: expected ',' or ')'\n");
            return -1;
        }
    }
    return 0;
}

static int parse_insert(struct lexer *l, struct query *out)
{
    out->query_type = QUERY_TYPE_INSERT;

    /* INTO */
    struct token tok = lexer_next(l);
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "INTO")) {
        fprintf(stderr, "parse error: expected INTO after INSERT\n");
        return -1;
    }

    /* table name */
    tok = lexer_next(l);
    if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
        out->table = tok.value;
    } else {
        fprintf(stderr, "parse error: expected table name\n");
        return -1;
    }

    /* optional (col, col, ...) then VALUES */
    tok = lexer_next(l);
    if (tok.type == TOK_LPAREN) {
        /* skip column list — just consume until ')' */
        for (;;) {
            tok = lexer_next(l);
            if (tok.type == TOK_RPAREN) break;
            if (tok.type == TOK_EOF) {
                fprintf(stderr, "parse error: unexpected end in column list\n");
                return -1;
            }
        }
        tok = lexer_next(l);
    }
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "VALUES")) {
        fprintf(stderr, "parse error: expected VALUES\n");
        return -1;
    }

    /* parse one or more value tuples: (v1, v2), (v3, v4), ... */
    da_init(&out->insert_rows);
    for (;;) {
        struct row r = {0};
        if (parse_value_tuple(l, &r) != 0) return -1;
        da_push(&out->insert_rows, r);

        struct token peek = lexer_peek(l);
        if (peek.type == TOK_COMMA) {
            lexer_next(l); /* consume comma between tuples */
        } else {
            break;
        }
    }

    /* backwards compat: point insert_row at first row */
    if (out->insert_rows.count > 0) {
        out->insert_row = &out->insert_rows.items[0];
    }

    /* optional RETURNING col, col, ... */
    struct token peek = lexer_peek(l);
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "RETURNING")) {
        lexer_next(l); /* consume RETURNING */
        tok = lexer_next(l);
        if (tok.type == TOK_STAR) {
            out->returning_columns = tok.value;
        } else if (tok.type == TOK_IDENTIFIER) {
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
            out->returning_columns = sv_from(start, (size_t)(end - start));
        }
    }

    return 0;
}

static enum column_type parse_column_type(sv type_name)
{
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
    if (sv_eq_ignorecase_cstr(type_name, "TIMESTAMP") ||
        sv_eq_ignorecase_cstr(type_name, "TIMESTAMPTZ"))
        return COLUMN_TYPE_TIMESTAMP;
    if (sv_eq_ignorecase_cstr(type_name, "UUID"))
        return COLUMN_TYPE_UUID;
    return COLUMN_TYPE_ENUM;
}

static int parse_create(struct lexer *l, struct query *out)
{
    struct token tok = lexer_next(l);

    /* CREATE TYPE name AS ENUM ('val1', 'val2', ...) */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "TYPE")) {
        out->query_type = QUERY_TYPE_CREATE_TYPE;

        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            fprintf(stderr, "parse error: expected type name after CREATE TYPE\n");
            return -1;
        }
        out->type_name = tok.value;

        tok = lexer_next(l);
        if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "AS")) {
            fprintf(stderr, "parse error: expected AS after type name\n");
            return -1;
        }

        tok = lexer_next(l);
        if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "ENUM")) {
            fprintf(stderr, "parse error: expected ENUM after AS\n");
            return -1;
        }

        tok = lexer_next(l);
        if (tok.type != TOK_LPAREN) {
            fprintf(stderr, "parse error: expected '(' after ENUM\n");
            return -1;
        }

        da_init(&out->enum_values);
        for (;;) {
            tok = lexer_next(l);
            if (tok.type != TOK_STRING) {
                fprintf(stderr, "parse error: expected string value in ENUM list\n");
                return -1;
            }
            da_push(&out->enum_values, sv_to_cstr(tok.value));

            tok = lexer_next(l);
            if (tok.type == TOK_RPAREN) break;
            if (tok.type != TOK_COMMA) {
                fprintf(stderr, "parse error: expected ',' or ')' in ENUM list\n");
                return -1;
            }
        }

        return 0;
    }

    /* CREATE INDEX name ON table (column) */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "INDEX")) {
        out->query_type = QUERY_TYPE_CREATE_INDEX;

        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            fprintf(stderr, "parse error: expected index name\n");
            return -1;
        }
        out->index_name = tok.value;

        tok = lexer_next(l);
        if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "ON")) {
            fprintf(stderr, "parse error: expected ON after index name\n");
            return -1;
        }

        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            fprintf(stderr, "parse error: expected table name after ON\n");
            return -1;
        }
        out->table = tok.value;

        tok = lexer_next(l);
        if (tok.type != TOK_LPAREN) {
            fprintf(stderr, "parse error: expected '(' after table name\n");
            return -1;
        }

        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER) {
            fprintf(stderr, "parse error: expected column name in index\n");
            return -1;
        }
        out->index_column = tok.value;

        tok = lexer_next(l);
        if (tok.type != TOK_RPAREN) {
            fprintf(stderr, "parse error: expected ')' after column name\n");
            return -1;
        }

        return 0;
    }

    /* CREATE TABLE ... */
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "TABLE")) {
        fprintf(stderr, "parse error: expected TABLE or INDEX after CREATE\n");
        return -1;
    }

    out->query_type = QUERY_TYPE_CREATE;

    /* table name */
    tok = lexer_next(l);
    if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
        out->table = tok.value;
    } else {
        fprintf(stderr, "parse error: expected table name\n");
        return -1;
    }

    /* ( col_name TYPE, ... ) */
    tok = lexer_next(l);
    if (tok.type != TOK_LPAREN) {
        fprintf(stderr, "parse error: expected '(' after table name\n");
        return -1;
    }

    for (;;) {
        /* column name */
        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER) {
            fprintf(stderr, "parse error: expected column name\n");
            return -1;
        }
        char *col_name = sv_to_cstr(tok.value);

        /* column type — keyword (INT/FLOAT/TEXT) or identifier (enum type name) */
        tok = lexer_next(l);
        if (tok.type != TOK_KEYWORD && tok.type != TOK_IDENTIFIER) {
            fprintf(stderr, "parse error: expected column type\n");
            free(col_name);
            return -1;
        }
        struct column col = {
            .name = col_name,
            .type = parse_column_type(tok.value),
            .enum_type_name = NULL
        };
        if (col.type == COLUMN_TYPE_ENUM) {
            col.enum_type_name = sv_to_cstr(tok.value);
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
                *col.default_value = parse_literal_value(val_tok);
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
            } else if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "CHECK")) {
                lexer_next(l); /* consume CHECK */
                /* skip CHECK(...) — consume until matching ')' */
                struct token lp = lexer_next(l);
                if (lp.type == TOK_LPAREN) {
                    int depth = 1;
                    while (depth > 0) {
                        tok = lexer_next(l);
                        if (tok.type == TOK_LPAREN) depth++;
                        else if (tok.type == TOK_RPAREN) depth--;
                        else if (tok.type == TOK_EOF) break;
                    }
                }
            } else {
                /* unknown constraint keyword, skip it */
                lexer_next(l);
            }
        }
        da_push(&out->create_columns, col);

        tok = lexer_next(l);
        if (tok.type == TOK_RPAREN) break;
        if (tok.type != TOK_COMMA) {
            fprintf(stderr, "parse error: expected ',' or ')' in column list\n");
            return -1;
        }
    }

    return 0;
}

static int parse_drop(struct lexer *l, struct query *out)
{
    struct token tok = lexer_next(l);

    /* DROP TYPE name */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "TYPE")) {
        out->query_type = QUERY_TYPE_DROP_TYPE;
        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            fprintf(stderr, "parse error: expected type name after DROP TYPE\n");
            return -1;
        }
        out->type_name = tok.value;
        return 0;
    }

    /* DROP INDEX name */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "INDEX")) {
        out->query_type = QUERY_TYPE_DROP_INDEX;
        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            fprintf(stderr, "parse error: expected index name after DROP INDEX\n");
            return -1;
        }
        out->index_name = tok.value;
        return 0;
    }

    /* DROP TABLE name */
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "TABLE")) {
        fprintf(stderr, "parse error: expected TABLE, INDEX, or TYPE after DROP\n");
        return -1;
    }

    out->query_type = QUERY_TYPE_DROP;
    tok = lexer_next(l);
    if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
        out->table = tok.value;
    } else {
        fprintf(stderr, "parse error: expected table name after DROP TABLE\n");
        return -1;
    }

    return 0;
}

static int parse_delete(struct lexer *l, struct query *out)
{
    out->query_type = QUERY_TYPE_DELETE;

    /* FROM */
    struct token tok = lexer_next(l);
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "FROM")) {
        fprintf(stderr, "parse error: expected FROM after DELETE\n");
        return -1;
    }

    /* table name */
    tok = lexer_next(l);
    if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
        out->table = tok.value;
    } else {
        fprintf(stderr, "parse error: expected table name\n");
        return -1;
    }

    /* optional WHERE */
    struct token peek = lexer_peek(l);
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "WHERE")) {
        lexer_next(l);
        if (parse_where_clause(l, out) != 0) return -1;
    }

    return 0;
}

static int parse_update(struct lexer *l, struct query *out)
{
    out->query_type = QUERY_TYPE_UPDATE;

    /* table name */
    struct token tok = lexer_next(l);
    if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
        fprintf(stderr, "parse error: expected table name after UPDATE\n");
        return -1;
    }
    out->table = tok.value;

    /* SET */
    tok = lexer_next(l);
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "SET")) {
        fprintf(stderr, "parse error: expected SET after table name\n");
        return -1;
    }

    /* col = val [, col = val ...] */
    da_init(&out->set_clauses);
    for (;;) {
        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER) {
            fprintf(stderr, "parse error: expected column name in SET\n");
            return -1;
        }
        struct set_clause sc;
        sc.column = tok.value;

        tok = lexer_next(l);
        if (tok.type != TOK_EQUALS) {
            fprintf(stderr, "parse error: expected '=' in SET clause\n");
            return -1;
        }

        tok = lexer_next(l);
        sc.value = parse_literal_value(tok);
        da_push(&out->set_clauses, sc);

        struct token peek = lexer_peek(l);
        if (peek.type != TOK_COMMA) break;
        lexer_next(l); /* consume comma */
    }

    /* optional WHERE */
    struct token peek = lexer_peek(l);
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "WHERE")) {
        lexer_next(l);
        if (parse_where_clause(l, out) != 0) return -1;
    }

    return 0;
}

static int parse_alter(struct lexer *l, struct query *out)
{
    out->query_type = QUERY_TYPE_ALTER;

    struct token tok = lexer_next(l);
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "TABLE")) {
        fprintf(stderr, "parse error: expected TABLE after ALTER\n");
        return -1;
    }

    tok = lexer_next(l);
    if (tok.type != TOK_IDENTIFIER && tok.type != TOK_KEYWORD && tok.type != TOK_STRING) {
        fprintf(stderr, "parse error: expected table name after ALTER TABLE\n");
        return -1;
    }
    out->table = tok.value;

    tok = lexer_next(l);
    if (tok.type != TOK_KEYWORD) {
        fprintf(stderr, "parse error: expected ADD/DROP/RENAME/ALTER after table name\n");
        return -1;
    }

    if (sv_eq_ignorecase_cstr(tok.value, "ADD")) {
        /* ALTER TABLE t ADD [COLUMN] col_name col_type [constraints...] */
        out->alter_action = ALTER_ADD_COLUMN;
        tok = lexer_next(l);
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "COLUMN"))
            tok = lexer_next(l); /* skip optional COLUMN keyword */
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_KEYWORD) {
            fprintf(stderr, "parse error: expected column name in ADD COLUMN\n");
            return -1;
        }
        out->alter_new_col.name = sv_to_cstr(tok.value);
        tok = lexer_next(l);
        out->alter_new_col.type = parse_column_type(tok.value);
        if (out->alter_new_col.type == COLUMN_TYPE_ENUM)
            out->alter_new_col.enum_type_name = sv_to_cstr(tok.value);
        /* skip optional (n) */
        {
            struct token peek = lexer_peek(l);
            if (peek.type == TOK_LPAREN) {
                lexer_next(l);
                for (;;) { tok = lexer_next(l); if (tok.type == TOK_RPAREN || tok.type == TOK_EOF) break; }
            }
        }
        return 0;
    }

    if (sv_eq_ignorecase_cstr(tok.value, "DROP")) {
        /* ALTER TABLE t DROP [COLUMN] col_name */
        out->alter_action = ALTER_DROP_COLUMN;
        tok = lexer_next(l);
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "COLUMN"))
            tok = lexer_next(l);
        out->alter_column = tok.value;
        return 0;
    }

    if (sv_eq_ignorecase_cstr(tok.value, "RENAME")) {
        /* ALTER TABLE t RENAME [COLUMN] old_name TO new_name */
        out->alter_action = ALTER_RENAME_COLUMN;
        tok = lexer_next(l);
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "COLUMN"))
            tok = lexer_next(l);
        out->alter_column = tok.value;
        tok = lexer_next(l); /* TO */
        if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "TO")) {
            fprintf(stderr, "parse error: expected TO in RENAME COLUMN\n");
            return -1;
        }
        tok = lexer_next(l);
        out->alter_new_name = tok.value;
        return 0;
    }

    if (sv_eq_ignorecase_cstr(tok.value, "ALTER")) {
        /* ALTER TABLE t ALTER [COLUMN] col_name TYPE new_type */
        out->alter_action = ALTER_COLUMN_TYPE;
        tok = lexer_next(l);
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "COLUMN"))
            tok = lexer_next(l);
        out->alter_column = tok.value;
        tok = lexer_next(l); /* TYPE or SET */
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "TYPE")) {
            tok = lexer_next(l);
            out->alter_new_col.type = parse_column_type(tok.value);
            /* skip optional (n) */
            struct token peek = lexer_peek(l);
            if (peek.type == TOK_LPAREN) {
                lexer_next(l);
                for (;;) { tok = lexer_next(l); if (tok.type == TOK_RPAREN || tok.type == TOK_EOF) break; }
            }
        }
        return 0;
    }

    fprintf(stderr, "parse error: unsupported ALTER TABLE action\n");
    return -1;
}

int query_parse(const char *sql, struct query *out)
{
    memset(out, 0, sizeof(*out));

    struct lexer l;
    lexer_init(&l, sql);

    struct token tok = lexer_next(&l);
    if (tok.type != TOK_KEYWORD) {
        fprintf(stderr, "parse error: expected SQL keyword, got '" SV_FMT "'\n", SV_ARG(tok.value));
        return -1;
    }

    if (sv_eq_ignorecase_cstr(tok.value, "CREATE"))
        return parse_create(&l, out);
    if (sv_eq_ignorecase_cstr(tok.value, "DROP"))
        return parse_drop(&l, out);
    if (sv_eq_ignorecase_cstr(tok.value, "SELECT"))
        return parse_select(&l, out);
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

    fprintf(stderr, "parse error: unsupported statement '" SV_FMT "'\n", SV_ARG(tok.value));
    return -1;
}
