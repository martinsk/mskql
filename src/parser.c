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
    TOK_PERCENT,
    TOK_PIPE_PIPE,
    TOK_DOUBLE_COLON,
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
        "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
        "PERCENT_RANK", "CUME_DIST",
        "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE",
        "OVER", "PARTITION", "BY", "ORDER",
        "UPDATE", "SET", "AND", "OR", "NOT", "NULL", "IS",
        "LIMIT", "OFFSET", "ASC", "DESC", "GROUP", "HAVING",
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
    if (sv_eq_ignorecase_cstr(word, "SUM"))   return AGG_SUM;
    if (sv_eq_ignorecase_cstr(word, "COUNT")) return AGG_COUNT;
    if (sv_eq_ignorecase_cstr(word, "AVG"))   return AGG_AVG;
    if (sv_eq_ignorecase_cstr(word, "MIN"))   return AGG_MIN;
    if (sv_eq_ignorecase_cstr(word, "MAX"))   return AGG_MAX;
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
                fprintf(stderr, "parse error: expected BETWEEN after ROWS/RANGE\n");
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
        case TOK_PERCENT:
        case TOK_PIPE_PIPE:
        case TOK_DOUBLE_COLON:
        case TOK_EOF:
        case TOK_UNKNOWN:
            return CMP_EQ;
    }
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
           sv_eq_ignorecase_cstr(name, "TO_CHAR");
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
    enum column_type ct = parse_column_type(tok.value);
    /* skip optional (n) or (p,s) after type name */
    struct token peek = lexer_peek(l);
    if (peek.type == TOK_LPAREN) {
        lexer_next(l); /* consume ( */
        for (;;) {
            struct token t = lexer_next(l);
            if (t.type == TOK_RPAREN || t.type == TOK_EOF) break;
        }
    }
    return ct;
}

/* parse an atom: literal, column ref, function call, CASE, subquery, parens */
static uint32_t parse_expr_atom(struct lexer *l, struct query_arena *a)
{
    struct token tok = lexer_peek(l);

    /* CAST(expr AS type) */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "CAST")) {
        lexer_next(l); /* consume CAST */
        struct token lp = lexer_next(l); /* consume ( */
        if (lp.type != TOK_LPAREN) {
            fprintf(stderr, "parse error: expected '(' after CAST\n");
            return IDX_NONE;
        }
        uint32_t operand = parse_expr(l, a);
        struct token as_tok = lexer_next(l); /* consume AS */
        if (as_tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(as_tok.value, "AS")) {
            fprintf(stderr, "parse error: expected AS in CAST\n");
            return IDX_NONE;
        }
        enum column_type target = parse_cast_type_name(l);
        struct token rp = lexer_next(l); /* consume ) */
        if (rp.type != TOK_RPAREN) {
            fprintf(stderr, "parse error: expected ')' after CAST type\n");
        }
        uint32_t ei = expr_alloc(a, EXPR_CAST);
        EXPR(a, ei).cast.operand = operand;
        EXPR(a, ei).cast.target = target;
        return ei;
    }

    /* EXTRACT(field FROM expr) */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "EXTRACT")) {
        lexer_next(l); /* consume EXTRACT */
        struct token lp = lexer_next(l); /* consume ( */
        if (lp.type != TOK_LPAREN) {
            fprintf(stderr, "parse error: expected '(' after EXTRACT\n");
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
            fprintf(stderr, "parse error: expected FROM in EXTRACT\n");
            return IDX_NONE;
        }
        uint32_t src_expr = parse_expr(l, a);
        struct token rp = lexer_next(l); /* consume ) */
        if (rp.type != TOK_RPAREN) {
            fprintf(stderr, "parse error: expected ')' after EXTRACT\n");
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
                    fprintf(stderr, "parse error: unterminated subquery in expression\n");
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
            fprintf(stderr, "parse error: expected ')' after expression\n");
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
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "CASE")) {
        lexer_next(l); /* consume CASE */
        uint32_t ei = expr_alloc(a, EXPR_CASE_WHEN);
        uint32_t branches_start = (uint32_t)a->branches.count;
        uint32_t branches_count = 0;
        EXPR(a, ei).case_when.else_expr = IDX_NONE;

        for (;;) {
            tok = lexer_peek(l);
            if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "WHEN")) {
                lexer_next(l); /* consume WHEN */
                uint32_t cond_idx = parse_or_cond(l, a);
                tok = lexer_next(l); /* consume THEN */
                if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "THEN")) {
                    fprintf(stderr, "parse error: expected THEN in CASE\n");
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
                fprintf(stderr, "parse error: unexpected token in CASE expression\n");
                break;
            }
        }
        EXPR(a, ei).case_when.branches_start = branches_start;
        EXPR(a, ei).case_when.branches_count = branches_count;
        return ei;
    }

    /* function call: FUNC(...) */
    if ((tok.type == TOK_KEYWORD || tok.type == TOK_IDENTIFIER) &&
        is_expr_func_keyword(tok.value)) {
        struct token peek2 = lexer_peek(l);
        /* look ahead past the name to check for ( */
        size_t saved = l->pos;
        lexer_next(l); /* consume func name */
        struct token maybe_lp = lexer_peek(l);
        if (maybe_lp.type == TOK_LPAREN) {
            lexer_next(l); /* consume ( */
            /* Parse all arguments first, collecting their root indices.
             * Nested parse_expr calls may interleave allocations, so the
             * arg root exprs are NOT consecutive in the arena. */
            uint32_t arg_indices[16];
            uint32_t args_count = 0;
            struct token p = lexer_peek(l);
            if (p.type != TOK_RPAREN) {
                for (;;) {
                    uint32_t arg_idx = parse_expr(l, a);
                    if (args_count < 16) arg_indices[args_count] = arg_idx;
                    args_count++;
                    p = lexer_peek(l);
                    if (p.type == TOK_COMMA) {
                        lexer_next(l); /* consume , */
                    } else {
                        break;
                    }
                }
            }
            /* Store arg root indices in arena.arg_indices (consecutive). */
            uint32_t ei = expr_alloc(a, EXPR_FUNC_CALL);
            EXPR(a, ei).func_call.func = expr_func_from_name(tok.value);
            /* Store arg indices in arena.arg_indices (consecutive uint32_t) */
            uint32_t args_start = (uint32_t)a->arg_indices.count;
            for (uint32_t ai = 0; ai < args_count && ai < 16; ai++)
                da_push(&a->arg_indices, arg_indices[ai]);
            EXPR(a, ei).func_call.args_start = args_start;
            EXPR(a, ei).func_call.args_count = args_count;
            tok = lexer_next(l); /* consume ) */
            if (tok.type != TOK_RPAREN) {
                fprintf(stderr, "parse error: expected ')' after function arguments\n");
            }
            (void)peek2;
            return ei;
        }
        /* not a function call, restore and fall through to column ref */
        l->pos = saved;
    }

    /* number literal */
    if (tok.type == TOK_NUMBER) {
        lexer_next(l);
        uint32_t ei = expr_alloc(a, EXPR_LITERAL);
        if (sv_contains_char(tok.value, '.')) {
            EXPR(a, ei).literal.type = COLUMN_TYPE_FLOAT;
            EXPR(a, ei).literal.value.as_float = sv_atof(tok.value);
        } else {
            long long v = 0;
            size_t k = 0;
            int neg = 0;
            if (tok.value.len > 0 && tok.value.data[0] == '-') { neg = 1; k = 1; }
            for (; k < tok.value.len; k++)
                v = v * 10 + (tok.value.data[k] - '0');
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

    fprintf(stderr, "parse error: unexpected token in expression: '" SV_FMT "'\n",
            SV_ARG(tok.value));
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

/* parse a single comparison or grouped/negated condition */
static uint32_t parse_single_cond(struct lexer *l, struct query_arena *a)
{
    struct token tok = lexer_peek(l);

    /* EXISTS (SELECT ...) */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "EXISTS")) {
        lexer_next(l); /* consume EXISTS */
        struct token lp = lexer_next(l);
        if (lp.type != TOK_LPAREN) {
            fprintf(stderr, "parse error: expected '(' after EXISTS\n");
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
                fprintf(stderr, "parse error: unterminated EXISTS subquery\n");
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
        if (is_multi_in) {
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
                    fprintf(stderr, "parse error: expected column in multi-column IN\n");
                    return IDX_NONE;
                }
                sv colsv = consume_identifier(l, col);
                arena_push_sv(a, colsv);
                mc_count++;
                struct token sep = lexer_next(l);
                if (sep.type == TOK_RPAREN) break;
                if (sep.type != TOK_COMMA) {
                    fprintf(stderr, "parse error: expected ',' or ')' in column list\n");
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
                fprintf(stderr, "parse error: expected IN after column tuple\n");
                return IDX_NONE;
            }
            struct token lp = lexer_next(l);
            if (lp.type != TOK_LPAREN) {
                fprintf(stderr, "parse error: expected '(' after IN\n");
                return IDX_NONE;
            }
            uint32_t mv_start = (uint32_t)a->cells.count;
            uint32_t mv_count = 0;
            for (;;) {
                struct token tp = lexer_next(l);
                if (tp.type != TOK_LPAREN) {
                    fprintf(stderr, "parse error: expected '(' for value tuple\n");
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
                            fprintf(stderr, "parse error: expected ',' in value tuple\n");
                            return IDX_NONE;
                        }
                    }
                }
                struct token rp = lexer_next(l);
                if (rp.type != TOK_RPAREN) {
                    fprintf(stderr, "parse error: expected ')' after value tuple\n");
                    return IDX_NONE;
                }
                struct token sep = lexer_next(l);
                if (sep.type == TOK_RPAREN) break;
                if (sep.type != TOK_COMMA) {
                    fprintf(stderr, "parse error: expected ',' or ')' in IN list\n");
                    return IDX_NONE;
                }
            }
            COND(a, ci).multi_values_start = mv_start;
            COND(a, ci).multi_values_count = mv_count;
            return ci;
        }
        /* not multi-column IN — restore and parse as parenthesized sub-expression */
        l->pos = saved;
        uint32_t inner = parse_or_cond(l, a);
        if (inner == IDX_NONE) return IDX_NONE;
        tok = lexer_next(l);
        if (tok.type != TOK_RPAREN) {
            fprintf(stderr, "parse error: expected ')' after grouped condition\n");
            return IDX_NONE;
        }
        return inner;
    }

    tok = lexer_next(l);
    /* accept identifiers and keywords as column names (e.g. sum, count, avg in HAVING) */
    if (tok.type != TOK_IDENTIFIER && tok.type != TOK_KEYWORD) {
        fprintf(stderr, "parse error: expected column name in WHERE/HAVING\n");
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

    /* If op_tok is an arithmetic operator, the LHS is an expression (e.g. a + b > 11).
     * Back up to before the identifier and re-parse the whole LHS as an expression. */
    if (op_tok.type == TOK_PLUS || op_tok.type == TOK_MINUS ||
        op_tok.type == TOK_STAR || op_tok.type == TOK_SLASH ||
        op_tok.type == TOK_PERCENT || op_tok.type == TOK_PIPE_PIPE) {
        l->pos = tok.value.data - l->input;
        COND(a, ci).lhs_expr = parse_expr(l, a);
        COND(a, ci).column = sv_from(NULL, 0);
        op_tok = lexer_next(l);
    }

parse_operator:

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
                    return IDX_NONE;
                }
                COND(a, ci).op = CMP_IS_NOT_DISTINCT;
                struct token val_tok = lexer_next(l);
                COND(a, ci).value = parse_literal_value_arena(val_tok, a);
                return ci;
            }
            next = lexer_next(l);
            if (next.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(next.value, "NULL")) {
                fprintf(stderr, "parse error: expected NULL after IS NOT\n");
                return IDX_NONE;
            }
            COND(a, ci).op = CMP_IS_NOT_NULL;
        } else if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "DISTINCT")) {
            struct token from_tok = lexer_next(l);
            if (from_tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(from_tok.value, "FROM")) {
                fprintf(stderr, "parse error: expected FROM after IS DISTINCT\n");
                return IDX_NONE;
            }
            COND(a, ci).op = CMP_IS_DISTINCT;
            struct token val_tok = lexer_next(l);
            COND(a, ci).value = parse_literal_value_arena(val_tok, a);
            return ci;
        } else if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "NULL")) {
            COND(a, ci).op = CMP_IS_NULL;
        } else {
            fprintf(stderr, "parse error: expected NULL, NOT, or DISTINCT after IS\n");
            return IDX_NONE;
        }
        return ci;
    }

    /* NOT IN (...) */
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "NOT")) {
        struct token next = lexer_next(l);
        if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "IN")) {
            COND(a, ci).op = CMP_NOT_IN;
            goto parse_in_list;
        }
        fprintf(stderr, "parse error: expected IN after NOT\n");
        return IDX_NONE;
    }

    /* IN (...) */
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "IN")) {
        COND(a, ci).op = CMP_IN;
parse_in_list:
        tok = lexer_next(l);
        if (tok.type != TOK_LPAREN) {
            fprintf(stderr, "parse error: expected '(' after IN\n");
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
                        fprintf(stderr, "parse error: unterminated subquery\n");
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
                fprintf(stderr, "parse error: expected ',' or ')' in IN list\n");
                return IDX_NONE;
            }
        }
        COND(a, ci).in_values_start = iv_start;
        COND(a, ci).in_values_count = iv_count;
        return ci;
    }

    /* BETWEEN low AND high */
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "BETWEEN")) {
        COND(a, ci).op = CMP_BETWEEN;
        tok = lexer_next(l);
        COND(a, ci).value = parse_literal_value_arena(tok, a);
        tok = lexer_next(l); /* AND */
        if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "AND")) {
            fprintf(stderr, "parse error: expected AND in BETWEEN\n");
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

    if (!is_cmp_token(op_tok.type)) {
        fprintf(stderr, "parse error: expected comparison operator in WHERE\n");
        return IDX_NONE;
    }
    COND(a, ci).op = cmp_from_token(op_tok.type);

    /* check for ANY/ALL/SOME: col op ANY(ARRAY[...]) or col op ANY(v1,v2,...) */
    {
        struct token peek_aas = lexer_peek(l);
        if (peek_aas.type == TOK_KEYWORD &&
            (sv_eq_ignorecase_cstr(peek_aas.value, "ANY") ||
             sv_eq_ignorecase_cstr(peek_aas.value, "ALL") ||
             sv_eq_ignorecase_cstr(peek_aas.value, "SOME"))) {
            int is_all = sv_eq_ignorecase_cstr(peek_aas.value, "ALL");
            lexer_next(l); /* consume ANY/ALL/SOME */
            struct token lp = lexer_next(l);
            if (lp.type != TOK_LPAREN) {
                fprintf(stderr, "parse error: expected '(' after ANY/ALL/SOME\n");
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
                for (;;) {
                    struct token col = lexer_next(l);
                    if (col.type == TOK_IDENTIFIER || col.type == TOK_KEYWORD) {
                        sv colsv = consume_identifier(l, col);
                        arena_push_sv(a, colsv);
                        gb_count++;
                    }
                    peek = lexer_peek(l);
                    if (peek.type != TOK_COMMA) break;
                    lexer_next(l); /* consume comma */
                }
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
            s->limit_count = sv_atoi(n.value);
        }
        peek = lexer_peek(l);
    }

    /* OFFSET n */
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "OFFSET")) {
        lexer_next(l);
        struct token n = lexer_next(l);
        if (n.type == TOK_NUMBER) {
            s->has_offset = 1;
            s->offset_count = sv_atoi(n.value);
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
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "DISTINCT")) {
        agg->has_distinct = 1;
        tok = lexer_next(l);
    }
    if (tok.type == TOK_STAR) {
        agg->column = tok.value;
    } else if (tok.type == TOK_IDENTIFIER || tok.type == TOK_KEYWORD) {
        agg->column = consume_identifier(l, tok);
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
    w->offset = 1; /* default offset for LAG/LEAD */

    struct token tok = lexer_next(l); /* ( */
    if (tok.type != TOK_LPAREN) {
        fprintf(stderr, "parse error: expected '(' after window function\n");
        return -1;
    }
    tok = lexer_next(l);
    if (tok.type == TOK_STAR) {
        w->arg_column = tok.value;
    } else if (tok.type == TOK_IDENTIFIER || tok.type == TOK_KEYWORD) {
        w->arg_column = tok.value;
    } else if (tok.type == TOK_NUMBER) {
        /* NTILE(n) — first arg is a number */
        long long v = 0;
        for (size_t k = 0; k < tok.value.len; k++)
            v = v * 10 + (tok.value.data[k] - '0');
        w->offset = (int)v;
    } else if (tok.type == TOK_RPAREN) {
        /* no args, e.g. ROW_NUMBER() */
        goto after_rparen;
    } else {
        fprintf(stderr, "parse error: unexpected token in window function args\n");
        return -1;
    }
    /* check for comma-separated second argument (offset for LAG/LEAD/NTH_VALUE) */
    tok = lexer_peek(l);
    if (tok.type == TOK_COMMA) {
        lexer_next(l); /* consume comma */
        tok = lexer_next(l); /* second arg (offset number) */
        if (tok.type == TOK_NUMBER) {
            long long v = 0;
            for (size_t k = 0; k < tok.value.len; k++)
                v = v * 10 + (tok.value.data[k] - '0');
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

static int parse_agg_list(struct lexer *l, struct query_arena *a, struct query_select *s, struct token first)
{
    uint32_t agg_start = (uint32_t)a->aggregates.count;
    uint32_t agg_count = 0;

    /* parse first aggregate: we already have the keyword token */
    struct agg_expr agg;
    memset(&agg, 0, sizeof(agg));
    agg.func = agg_from_keyword(first.value);

    struct token tok = lexer_next(l); /* ( */
    if (tok.type != TOK_LPAREN) {
        fprintf(stderr, "parse error: expected '(' after aggregate function\n");
        return -1;
    }
    tok = lexer_next(l); /* column or * or DISTINCT */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "DISTINCT")) {
        agg.has_distinct = 1;
        tok = lexer_next(l); /* column after DISTINCT */
    }
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

        tok = lexer_next(l);
        if (tok.type == TOK_KEYWORD && is_agg_keyword(tok.value)) {
            struct agg_expr a2;
            memset(&a2, 0, sizeof(a2));
            if (parse_single_agg(l, tok.value, &a2) != 0) return -1;
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
            fprintf(stderr, "parse error: expected aggregate or column after ','\n");
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

    /* optional DISTINCT [ON (expr, ...)] */
    struct token peek_dist = lexer_peek(l);
    if (peek_dist.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek_dist.value, "DISTINCT")) {
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
                uint32_t se_start = (uint32_t)a->select_exprs.count;
                uint32_t se_count = 0;
                struct select_expr se = {0};
                se.kind = SEL_WINDOW;
                if (parse_win_call(l, tok.value, &se.win) != 0) return -1;
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
                        if (parse_win_call(l, tok.value, &se2.win) != 0) return -1;
                        arena_push_select_expr(a, se2);
                        se_count++;
                    } else if (tok.type == TOK_IDENTIFIER) {
                        struct select_expr se2 = {0};
                        se2.kind = SEL_COLUMN;
                        se2.column = tok.value;
                        arena_push_select_expr(a, se2);
                        se_count++;
                    } else {
                        fprintf(stderr, "parse error: expected column or window function\n");
                        return -1;
                    }
                }
                s->select_exprs_start = se_start;
                s->select_exprs_count = se_count;
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
        /* consume qualified name (e.g. t1.name) before checking for comma */
        size_t pre_qual_pos = l->pos;
        sv first_col = consume_identifier(l, tok);
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
                uint32_t se_start = (uint32_t)a->select_exprs.count;
                uint32_t se_count = 0;
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
                        if (parse_win_call(l, tok.value, &se2.win) != 0) return -1;
                        arena_push_select_expr(a, se2);
                        se_count++;
                    } else if (tok.type == TOK_IDENTIFIER) {
                        struct select_expr se2 = {0};
                        se2.kind = SEL_COLUMN;
                        se2.column = tok.value;
                        arena_push_select_expr(a, se2);
                        se_count++;
                    } else {
                        fprintf(stderr, "parse error: expected column or window function\n");
                        return -1;
                    }
                }
                s->select_exprs_start = se_start;
                s->select_exprs_count = se_count;
                tok = lexer_next(l);
                if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "FROM")) {
                    fprintf(stderr, "parse error: expected FROM\n");
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
                        if (parse_single_agg(l, tok.value, &agg) != 0) return -1;
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
                    } else {
                        fprintf(stderr, "parse error: expected column or aggregate function\n");
                        return -1;
                    }
                }
                s->aggregates_start = agg_start;
                s->aggregates_count = agg_count;
                s->columns = sv_from(col_start, (size_t)(col_end - col_start));
                tok = lexer_next(l);
                if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "FROM")) {
                    fprintf(stderr, "parse error: expected FROM\n");
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
        const char *raw_col_start = l->input + l->pos;
        skip_whitespace(l);
        raw_col_start = l->input + l->pos;
        const char *raw_col_end = raw_col_start;
        for (;;) {
            struct select_column sc = {0};
            sc.expr_idx = parse_expr(l, a);
            if (sc.expr_idx == IDX_NONE) {
                fprintf(stderr, "parse error: expected expression in SELECT column list\n");
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
        fprintf(stderr, "parse error: expected FROM, got '" SV_FMT "'\n", SV_ARG(tok.value));
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
                fprintf(stderr, "parse error: unterminated FROM subquery\n");
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
            fprintf(stderr, "parse error: expected AS alias after FROM subquery\n");
            // NOTE: from_subquery_sql was just malloc'd above. Safe as long as the
            // caller always calls query_free on failure (query_type is QUERY_TYPE_SELECT
            // here, so query_select_free will free it). All current callers do this.
            return -1;
        }
    } else if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
        s->table = tok.value;
    } else {
        fprintf(stderr, "parse error: expected table name, got '" SV_FMT "'\n", SV_ARG(tok.value));
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

    /* optional: one or more [INNER|LEFT|RIGHT|FULL|CROSS|NATURAL] [OUTER] JOIN table ... */
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
        tok = lexer_next(l);
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "LATERAL")) {
            ji.is_lateral = 1;
            tok = lexer_next(l); /* should be ( */
            if (tok.type != TOK_LPAREN) {
                fprintf(stderr, "parse error: expected '(' after LATERAL\n");
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
                    fprintf(stderr, "parse error: unterminated LATERAL subquery\n");
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
                fprintf(stderr, "parse error: expected alias after LATERAL subquery\n");
                return -1;
            }
        } else if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
            ji.join_table = tok.value;

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
            fprintf(stderr, "parse error: expected table name after JOIN\n");
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
                    fprintf(stderr, "parse error: expected '(' after USING\n");
                    return -1;
                }
                tok = lexer_next(l);
                ji.has_using = 1;
                ji.using_col = tok.value;
                tok = lexer_next(l); /* ) */
                if (tok.type != TOK_RPAREN) {
                    fprintf(stderr, "parse error: expected ')' after USING column\n");
                    return -1;
                }
            } else {
                /* ON */
                tok = lexer_next(l);
                if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "ON")) {
                    fprintf(stderr, "parse error: expected ON after JOIN table\n");
                    return -1;
                }

                /* parse full ON condition tree (supports AND/OR/compound) */
                ji.join_on_cond = parse_or_cond(l, a);
                if (ji.join_on_cond == IDX_NONE) {
                    fprintf(stderr, "parse error: invalid ON condition\n");
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
    s->joins_start = joins_start;
    s->joins_count = joins_count;

    /* backwards compat: populate single-join fields from first join */
    if (joins_count > 0) {
        s->has_join = 1;
        s->join_type = a->joins.items[joins_start].join_type;
        s->join_table = a->joins.items[joins_start].join_table;
        s->join_left_col = a->joins.items[joins_start].join_left_col;
        s->join_right_col = a->joins.items[joins_start].join_right_col;
    }

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
    {
        struct token peek = lexer_peek(l);
        if (peek.type == TOK_KEYWORD &&
            (sv_eq_ignorecase_cstr(peek.value, "UNION") ||
             sv_eq_ignorecase_cstr(peek.value, "INTERSECT") ||
             sv_eq_ignorecase_cstr(peek.value, "EXCEPT"))) {
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
    }

    return 0;
}

static int parse_value_tuple(struct lexer *l, struct row *r, struct query_arena *a)
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
                    fprintf(stderr, "parse error: failed to parse expression in VALUES\n");
                    return -1;
                }
                c.type = COLUMN_TYPE_INT;
                c.is_null = 2; /* sentinel: value.as_int is an expr index */
                c.value.as_int = (int)ei;
                da_push(&r->cells, c);
                tok = lexer_next(l);
                if (tok.type == TOK_RPAREN) break;
                if (tok.type != TOK_COMMA) {
                    fprintf(stderr, "parse error: expected ',' or ')'\n");
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

    /* INTO */
    struct token tok = lexer_next(l);
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "INTO")) {
        fprintf(stderr, "parse error: expected INTO after INSERT\n");
        return -1;
    }

    /* table name */
    tok = lexer_next(l);
    if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
        ins->table = tok.value;
    } else {
        fprintf(stderr, "parse error: expected table name\n");
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
                fprintf(stderr, "parse error: unexpected end in column list\n");
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
        fprintf(stderr, "parse error: expected VALUES or SELECT\n");
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
                        fprintf(stderr, "parse error: expected expression in ON CONFLICT SET\n");
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

static int parse_create(struct lexer *l, struct query *out)
{
    struct token tok = lexer_next(l);

    /* CREATE SEQUENCE name [START WITH n] [INCREMENT BY n] */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "SEQUENCE")) {
        out->query_type = QUERY_TYPE_CREATE_SEQUENCE;
        struct query_create_sequence *cs = &out->create_seq;
        cs->start_value = 1;
        cs->increment = 1;
        cs->min_value = 1;
        cs->max_value = 9223372036854775807LL;

        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            fprintf(stderr, "parse error: expected sequence name\n");
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

    /* CREATE [OR REPLACE] VIEW name AS SELECT ... */
    if (tok.type == TOK_KEYWORD &&
        (sv_eq_ignorecase_cstr(tok.value, "VIEW") ||
         sv_eq_ignorecase_cstr(tok.value, "OR"))) {
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
            fprintf(stderr, "parse error: expected view name\n");
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

    /* CREATE TYPE name AS ENUM ('val1', 'val2', ...) */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "TYPE")) {
        out->query_type = QUERY_TYPE_CREATE_TYPE;
        struct query_create_type *ct = &out->create_type;

        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            fprintf(stderr, "parse error: expected type name after CREATE TYPE\n");
            return -1;
        }
        ct->type_name = tok.value;

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

        uint32_t ev_start = (uint32_t)out->arena.strings.count;
        uint32_t ev_count = 0;
        for (;;) {
            tok = lexer_next(l);
            if (tok.type != TOK_STRING) {
                fprintf(stderr, "parse error: expected string value in ENUM list\n");
                return -1;
            }
            arena_own_string(&out->arena, sv_to_cstr(tok.value));
            ev_count++;

            tok = lexer_next(l);
            if (tok.type == TOK_RPAREN) break;
            if (tok.type != TOK_COMMA) {
                fprintf(stderr, "parse error: expected ',' or ')' in ENUM list\n");
                return -1;
            }
        }
        ct->enum_values_start = ev_start;
        ct->enum_values_count = ev_count;

        return 0;
    }

    /* CREATE INDEX name ON table (column) */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "INDEX")) {
        out->query_type = QUERY_TYPE_CREATE_INDEX;
        struct query_create_index *ci = &out->create_index;

        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            fprintf(stderr, "parse error: expected index name\n");
            return -1;
        }
        ci->index_name = tok.value;

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
        ci->table = tok.value;

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
        ci->index_column = tok.value;

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
    struct query_create_table *crt = &out->create_table;

    /* table name */
    tok = lexer_next(l);
    if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
        crt->table = tok.value;
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

    uint32_t col_start_idx = (uint32_t)out->arena.columns.count;
    uint32_t col_count = 0;
    for (;;) {
        /* column name */
        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_KEYWORD) {
            fprintf(stderr, "parse error: expected column name\n");
            return -1;
        }
        char *col_name = bump_strndup(&out->arena.bump, tok.value.data, tok.value.len);

        /* column type — keyword (INT/FLOAT/TEXT) or identifier (enum type name) */
        tok = lexer_next(l);
        if (tok.type != TOK_KEYWORD && tok.type != TOK_IDENTIFIER) {
            fprintf(stderr, "parse error: expected column type\n");
            return -1;
        }
        int is_serial = sv_eq_ignorecase_cstr(tok.value, "SERIAL") ||
                        sv_eq_ignorecase_cstr(tok.value, "BIGSERIAL");
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
                /* parse optional ON DELETE/UPDATE CASCADE */
                for (;;) {
                    struct token on_peek = lexer_peek(l);
                    if (on_peek.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(on_peek.value, "ON"))
                        break;
                    lexer_next(l); /* consume ON */
                    struct token action = lexer_next(l);
                    struct token cascade = lexer_next(l);
                    if (sv_eq_ignorecase_cstr(action.value, "DELETE") &&
                        sv_eq_ignorecase_cstr(cascade.value, "CASCADE"))
                        col.fk_on_delete_cascade = 1;
                    else if (sv_eq_ignorecase_cstr(action.value, "UPDATE") &&
                             sv_eq_ignorecase_cstr(cascade.value, "CASCADE"))
                        col.fk_on_update_cascade = 1;
                }
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
        arena_push_column(&out->arena, col);
        col_count++;

        tok = lexer_next(l);
        if (tok.type == TOK_RPAREN) break;
        if (tok.type != TOK_COMMA) {
            fprintf(stderr, "parse error: expected ',' or ')' in column list\n");
            return -1;
        }
    }
    crt->columns_start = col_start_idx;
    crt->columns_count = col_count;

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
        out->drop_type.type_name = tok.value;
        return 0;
    }

    /* DROP SEQUENCE name */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "SEQUENCE")) {
        out->query_type = QUERY_TYPE_DROP_SEQUENCE;
        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_STRING) {
            fprintf(stderr, "parse error: expected sequence name after DROP SEQUENCE\n");
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
            fprintf(stderr, "parse error: expected view name after DROP VIEW\n");
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
            fprintf(stderr, "parse error: expected index name after DROP INDEX\n");
            return -1;
        }
        out->drop_index.index_name = tok.value;
        return 0;
    }

    /* DROP TABLE name */
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "TABLE")) {
        fprintf(stderr, "parse error: expected TABLE, INDEX, or TYPE after DROP\n");
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
            fprintf(stderr, "parse error: expected EXISTS after IF\n");
            return -1;
        }
    }
    if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
        out->drop_table.table = tok.value;
    } else {
        fprintf(stderr, "parse error: expected table name after DROP TABLE\n");
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
        fprintf(stderr, "parse error: expected FROM after DELETE\n");
        return -1;
    }

    /* table name */
    tok = lexer_next(l);
    if (tok.type == TOK_IDENTIFIER || tok.type == TOK_STRING) {
        d->table = tok.value;
    } else {
        fprintf(stderr, "parse error: expected table name\n");
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
        fprintf(stderr, "parse error: expected table name after UPDATE\n");
        return -1;
    }
    u->table = tok.value;

    /* SET */
    tok = lexer_next(l);
    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "SET")) {
        fprintf(stderr, "parse error: expected SET after table name\n");
        return -1;
    }

    /* col = val [, col = val ...] */
    uint32_t sc_start = (uint32_t)out->arena.set_clauses.count;
    uint32_t sc_count = 0;
    for (;;) {
        tok = lexer_next(l);
        if (tok.type != TOK_IDENTIFIER) {
            fprintf(stderr, "parse error: expected column name in SET\n");
            return -1;
        }
        struct set_clause sc;
        memset(&sc, 0, sizeof(sc));
        sc.column = tok.value;

        tok = lexer_next(l);
        if (tok.type != TOK_EQUALS) {
            fprintf(stderr, "parse error: expected '=' in SET clause\n");
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
        fprintf(stderr, "parse error: expected TABLE after ALTER\n");
        return -1;
    }

    tok = lexer_next(l);
    if (tok.type != TOK_IDENTIFIER && tok.type != TOK_KEYWORD && tok.type != TOK_STRING) {
        fprintf(stderr, "parse error: expected table name after ALTER TABLE\n");
        return -1;
    }
    a->table = tok.value;

    tok = lexer_next(l);
    if (tok.type != TOK_KEYWORD) {
        fprintf(stderr, "parse error: expected ADD/DROP/RENAME/ALTER after table name\n");
        return -1;
    }

    if (sv_eq_ignorecase_cstr(tok.value, "ADD")) {
        /* ALTER TABLE t ADD [COLUMN] col_name col_type [constraints...] */
        a->alter_action = ALTER_ADD_COLUMN;
        tok = lexer_next(l);
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "COLUMN"))
            tok = lexer_next(l); /* skip optional COLUMN keyword */
        if (tok.type != TOK_IDENTIFIER && tok.type != TOK_KEYWORD) {
            fprintf(stderr, "parse error: expected column name in ADD COLUMN\n");
            return -1;
        }
        a->alter_new_col.name = sv_to_cstr(tok.value);
        tok = lexer_next(l);
        a->alter_new_col.type = parse_column_type(tok.value);
        if (a->alter_new_col.type == COLUMN_TYPE_ENUM)
            a->alter_new_col.enum_type_name = sv_to_cstr(tok.value);
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
            fprintf(stderr, "parse error: expected TO in RENAME COLUMN\n");
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

    fprintf(stderr, "parse error: unsupported ALTER TABLE action\n");
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
        fprintf(stderr, "parse error: expected SQL keyword, got '" SV_FMT "'\n", SV_ARG(tok.value));
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
                fprintf(stderr, "parse error: expected CTE name after WITH\n");
                return -1;
            }
            struct cte_def cte = {0};
            char *name_str = sv_to_cstr(tok.value);
            cte.name_idx = arena_own_string(a, name_str);
            cte.is_recursive = is_recursive;

            tok = lexer_next(&l); /* AS */
            if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "AS")) {
                fprintf(stderr, "parse error: expected AS after CTE name\n");
                return -1;
            }
            tok = lexer_next(&l); /* ( */
            if (tok.type != TOK_LPAREN) {
                fprintf(stderr, "parse error: expected '(' after AS in CTE\n");
                return -1;
            }
            const char *cte_start = l.input + l.pos;
            int depth = 1;
            while (depth > 0) {
                tok = lexer_next(&l);
                if (tok.type == TOK_LPAREN) depth++;
                else if (tok.type == TOK_RPAREN) depth--;
                else if (tok.type == TOK_EOF) {
                    fprintf(stderr, "parse error: unterminated CTE\n");
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
        /* now parse the main SELECT (parse_select resets index fields) */
        if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "SELECT")) {
            fprintf(stderr, "parse error: expected SELECT after CTE\n");
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

    fprintf(stderr, "parse error: unsupported statement '" SV_FMT "'\n", SV_ARG(tok.value));
    return -1;
}

/* ---------------------------------------------------------------------------
 * Arena-based destroy — replaces all recursive free functions.
 * All parser-allocated memory lives in the arena; a single destroy call
 * frees everything.
 * ------------------------------------------------------------------------- */

void query_free(struct query *q)
{
    query_arena_destroy(&q->arena);

    /* free ALTER column (not arena-managed since it uses column_free) */
    if (q->query_type == QUERY_TYPE_ALTER)
        column_free(&q->alter.alter_new_col);
}
