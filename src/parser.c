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
        "INNER", "CROSS", "NATURAL", "USING", "LATERAL",
        "UNION", "INTERSECT", "EXCEPT", "ALL",
        "WITH", "RECURSIVE", "EXISTS",
        "CONFLICT", "DO", "NOTHING",
        "NULLIF", "GREATEST", "LEAST",
        "UPPER", "LOWER", "LENGTH", "SUBSTRING", "TRIM",
        "ANY", "SOME", "ARRAY",
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
            /* consume optional ASC/DESC */
            peek = lexer_peek(l);
            if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "DESC")) {
                lexer_next(l);
                w->order_desc = 1;
            } else if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "ASC")) {
                lexer_next(l);
                w->order_desc = 0;
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
static struct condition *parse_or_cond(struct lexer *l);

static struct expr *parse_expr(struct lexer *l);

static struct expr *expr_alloc(enum expr_type type)
{
    struct expr *e = calloc(1, sizeof(*e));
    e->type = type;
    return e;
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
           sv_eq_ignorecase_cstr(name, "SUBSTRING");
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

/* parse an atom: literal, column ref, function call, CASE, subquery, parens */
static struct expr *parse_expr_atom(struct lexer *l)
{
    struct token tok = lexer_peek(l);

    /* unary minus */
    if (tok.type == TOK_MINUS) {
        lexer_next(l);
        struct expr *operand = parse_expr_atom(l);
        if (!operand) return NULL;
        struct expr *e = expr_alloc(EXPR_UNARY_OP);
        e->unary.op = OP_NEG;
        e->unary.operand = operand;
        return e;
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
                    return NULL;
                }
            }
            const char *sq_end = st.value.data;
            while (sq_end > sq_start && (sq_end[-1] == ' ' || sq_end[-1] == '\n')) sq_end--;
            size_t sq_len = (size_t)(sq_end - sq_start);
            struct expr *e = expr_alloc(EXPR_SUBQUERY);
            e->subquery.sql = malloc(sq_len + 1);
            memcpy(e->subquery.sql, sq_start, sq_len);
            e->subquery.sql[sq_len] = '\0';
            return e;
        }
        /* parenthesized expression */
        struct expr *inner = parse_expr(l);
        if (!inner) return NULL;
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
        struct expr *e = expr_alloc(EXPR_LITERAL);
        e->literal.type = COLUMN_TYPE_TEXT;
        e->literal.is_null = 1;
        return e;
    }

    /* boolean literal */
    if (tok.type == TOK_KEYWORD &&
        (sv_eq_ignorecase_cstr(tok.value, "TRUE") ||
         sv_eq_ignorecase_cstr(tok.value, "FALSE"))) {
        lexer_next(l);
        struct expr *e = expr_alloc(EXPR_LITERAL);
        e->literal.type = COLUMN_TYPE_BOOLEAN;
        e->literal.value.as_bool = sv_eq_ignorecase_cstr(tok.value, "TRUE") ? 1 : 0;
        return e;
    }

    /* CASE WHEN ... THEN ... [ELSE ...] END */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "CASE")) {
        lexer_next(l); /* consume CASE */
        struct expr *e = expr_alloc(EXPR_CASE_WHEN);
        da_init(&e->case_when.branches);
        e->case_when.else_expr = NULL;

        for (;;) {
            tok = lexer_peek(l);
            if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "WHEN")) {
                lexer_next(l); /* consume WHEN */
                struct condition *cond = parse_or_cond(l);
                tok = lexer_next(l); /* consume THEN */
                if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "THEN")) {
                    fprintf(stderr, "parse error: expected THEN in CASE\n");
                    condition_free(cond);
                    /* TODO: free partial expr */
                    return e;
                }
                struct expr *then_e = parse_expr(l);
                struct case_when_branch branch = { .cond = cond, .then_expr = then_e };
                da_push(&e->case_when.branches, branch);
            } else if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "ELSE")) {
                lexer_next(l); /* consume ELSE */
                e->case_when.else_expr = parse_expr(l);
            } else if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "END")) {
                lexer_next(l); /* consume END */
                break;
            } else {
                fprintf(stderr, "parse error: unexpected token in CASE expression\n");
                break;
            }
        }
        return e;
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
            struct expr *e = expr_alloc(EXPR_FUNC_CALL);
            e->func_call.func = expr_func_from_name(tok.value);
            da_init(&e->func_call.args);
            /* parse comma-separated arguments */
            struct token p = lexer_peek(l);
            if (p.type != TOK_RPAREN) {
                for (;;) {
                    struct expr *arg = parse_expr(l);
                    da_push(&e->func_call.args, arg);
                    p = lexer_peek(l);
                    if (p.type == TOK_COMMA) {
                        lexer_next(l); /* consume , */
                    } else {
                        break;
                    }
                }
            }
            tok = lexer_next(l); /* consume ) */
            if (tok.type != TOK_RPAREN) {
                fprintf(stderr, "parse error: expected ')' after function arguments\n");
            }
            (void)peek2;
            return e;
        }
        /* not a function call, restore and fall through to column ref */
        l->pos = saved;
    }

    /* number literal */
    if (tok.type == TOK_NUMBER) {
        lexer_next(l);
        struct expr *e = expr_alloc(EXPR_LITERAL);
        if (sv_contains_char(tok.value, '.')) {
            e->literal.type = COLUMN_TYPE_FLOAT;
            e->literal.value.as_float = sv_atof(tok.value);
        } else {
            long long v = 0;
            size_t k = 0;
            int neg = 0;
            if (tok.value.len > 0 && tok.value.data[0] == '-') { neg = 1; k = 1; }
            for (; k < tok.value.len; k++)
                v = v * 10 + (tok.value.data[k] - '0');
            if (neg) v = -v;
            if (v > 2147483647LL || v < -2147483648LL) {
                e->literal.type = COLUMN_TYPE_BIGINT;
                e->literal.value.as_bigint = v;
            } else {
                e->literal.type = COLUMN_TYPE_INT;
                e->literal.value.as_int = (int)v;
            }
        }
        return e;
    }

    /* string literal */
    if (tok.type == TOK_STRING) {
        lexer_next(l);
        struct expr *e = expr_alloc(EXPR_LITERAL);
        e->literal.type = COLUMN_TYPE_TEXT;
        e->literal.value.as_text = sv_to_cstr(tok.value);
        return e;
    }

    /* column reference: [table.]column or bare identifier/keyword used as column */
    if (tok.type == TOK_KEYWORD && is_expr_terminator_keyword(tok.value)) {
        /* structural keyword — not part of this expression */
        return NULL;
    }
    if (tok.type == TOK_IDENTIFIER || tok.type == TOK_KEYWORD) {
        lexer_next(l);
        sv first = tok.value;
        struct token dot = lexer_peek(l);
        if (dot.type == TOK_DOT) {
            lexer_next(l); /* consume . */
            struct token col_tok = lexer_next(l);
            struct expr *e = expr_alloc(EXPR_COLUMN_REF);
            e->column_ref.table = first;
            e->column_ref.column = col_tok.value;
            return e;
        }
        struct expr *e = expr_alloc(EXPR_COLUMN_REF);
        e->column_ref.table = sv_from(NULL, 0);
        e->column_ref.column = first;
        return e;
    }

    /* star (*) as a column reference (for SELECT *) */
    if (tok.type == TOK_STAR) {
        lexer_next(l);
        struct expr *e = expr_alloc(EXPR_COLUMN_REF);
        e->column_ref.table = sv_from(NULL, 0);
        e->column_ref.column = tok.value;
        return e;
    }

    fprintf(stderr, "parse error: unexpected token in expression: '" SV_FMT "'\n",
            SV_ARG(tok.value));
    return NULL;
}

/* multiplicative: atom (('*' | '/' | '%') atom)* */
static struct expr *parse_expr_mul(struct lexer *l)
{
    struct expr *left = parse_expr_atom(l);
    if (!left) return NULL;

    for (;;) {
        struct token tok = lexer_peek(l);
        enum expr_op op;
        if (tok.type == TOK_STAR)        op = OP_MUL;
        else if (tok.type == TOK_SLASH)  op = OP_DIV;
        else if (tok.type == TOK_PERCENT) op = OP_MOD;
        else break;

        lexer_next(l); /* consume operator */
        struct expr *right = parse_expr_atom(l);
        if (!right) return left;

        struct expr *bin = expr_alloc(EXPR_BINARY_OP);
        bin->binary.op = op;
        bin->binary.left = left;
        bin->binary.right = right;
        left = bin;
    }
    return left;
}

/* additive: mul (('+' | '-') mul)* */
static struct expr *parse_expr_add(struct lexer *l)
{
    struct expr *left = parse_expr_mul(l);
    if (!left) return NULL;

    for (;;) {
        struct token tok = lexer_peek(l);
        enum expr_op op;
        if (tok.type == TOK_PLUS)       op = OP_ADD;
        else if (tok.type == TOK_MINUS) op = OP_SUB;
        else break;

        lexer_next(l); /* consume operator */
        struct expr *right = parse_expr_mul(l);
        if (!right) return left;

        struct expr *bin = expr_alloc(EXPR_BINARY_OP);
        bin->binary.op = op;
        bin->binary.left = left;
        bin->binary.right = right;
        left = bin;
    }
    return left;
}

/* top-level expression: add (('||') add)* */
static struct expr *parse_expr(struct lexer *l)
{
    struct expr *left = parse_expr_add(l);
    if (!left) return NULL;

    for (;;) {
        struct token tok = lexer_peek(l);
        if (tok.type != TOK_PIPE_PIPE) break;

        lexer_next(l); /* consume || */
        struct expr *right = parse_expr_add(l);
        if (!right) return left;

        struct expr *bin = expr_alloc(EXPR_BINARY_OP);
        bin->binary.op = OP_CONCAT;
        bin->binary.left = left;
        bin->binary.right = right;
        left = bin;
    }
    return left;
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
        // TODO: sv_to_cstr heap-allocates; for short-lived comparisons in WHERE
        // conditions this could use a stack buffer or sv-aware cell type
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

/* parse a single comparison or grouped/negated condition */
static struct condition *parse_single_cond(struct lexer *l)
{
    struct token tok = lexer_peek(l);

    /* EXISTS (SELECT ...) */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "EXISTS")) {
        lexer_next(l); /* consume EXISTS */
        struct token lp = lexer_next(l);
        if (lp.type != TOK_LPAREN) {
            fprintf(stderr, "parse error: expected '(' after EXISTS\n");
            return NULL;
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
                return NULL;
            }
        }
        const char *sq_end = l->input + l->pos - 1; /* before closing ')' */
        struct condition *c = calloc(1, sizeof(*c));
        c->type = COND_COMPARE;
        c->op = CMP_EXISTS;
        size_t sql_len = (size_t)(sq_end - sq_start);
        c->subquery_sql = malloc(sql_len + 1);
        memcpy(c->subquery_sql, sq_start, sql_len);
        c->subquery_sql[sql_len] = '\0';
        return c;
    }

    /* NOT expr / NOT EXISTS */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "NOT")) {
        lexer_next(l); /* consume NOT */
        struct token next_peek = lexer_peek(l);
        if (next_peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next_peek.value, "EXISTS")) {
            struct condition *inner = parse_single_cond(l);
            if (!inner) return NULL;
            inner->op = CMP_NOT_EXISTS;
            return inner;
        }
        struct condition *inner = parse_single_cond(l);
        if (!inner) return NULL;
        struct condition *node = calloc(1, sizeof(*node));
        node->type = COND_NOT;
        node->left = inner;
        return node;
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
            /* parse multi-column IN: (col1, col2, ...) [NOT] IN ((v1,v2), (v3,v4), ...) */
            /* l->pos is right after (, so we can parse column list directly */
            struct condition *c = calloc(1, sizeof(*c));
            c->type = COND_MULTI_IN;
            da_init(&c->multi_columns);
            da_init(&c->multi_values);
            for (;;) {
                struct token col = lexer_next(l);
                if (col.type != TOK_IDENTIFIER && col.type != TOK_KEYWORD) {
                    fprintf(stderr, "parse error: expected column in multi-column IN\n");
                    condition_free(c); return NULL;
                }
                sv colsv = consume_identifier(l, col);
                da_push(&c->multi_columns, colsv);
                struct token sep = lexer_next(l);
                if (sep.type == TOK_RPAREN) break;
                if (sep.type != TOK_COMMA) {
                    fprintf(stderr, "parse error: expected ',' or ')' in column list\n");
                    condition_free(c); return NULL;
                }
            }
            c->multi_tuple_width = (int)c->multi_columns.count;
            c->op = CMP_IN;
            struct token kw = lexer_next(l);
            if (kw.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(kw.value, "NOT")) {
                c->op = CMP_NOT_IN;
                kw = lexer_next(l);
            }
            if (kw.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(kw.value, "IN")) {
                fprintf(stderr, "parse error: expected IN after column tuple\n");
                condition_free(c); return NULL;
            }
            struct token lp = lexer_next(l);
            if (lp.type != TOK_LPAREN) {
                fprintf(stderr, "parse error: expected '(' after IN\n");
                condition_free(c); return NULL;
            }
            for (;;) {
                struct token tp = lexer_next(l);
                if (tp.type != TOK_LPAREN) {
                    fprintf(stderr, "parse error: expected '(' for value tuple\n");
                    condition_free(c); return NULL;
                }
                for (int vi = 0; vi < c->multi_tuple_width; vi++) {
                    struct token vt = lexer_next(l);
                    struct cell v = parse_literal_value(vt);
                    da_push(&c->multi_values, v);
                    if (vi < c->multi_tuple_width - 1) {
                        struct token cm = lexer_next(l);
                        if (cm.type != TOK_COMMA) {
                            fprintf(stderr, "parse error: expected ',' in value tuple\n");
                            condition_free(c); return NULL;
                        }
                    }
                }
                struct token rp = lexer_next(l);
                if (rp.type != TOK_RPAREN) {
                    fprintf(stderr, "parse error: expected ')' after value tuple\n");
                    condition_free(c); return NULL;
                }
                struct token sep = lexer_next(l);
                if (sep.type == TOK_RPAREN) break;
                if (sep.type != TOK_COMMA) {
                    fprintf(stderr, "parse error: expected ',' or ')' in IN list\n");
                    condition_free(c); return NULL;
                }
            }
            return c;
        }
        /* not multi-column IN — restore and parse as parenthesized sub-expression */
        l->pos = saved;
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

    struct token op_tok;

    /* check if LHS is a function call (e.g. COALESCE(val, 0) > 5) */
    if (is_expr_func_keyword(tok.value)) {
        struct token peek_lp = lexer_peek(l);
        if (peek_lp.type == TOK_LPAREN) {
            /* back up to before the function name and parse as expression */
            l->pos = tok.value.data - l->input;
            c->lhs_expr = parse_expr(l);
            c->column = sv_from(NULL, 0);
            op_tok = lexer_next(l);
            goto parse_operator;
        }
    }

    c->column = consume_identifier(l, tok);
    op_tok = lexer_next(l);
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
                    condition_free(c); return NULL;
                }
                c->op = CMP_IS_NOT_DISTINCT;
                struct token val_tok = lexer_next(l);
                c->value = parse_literal_value(val_tok);
                return c;
            }
            next = lexer_next(l);
            if (next.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(next.value, "NULL")) {
                fprintf(stderr, "parse error: expected NULL after IS NOT\n");
                condition_free(c); return NULL;
            }
            c->op = CMP_IS_NOT_NULL;
        } else if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "DISTINCT")) {
            struct token from_tok = lexer_next(l);
            if (from_tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(from_tok.value, "FROM")) {
                fprintf(stderr, "parse error: expected FROM after IS DISTINCT\n");
                condition_free(c); return NULL;
            }
            c->op = CMP_IS_DISTINCT;
            struct token val_tok = lexer_next(l);
            c->value = parse_literal_value(val_tok);
            return c;
        } else if (next.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(next.value, "NULL")) {
            c->op = CMP_IS_NULL;
        } else {
            fprintf(stderr, "parse error: expected NULL, NOT, or DISTINCT after IS\n");
            condition_free(c); return NULL;
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
        condition_free(c); return NULL;
    }

    /* IN (...) */
    if (op_tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(op_tok.value, "IN")) {
        c->op = CMP_IN;
parse_in_list:
        tok = lexer_next(l);
        if (tok.type != TOK_LPAREN) {
            fprintf(stderr, "parse error: expected '(' after IN\n");
            condition_free(c); return NULL;
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
                        condition_free(c); return NULL;
                    }
                }
                /* tok is now the closing ')' */
                const char *sq_end = tok.value.data; /* points at ')' */
                size_t sq_len = (size_t)(sq_end - sq_start);
                // TODO: OWNERSHIP VIOLATION (JPL): subquery_sql is malloc'd here in parser.c
                // but freed by condition_free in query.c. Allocator and deallocator are in
                // different files.
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
                condition_free(c); return NULL;
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
            condition_free(c); return NULL;
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
        condition_free(c); return NULL;
    }
    c->op = cmp_from_token(op_tok.type);

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
                condition_free(c); return NULL;
            }
            /* optional ARRAY keyword */
            struct token arr_peek = lexer_peek(l);
            if (arr_peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(arr_peek.value, "ARRAY")) {
                lexer_next(l); /* consume ARRAY */
                struct token lb = lexer_next(l);
                /* expect '[' — but our lexer doesn't have TOK_LBRACKET, so check raw char */
                (void)lb; /* ARRAY[ is handled by consuming until ] */
            }
            da_init(&c->array_values);
            c->is_any = is_all ? 0 : 1;
            c->is_all = is_all ? 1 : 0;
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
                struct cell v = parse_literal_value(vt);
                da_push(&c->array_values, v);
                struct token sep = lexer_peek(l);
                if (sep.type == TOK_COMMA) lexer_next(l);
            }
            return c;
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
                c->scalar_subquery_sql = malloc(sq_len + 1);
                memcpy(c->scalar_subquery_sql, sq_start, sq_len);
                c->scalar_subquery_sql[sq_len] = '\0';
                return c;
            }
            /* not a subquery, restore */
            l->pos = saved;
        }
    }

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

static int parse_where_clause(struct lexer *l, struct where_clause *w)
{
    w->has_where = 1;
    w->where_cond = parse_or_cond(l);
    if (!w->where_cond) return -1;

    /* also fill legacy where_column/where_value for backward compat with index lookup */
    if (w->where_cond->type == COND_COMPARE && w->where_cond->op == CMP_EQ) {
        w->where_column = w->where_cond->column;
        w->where_value = w->where_cond->value;
    }
    return 0;
}

/* parse optional GROUP BY col HAVING ... ORDER BY col [ASC|DESC] LIMIT n OFFSET n */
static void parse_order_limit(struct lexer *l, struct query_select *s)
{
    struct token peek = lexer_peek(l);

    /* GROUP BY col [, col ...] */
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "GROUP")) {
        lexer_next(l);
        struct token by = lexer_next(l);
        if (by.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(by.value, "BY")) {
            s->has_group_by = 1;
            da_init(&s->group_by_cols);
            for (;;) {
                struct token col = lexer_next(l);
                if (col.type == TOK_IDENTIFIER || col.type == TOK_KEYWORD) {
                    sv colsv = consume_identifier(l, col);
                    da_push(&s->group_by_cols, colsv);
                }
                peek = lexer_peek(l);
                if (peek.type != TOK_COMMA) break;
                lexer_next(l); /* consume comma */
            }
            /* backward compat: populate single group_by_col from first item */
            if (s->group_by_cols.count > 0)
                s->group_by_col = s->group_by_cols.items[0];
        }
        peek = lexer_peek(l);
    }

    /* HAVING condition */
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "HAVING")) {
        lexer_next(l);
        s->has_having = 1;
        s->having_cond = parse_or_cond(l);
        peek = lexer_peek(l);
    }

    /* ORDER BY col [ASC|DESC] [, col [ASC|DESC] ...] */
    if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "ORDER")) {
        lexer_next(l);
        struct token by = lexer_next(l);
        if (by.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(by.value, "BY")) return;
        s->has_order_by = 1;
        da_init(&s->order_by_items);
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
            da_push(&s->order_by_items, item);
            peek = lexer_peek(l);
            if (peek.type != TOK_COMMA) break;
            lexer_next(l); /* consume comma */
        }
        /* backward compat: populate single-column fields from first item */
        if (s->order_by_items.count > 0) {
            s->order_by_col = s->order_by_items.items[0].column;
            s->order_desc = s->order_by_items.items[0].desc;
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

static int parse_agg_list(struct lexer *l, struct query_select *s, struct token first)
{
    da_init(&s->aggregates);

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
    /* store optional AS alias */
    {
        struct token pa = lexer_peek(l);
        if (pa.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(pa.value, "AS")) {
            lexer_next(l); /* AS */
            struct token alias_tok = lexer_next(l); /* alias */
            agg.alias = alias_tok.value;
        }
    }
    da_push(&s->aggregates, agg);

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
        /* store optional AS alias */
        {
            struct token pa = lexer_peek(l);
            if (pa.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(pa.value, "AS")) {
                lexer_next(l); /* AS */
                struct token alias_tok = lexer_next(l); /* alias */
                a2.alias = alias_tok.value;
            }
        }
        da_push(&s->aggregates, a2);
    }

    return 0;
}

static int parse_select(struct lexer *l, struct query *out)
{
    out->query_type = QUERY_TYPE_SELECT;
    struct query_select *s = &out->select;

    /* optional DISTINCT */
    struct token peek_dist = lexer_peek(l);
    if (peek_dist.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek_dist.value, "DISTINCT")) {
        lexer_next(l);
        s->has_distinct = 1;
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
                if (parse_agg_list(l, s, tok) != 0) return -1;
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
                da_init(&s->select_exprs);
                struct select_expr se = {0};
                se.kind = SEL_WINDOW;
                if (parse_win_call(l, tok.value, &se.win) != 0) return -1;
                da_push(&s->select_exprs, se);

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
                        da_push(&s->select_exprs, se2);
                    } else if (tok.type == TOK_IDENTIFIER) {
                        struct select_expr se2 = {0};
                        se2.kind = SEL_COLUMN;
                        se2.column = tok.value;
                        da_push(&s->select_exprs, se2);
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
                da_init(&s->select_exprs);
                struct select_expr se = {0};
                se.kind = SEL_COLUMN;
                se.column = first_col;
                da_push(&s->select_exprs, se);

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
                        da_push(&s->select_exprs, se2);
                    } else if (tok.type == TOK_IDENTIFIER) {
                        struct select_expr se2 = {0};
                        se2.kind = SEL_COLUMN;
                        se2.column = tok.value;
                        da_push(&s->select_exprs, se2);
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
                /* collect all plain columns into s->columns sv, aggregates into s->aggregates */
                const char *col_start = first_col.data;
                const char *col_end = first_col.data + first_col.len;
                da_init(&s->aggregates);

                for (;;) {
                    peek = lexer_peek(l);
                    if (peek.type != TOK_COMMA) break;
                    lexer_next(l); /* comma */
                    tok = lexer_next(l);
                    if (tok.type == TOK_KEYWORD && is_agg_keyword(tok.value)) {
                        struct agg_expr agg;
                        if (parse_single_agg(l, tok.value, &agg) != 0) return -1;
                        /* store optional AS alias */
                        struct token pa = lexer_peek(l);
                        if (pa.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(pa.value, "AS")) {
                            lexer_next(l); /* AS */
                            struct token alias_tok = lexer_next(l); /* alias */
                            agg.alias = alias_tok.value;
                        }
                        da_push(&s->aggregates, agg);
                    } else if (tok.type == TOK_IDENTIFIER || tok.type == TOK_KEYWORD) {
                        sv id = consume_identifier(l, tok);
                        col_end = id.data + id.len;
                    } else {
                        fprintf(stderr, "parse error: expected column or aggregate function\n");
                        return -1;
                    }
                }
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
        da_init(&s->parsed_columns);
        const char *raw_col_start = l->input + l->pos;
        skip_whitespace(l);
        raw_col_start = l->input + l->pos;
        const char *raw_col_end = raw_col_start;
        for (;;) {
            struct select_column sc = {0};
            sc.expr = parse_expr(l);
            if (!sc.expr) {
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
            da_push(&s->parsed_columns, sc);
            peek = lexer_peek(l);
            if (peek.type != TOK_COMMA) break;
            lexer_next(l); /* consume comma */
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
        s->from_subquery_sql = malloc(sq_len + 1);
        memcpy(s->from_subquery_sql, sq_start, sq_len);
        s->from_subquery_sql[sq_len] = '\0';
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
    da_init(&s->joins);
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
            ji.lateral_subquery_sql = malloc(sq_len + 1);
            memcpy(ji.lateral_subquery_sql, sq_start, sq_len);
            ji.lateral_subquery_sql[sq_len] = '\0';
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
                free(ji.lateral_subquery_sql);
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

                /* left: table.col */
                tok = lexer_next(l);
                ji.join_left_col = consume_identifier(l, tok);

                /* comparison operator */
                tok = lexer_next(l);
                if (!is_cmp_token(tok.type)) {
                    fprintf(stderr, "parse error: expected comparison operator in ON clause\n");
                    return -1;
                }
                ji.join_op = cmp_from_token(tok.type);

                /* right: table.col */
                tok = lexer_next(l);
                ji.join_right_col = consume_identifier(l, tok);
            }
        }

        da_push(&s->joins, ji);
    }

    /* backwards compat: populate single-join fields from first join */
    if (s->joins.count > 0) {
        s->has_join = 1;
        s->join_type = s->joins.items[0].join_type;
        s->join_table = s->joins.items[0].join_table;
        s->join_left_col = s->joins.items[0].join_left_col;
        s->join_right_col = s->joins.items[0].join_right_col;
    }

    /* optional: WHERE condition */
    {
        struct token peek = lexer_peek(l);
        if (peek.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(peek.value, "WHERE")) {
            lexer_next(l); /* consume WHERE */
            if (parse_where_clause(l, &s->where) != 0) return -1;
        }
    }

    /* optional: GROUP BY / HAVING / ORDER BY / LIMIT / OFFSET */
    parse_order_limit(l, s);

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
                s->set_order_by = malloc(ob_len + 1);
                memcpy(s->set_order_by, ob_str, ob_len);
                s->set_order_by[ob_len] = '\0';
                rhs_len = last_order;
                /* trim whitespace before ORDER BY */
                while (rhs_len > 0 && rhs_start[rhs_len-1] == ' ') rhs_len--;
            }
            s->set_rhs_sql = malloc(rhs_len + 1);
            memcpy(s->set_rhs_sql, rhs_start, rhs_len);
            s->set_rhs_sql[rhs_len] = '\0';
        }
    }

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
            // TODO: sv_to_cstr heap-allocates per string value in every INSERT row;
            // for bulk inserts this is many small allocations that could be batched
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
    /* INSERT ... SELECT ... */
    if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "SELECT")) {
        /* capture everything from SELECT to end as insert_select_sql */
        const char *sel_start = tok.value.data;
        size_t sel_len = strlen(sel_start);
        while (sel_len > 0 && (sel_start[sel_len-1] == ';' || sel_start[sel_len-1] == ' '
                                || sel_start[sel_len-1] == '\n'))
            sel_len--;
        ins->insert_select_sql = malloc(sel_len + 1);
        memcpy(ins->insert_select_sql, sel_start, sel_len);
        ins->insert_select_sql[sel_len] = '\0';
        return 0;
    }

    if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "VALUES")) {
        fprintf(stderr, "parse error: expected VALUES or SELECT\n");
        return -1;
    }

    /* parse one or more value tuples: (v1, v2), (v3, v4), ... */
    da_init(&ins->insert_rows);
    for (;;) {
        struct row r = {0};
        if (parse_value_tuple(l, &r) != 0) {
            row_free(&r);
            return -1;
        }
        da_push(&ins->insert_rows, r);

        struct token peek = lexer_peek(l);
        if (peek.type == TOK_COMMA) {
            lexer_next(l); /* consume comma between tuples */
        } else {
            break;
        }
    }

    /* backwards compat: point insert_row at first row */
    if (ins->insert_rows.count > 0) {
        ins->insert_row = &ins->insert_rows.items[0];
    }

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
            tok = lexer_next(l); /* NOTHING */
            if (sv_eq_ignorecase_cstr(tok.value, "NOTHING")) {
                ins->on_conflict_do_nothing = 1;
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

        da_init(&ct->enum_values);
        for (;;) {
            tok = lexer_next(l);
            if (tok.type != TOK_STRING) {
                fprintf(stderr, "parse error: expected string value in ENUM list\n");
                return -1;
            }
            da_push(&ct->enum_values, sv_to_cstr(tok.value));

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
        da_push(&crt->create_columns, col);

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
        out->drop_type.type_name = tok.value;
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
    tok = lexer_next(l);
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
        if (parse_where_clause(l, &d->where) != 0) return -1;
    }

    parse_returning_clause(l, &d->has_returning, &d->returning_columns);

    return 0;
}

static int parse_update(struct lexer *l, struct query *out)
{
    out->query_type = QUERY_TYPE_UPDATE;
    struct query_update *u = &out->update;

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
    da_init(&u->set_clauses);
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

        /* parse the value as an expression AST */
        sc.expr = parse_expr(l);
        sc.value = (struct cell){0};
        da_push(&u->set_clauses, sc);

        {
            struct token peek = lexer_peek(l);
            if (peek.type != TOK_COMMA) break;
            lexer_next(l); /* consume comma */
        }
    }

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
                if (parse_where_clause(l, &u->where) != 0) return -1;
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

    /* With [RECURSIVE] name AS (...) [, name AS (...) ...] SELECT ... (CTE) */
    if (sv_eq_ignorecase_cstr(tok.value, "WITH")) {
        struct query_select *s = &out->select;
        tok = lexer_next(&l);

        /* optional RECURSIVE keyword */
        int is_recursive = 0;
        if (tok.type == TOK_KEYWORD && sv_eq_ignorecase_cstr(tok.value, "RECURSIVE")) {
            is_recursive = 1;
            s->has_recursive_cte = 1;
            tok = lexer_next(&l);
        }

        da_init(&s->ctes);

        /* set query_type early so query_free takes the SELECT branch on error */
        out->query_type = QUERY_TYPE_SELECT;

        /* parse one or more CTE definitions: name AS (...) [, ...] */
        for (;;) {
            if (tok.type != TOK_IDENTIFIER && tok.type != TOK_KEYWORD) {
                fprintf(stderr, "parse error: expected CTE name after WITH\n");
                return -1;
            }
            struct cte_def cte = {0};
            cte.name = sv_to_cstr(tok.value);
            cte.is_recursive = is_recursive;

            tok = lexer_next(&l); /* AS */
            if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "AS")) {
                fprintf(stderr, "parse error: expected AS after CTE name\n");
                free(cte.name);
                return -1;
            }
            tok = lexer_next(&l); /* ( */
            if (tok.type != TOK_LPAREN) {
                fprintf(stderr, "parse error: expected '(' after AS in CTE\n");
                free(cte.name);
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
                    free(cte.name);
                    return -1;
                }
            }
            const char *cte_end = tok.value.data; /* points at ')' */
            size_t cte_len = (size_t)(cte_end - cte_start);
            while (cte_len > 0 && (cte_start[cte_len-1] == ' ' || cte_start[cte_len-1] == '\n'))
                cte_len--;
            cte.sql = malloc(cte_len + 1);
            memcpy(cte.sql, cte_start, cte_len);
            cte.sql[cte_len] = '\0';

            da_push(&s->ctes, cte);

            /* backward compat: also populate legacy single-CTE fields for first CTE */
            if (s->ctes.count == 1) {
                s->cte_name = strdup(cte.name);
                s->cte_sql = strdup(cte.sql);
            }

            /* check for comma (more CTEs) or SELECT */
            tok = lexer_next(&l);
            if (tok.type == TOK_COMMA) {
                tok = lexer_next(&l); /* next CTE name */
                continue;
            }
            break;
        }

        /* now parse the main SELECT */
        if (tok.type != TOK_KEYWORD || !sv_eq_ignorecase_cstr(tok.value, "SELECT")) {
            fprintf(stderr, "parse error: expected SELECT after CTE\n");
            return -1;
        }
        return parse_select(&l, out);
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

/* ---------------------------------------------------------------------------
 * Free functions — parser.c owns the lifecycle of all query structs it
 * allocates (conditions, set_clauses, insert_rows, CTEs, subquery SQL, etc.).
 * Placing deallocators here satisfies the JPL rule that the allocating module
 * must also be the deallocating module.
 * ------------------------------------------------------------------------- */

void condition_free(struct condition *c)
{
    if (!c) return;
    if (c->type == COND_AND || c->type == COND_OR) {
        condition_free(c->left);
        condition_free(c->right);
    } else if (c->type == COND_NOT) {
        condition_free(c->left);
    } else if (c->type == COND_MULTI_IN) {
        da_free(&c->multi_columns);
        for (size_t i = 0; i < c->multi_values.count; i++)
            cell_free_text(&c->multi_values.items[i]);
        da_free(&c->multi_values);
    } else if (c->type == COND_COMPARE) {
        expr_free(c->lhs_expr);
        cell_free_text(&c->value);
        /* free IN / NOT IN value list */
        for (size_t i = 0; i < c->in_values.count; i++)
            cell_free_text(&c->in_values.items[i]);
        da_free(&c->in_values);
        /* free ANY/ALL/SOME array values */
        for (size_t i = 0; i < c->array_values.count; i++)
            cell_free_text(&c->array_values.items[i]);
        da_free(&c->array_values);
        /* free BETWEEN high value */
        cell_free_text(&c->between_high);
        /* free unresolved subquery SQL */
        free(c->subquery_sql);
        free(c->scalar_subquery_sql);
    }
    free(c);
}

void expr_free(struct expr *e)
{
    if (!e) return;
    switch (e->type) {
    case EXPR_LITERAL:
        cell_free_text(&e->literal);
        break;
    case EXPR_COLUMN_REF:
        break; /* sv fields are not owned */
    case EXPR_BINARY_OP:
        expr_free(e->binary.left);
        expr_free(e->binary.right);
        break;
    case EXPR_UNARY_OP:
        expr_free(e->unary.operand);
        break;
    case EXPR_FUNC_CALL:
        for (size_t i = 0; i < e->func_call.args.count; i++)
            expr_free(e->func_call.args.items[i]);
        da_free(&e->func_call.args);
        break;
    case EXPR_CASE_WHEN:
        for (size_t i = 0; i < e->case_when.branches.count; i++) {
            condition_free(e->case_when.branches.items[i].cond);
            expr_free(e->case_when.branches.items[i].then_expr);
        }
        da_free(&e->case_when.branches);
        expr_free(e->case_when.else_expr);
        break;
    case EXPR_SUBQUERY:
        free(e->subquery.sql);
        break;
    }
    free(e);
}

void condition_release_subquery_sql(struct condition *c)
{
    if (!c) return;
    free(c->subquery_sql);
    c->subquery_sql = NULL;
    free(c->scalar_subquery_sql);
    c->scalar_subquery_sql = NULL;
}

static void where_clause_free(struct where_clause *w)
{
    condition_free(w->where_cond);
    w->where_cond = NULL;
    /* where_value is a shallow copy of where_cond->value — already freed above;
     * clear it to avoid dangling pointer if resolve_subqueries overwrote cond->value */
    memset(&w->where_value, 0, sizeof(w->where_value));
}

static void query_select_free(struct query_select *s)
{
    where_clause_free(&s->where);
    condition_free(s->having_cond);
    s->having_cond = NULL;
    da_free(&s->aggregates);
    da_free(&s->select_exprs);
    for (size_t i = 0; i < s->parsed_columns.count; i++)
        expr_free(s->parsed_columns.items[i].expr);
    da_free(&s->parsed_columns);
    for (size_t i = 0; i < s->joins.count; i++)
        free(s->joins.items[i].lateral_subquery_sql);
    da_free(&s->joins);
    da_free(&s->order_by_items);
    da_free(&s->group_by_cols);
    free(s->set_rhs_sql);
    free(s->set_order_by);
    free(s->cte_name);
    free(s->cte_sql);
    free(s->from_subquery_sql);
    for (size_t i = 0; i < s->ctes.count; i++) {
        free(s->ctes.items[i].name);
        free(s->ctes.items[i].sql);
    }
    da_free(&s->ctes);
    for (size_t i = 0; i < s->insert_rows.count; i++) {
        for (size_t j = 0; j < s->insert_rows.items[i].cells.count; j++)
            cell_free_text(&s->insert_rows.items[i].cells.items[j]);
        da_free(&s->insert_rows.items[i].cells);
    }
    da_free(&s->insert_rows);
    s->insert_row = NULL;
}

static void query_insert_free(struct query_insert *ins)
{
    for (size_t i = 0; i < ins->insert_rows.count; i++) {
        for (size_t j = 0; j < ins->insert_rows.items[i].cells.count; j++)
            cell_free_text(&ins->insert_rows.items[i].cells.items[j]);
        da_free(&ins->insert_rows.items[i].cells);
    }
    da_free(&ins->insert_rows);
    ins->insert_row = NULL;
    free(ins->insert_select_sql);
}

static void query_update_free(struct query_update *u)
{
    where_clause_free(&u->where);
    for (size_t i = 0; i < u->set_clauses.count; i++) {
        cell_free_text(&u->set_clauses.items[i].value);
        expr_free(u->set_clauses.items[i].expr);
    }
    da_free(&u->set_clauses);
}

static void query_delete_free(struct query_delete *d)
{
    where_clause_free(&d->where);
}

static void query_create_table_free(struct query_create_table *ct)
{
    for (size_t i = 0; i < ct->create_columns.count; i++)
        column_free(&ct->create_columns.items[i]);
    da_free(&ct->create_columns);
}

static void query_alter_free(struct query_alter *a)
{
    column_free(&a->alter_new_col);
}

static void query_create_type_free(struct query_create_type *ct)
{
    for (size_t i = 0; i < ct->enum_values.count; i++)
        free(ct->enum_values.items[i]);
    da_free(&ct->enum_values);
}

void query_free(struct query *q)
{
    switch (q->query_type) {
    case QUERY_TYPE_SELECT:     query_select_free(&q->select);       break;
    case QUERY_TYPE_INSERT:     query_insert_free(&q->insert);       break;
    case QUERY_TYPE_UPDATE:     query_update_free(&q->update);       break;
    case QUERY_TYPE_DELETE:     query_delete_free(&q->del);          break;
    case QUERY_TYPE_CREATE:     query_create_table_free(&q->create_table); break;
    case QUERY_TYPE_ALTER:      query_alter_free(&q->alter);         break;
    case QUERY_TYPE_CREATE_TYPE: query_create_type_free(&q->create_type); break;
    case QUERY_TYPE_DROP:
    case QUERY_TYPE_DROP_INDEX:
    case QUERY_TYPE_DROP_TYPE:
    case QUERY_TYPE_CREATE_INDEX:
    case QUERY_TYPE_BEGIN:
    case QUERY_TYPE_COMMIT:
    case QUERY_TYPE_ROLLBACK:
        break;
    }
}
