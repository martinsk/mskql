#ifndef PARSER_H
#define PARSER_H

#include "query.h"

int query_parse(const char *sql, struct query *out);

/* Parse into a query using a pre-existing arena (caller owns the arena).
 * The arena is reset before use. query_free must NOT be called on the result;
 * the caller is responsible for the arena lifecycle. */
int query_parse_into(const char *sql, struct query *out, struct query_arena *arena);

/* Single destroy function — replaces all recursive free functions. */
void query_free(struct query *q);

#endif
