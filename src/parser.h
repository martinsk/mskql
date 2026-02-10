#ifndef PARSER_H
#define PARSER_H

#include "query.h"

int query_parse(const char *sql, struct query *out);

/* Free functions live in parser.c (the allocating module) per JPL rules. */
void query_free(struct query *q);
void condition_free(struct condition *c);
void condition_release_subquery_sql(struct condition *c);
void expr_free(struct expr *e);

#endif
