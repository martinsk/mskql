#ifndef PARSER_H
#define PARSER_H

#include "query.h"

int query_parse(const char *sql, struct query *out);

/* Single destroy function — replaces all recursive free functions. */
void query_free(struct query *q);

#endif
