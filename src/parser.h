#ifndef PARSER_H
#define PARSER_H

#include "query.h"

int query_parse(const char *sql, struct query *out);

#endif
