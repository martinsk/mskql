#ifndef EXPLAIN_AST_H
#define EXPLAIN_AST_H

#include "query.h"

/* Pretty-print a parsed query_select as a flat structured dump.
 * Output format:
 *   Parse AST (SELECT)
 *     FROM:      table [alias: a]
 *     COLUMNS:   col1, col2  (or *)
 *     WHERE:     id > 10
 *     JOIN:      other b ON t.id = b.fk (INNER)
 *     GROUP BY:  col1, col2
 *     HAVING:    SUM(amount) > 500
 *     ORDER BY:  amount DESC
 *     LIMIT:     100
 *     OFFSET:    0
 *
 * Returns the number of bytes written (excluding null terminator).
 * buf must be at least buflen bytes. */
int query_select_print(struct query_select *s, struct query_arena *arena,
                       char *buf, int buflen);

#endif
