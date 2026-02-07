#include <stdio.h>
#include "database.h"
#include "pgwire.h"

int main(void)
{
    struct database db = {0};
    db_init(&db, "mskql");

    struct pgwire_server srv = {0};
    if (pgwire_init(&srv, &db, 5433) != 0) {
        fprintf(stderr, "failed to start pgwire server\n");
        return 1;
    }

    // TODO: UNREACHABLE CLEANUP: pgwire_run() contains an infinite accept() loop that
    // never returns (no signal handling). The pgwire_stop() and db_free() calls below
    // are dead code. A signal handler (e.g. for SIGINT/SIGTERM) should be installed to
    // break the loop and allow graceful shutdown with proper resource cleanup.
    pgwire_run(&srv);

    pgwire_stop(&srv);
    db_free(&db);

    return 0;
}
