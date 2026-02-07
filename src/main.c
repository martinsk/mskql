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

    pgwire_run(&srv);

    pgwire_stop(&srv);
    db_free(&db);

    return 0;
}
