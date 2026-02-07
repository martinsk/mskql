#ifndef PGWIRE_H
#define PGWIRE_H

#include "database.h"

struct pgwire_server {
    int listen_fd;
    int port;
    struct database *db;
};

int  pgwire_init(struct pgwire_server *srv, struct database *db, int port);
int  pgwire_run(struct pgwire_server *srv);
void pgwire_stop(struct pgwire_server *srv);

#endif
