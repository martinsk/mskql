#include <stdio.h>
#include <signal.h>
#include "database.h"
#include "pgwire.h"

volatile sig_atomic_t g_running = 1;

static void handle_signal(int sig)
{
    (void)sig;
    g_running = 0;
}

int main(void)
{
    struct sigaction sa = {0};
    sa.sa_handler = handle_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

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
