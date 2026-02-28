#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/stat.h>
#include "database.h"
#include "pgwire.h"
#include "diskio.h"

volatile sig_atomic_t g_running = 1;

static void handle_signal(int sig)
{
    (void)sig;
    g_running = 0;
    pgwire_signal_wakeup();
}

int main(void)
{
    signal(SIGPIPE, SIG_IGN);

    struct sigaction sa = {0};
    sa.sa_handler = handle_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    struct database db = {0};
    db_init(&db, "mskql");

    /* Set up disk table catalog */
    const char *data_dir = getenv("MSKQL_DATA_DIR");
    if (!data_dir) data_dir = "mskql_data";
    mkdir(data_dir, 0755);
    char catalog_path[1024];
    snprintf(catalog_path, sizeof(catalog_path), "%s/catalog.mcat", data_dir);
    db.catalog_path = strdup(catalog_path);
    int n_loaded = disk_catalog_load(db.catalog_path, &db);
    if (n_loaded > 0)
        printf("[mskql] loaded %d disk table(s) from catalog\n", n_loaded);

    int port = 5433;
    const char *port_env = getenv("MSKQL_PORT");
    if (port_env) port = atoi(port_env);

    struct pgwire_server srv = {0};
    if (pgwire_init(&srv, &db, port) != 0) {
        fprintf(stderr, "failed to start pgwire server\n");
        return 1;
    }

    pgwire_run(&srv);

    pgwire_stop(&srv);

    /* Save catalog and compact dirty disk tables before exit */
    if (db.catalog_path) {
        while (db_needs_compaction(&db))
            db_compact_step(&db);
        disk_catalog_save(db.catalog_path, &db);
    }

    db_free(&db);

    return 0;
}
