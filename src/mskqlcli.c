/*
 * mskqlcli.c — minimal PostgreSQL wire-protocol CLI client
 *
 * Speaks the v3 Simple Query protocol. Designed as a drop-in replacement
 * for psql in benchmarks and tests (batch mode only, no interactive REPL).
 *
 * Usage:
 *   mskqlcli -h HOST -p PORT -U USER -d DB -f file.sql
 *   mskqlcli -h HOST -p PORT -U USER -d DB -c "SELECT 1"
 *   echo "SELECT 1" | mskqlcli -h HOST -p PORT -U USER -d DB
 *
 * Compatible flags: -t (tuples only), -A (unaligned), -X (no-psqlrc, no-op),
 *                   -q (quiet), --no-psqlrc (no-op)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <errno.h>

/* ------------------------------------------------------------------ */
/*  Big-endian helpers                                                  */
/* ------------------------------------------------------------------ */

static void put_u32(uint8_t *buf, uint32_t v)
{
    buf[0] = (v >> 24) & 0xff;
    buf[1] = (v >> 16) & 0xff;
    buf[2] = (v >>  8) & 0xff;
    buf[3] =  v        & 0xff;
}

static uint32_t get_u32(const uint8_t *buf)
{
    return ((uint32_t)buf[0] << 24) |
           ((uint32_t)buf[1] << 16) |
           ((uint32_t)buf[2] <<  8) |
           ((uint32_t)buf[3]);
}

static uint16_t get_u16(const uint8_t *buf)
{
    return (uint16_t)((buf[0] << 8) | buf[1]);
}

/* ------------------------------------------------------------------ */
/*  Low-level I/O                                                      */
/* ------------------------------------------------------------------ */

static int send_all(int fd, const void *buf, size_t len)
{
    const uint8_t *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        p   += n;
        len -= (size_t)n;
    }
    return 0;
}

static int read_all(int fd, void *buf, size_t len)
{
    uint8_t *p = buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        p   += n;
        len -= (size_t)n;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Buffered reader — amortises read() syscalls over many messages      */
/* ------------------------------------------------------------------ */

#define READBUF_SIZE (256 * 1024)

struct readbuf {
    int     fd;
    size_t  pos;
    size_t  len;
    uint8_t buf[READBUF_SIZE];
};

static void readbuf_init(struct readbuf *rb, int fd)
{
    rb->fd  = fd;
    rb->pos = 0;
    rb->len = 0;
}

static int readbuf_read(struct readbuf *rb, void *dst, size_t need)
{
    uint8_t *out = dst;
    while (need > 0) {
        size_t avail = rb->len - rb->pos;
        if (avail > 0) {
            size_t take = avail < need ? avail : need;
            memcpy(out, rb->buf + rb->pos, take);
            rb->pos += take;
            out     += take;
            need    -= take;
        } else {
            ssize_t n = read(rb->fd, rb->buf, READBUF_SIZE);
            if (n < 0) {
                if (errno == EINTR) continue;
                return -1;
            }
            if (n == 0) return -1;
            rb->pos = 0;
            rb->len = (size_t)n;
        }
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/*  TCP connect                                                        */
/* ------------------------------------------------------------------ */

static int tcp_connect(const char *host, int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        close(fd);
        return -1;
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    return fd;
}

/* ------------------------------------------------------------------ */
/*  Startup + auth                                                     */
/* ------------------------------------------------------------------ */

static int pg_startup(int fd, const char *user, const char *database)
{
    uint8_t buf[512];
    size_t off = 8; /* length(4) + version(4) */

    /* user param */
    memcpy(buf + off, "user", 5); off += 5;
    size_t ulen = strlen(user) + 1;
    if (off + ulen + 64 > sizeof(buf)) return -1;
    memcpy(buf + off, user, ulen); off += ulen;

    /* database param */
    if (database) {
        memcpy(buf + off, "database", 9); off += 9;
        size_t dlen = strlen(database) + 1;
        if (off + dlen + 4 > sizeof(buf)) return -1;
        memcpy(buf + off, database, dlen); off += dlen;
    }

    buf[off++] = '\0'; /* terminator */
    put_u32(buf, (uint32_t)off);       /* total length */
    put_u32(buf + 4, 196608);          /* version 3.0 */

    if (send_all(fd, buf, off) != 0) return -1;

    /* read server responses until ReadyForQuery */
    for (;;) {
        uint8_t type;
        if (read_all(fd, &type, 1) != 0) return -1;
        uint8_t lbuf[4];
        if (read_all(fd, lbuf, 4) != 0) return -1;
        uint32_t mlen = get_u32(lbuf);
        if (mlen < 4) return -1;
        uint32_t body_len = mlen - 4;
        uint8_t body_stack[512];
        uint8_t *body = (body_len <= sizeof(body_stack)) ? body_stack : malloc(body_len);
        if (body_len > 0) {
            if (read_all(fd, body, body_len) != 0) {
                if (body != body_stack) free(body);
                return -1;
            }
        }

        if (type == 'E') {
            /* ErrorResponse — print and fail */
            const char *msg = "";
            size_t pos = 0;
            while (pos < body_len) {
                uint8_t field = body[pos++];
                if (field == 0) break;
                const char *val = (const char *)(body + pos);
                size_t vlen = strlen(val);
                pos += vlen + 1;
                if (field == 'M') msg = val;
            }
            fprintf(stderr, "mskqlcli: startup error: %s\n", msg);
            if (body != body_stack) free(body);
            return -1;
        }

        if (body != body_stack) free(body);
        if (type == 'Z') return 0; /* ReadyForQuery */
    }
}

/* ------------------------------------------------------------------ */
/*  Simple Query execution                                             */
/* ------------------------------------------------------------------ */

/* Output configuration */
struct cli_opts {
    const char *host;
    int         port;
    const char *user;
    const char *database;
    const char *file;
    const char *command;
    int         tuples_only;  /* -t */
    int         unaligned;    /* -A */
    int         quiet;        /* -q */
};

/* Send a Simple Query message and process all responses.
 * Prints DataRow fields to stdout in psql -tA compatible format.
 * Returns 0 on success, -1 on error. */
static int pg_simple_query(struct readbuf *rb, const char *sql, const struct cli_opts *opts)
{
    int fd = rb->fd;
    size_t sql_len = strlen(sql) + 1; /* include NUL */
    size_t total = 1 + 4 + sql_len;   /* 'Q' + length + sql\0 */

    /* build message */
    uint8_t stack[65536];
    uint8_t *msg = (total <= sizeof(stack)) ? stack : malloc(total);
    if (!msg) return -1;
    msg[0] = 'Q';
    put_u32(msg + 1, (uint32_t)(4 + sql_len));
    memcpy(msg + 5, sql, sql_len);

    if (send_all(fd, msg, total) != 0) {
        if (msg != stack) free(msg);
        return -1;
    }
    if (msg != stack) free(msg);

    /* response drain buffer */
    uint8_t drain_stack[65536];
    uint8_t *drain_heap = NULL;
    size_t drain_heap_cap = 0;

    int had_error = 0;
    int had_row_desc = 0;  /* RowDescription received = real SELECT query */

    for (;;) {
        uint8_t hdr[5];
        if (readbuf_read(rb, hdr, 5) != 0) { free(drain_heap); return -1; }

        uint8_t type = hdr[0];
        uint32_t mlen = get_u32(hdr + 1);
        if (mlen < 4) { free(drain_heap); return -1; }
        uint32_t body_len = mlen - 4;

        /* read body */
        uint8_t *body;
        if (body_len == 0) {
            body = drain_stack; /* unused but safe */
        } else if (body_len <= sizeof(drain_stack)) {
            body = drain_stack;
            if (readbuf_read(rb, body, body_len) != 0) { free(drain_heap); return -1; }
        } else {
            if (body_len > drain_heap_cap) {
                free(drain_heap);
                drain_heap_cap = body_len;
                drain_heap = malloc(drain_heap_cap);
                if (!drain_heap) return -1;
            }
            body = drain_heap;
            if (readbuf_read(rb, body, body_len) != 0) { free(drain_heap); return -1; }
        }

        switch (type) {
        case 'T': /* RowDescription — skip (tuples-only mode) */
            had_row_desc = 1;
            break;

        case 'D': { /* DataRow */
            if (body_len < 2) break;
            uint16_t ncols = get_u16(body);
            size_t pos = 2;
            for (uint16_t i = 0; i < ncols; i++) {
                if (pos + 4 > body_len) break;
                int32_t flen = (int32_t)get_u32(body + pos);
                pos += 4;
                if (i > 0) {
                    putchar(opts->unaligned ? '|' : '\t');
                }
                if (flen == -1) {
                    /* NULL — print nothing (matches psql -tA) */
                } else {
                    if ((size_t)flen > 0 && pos + (size_t)flen <= body_len) {
                        fwrite(body + pos, 1, (size_t)flen, stdout);
                    }
                    pos += (size_t)flen;
                }
            }
            putchar('\n');
            break;
        }

        case 'C': { /* CommandComplete — print tag (matches psql -tA) */
            if (body_len > 0) {
                const char *tag = (const char *)body;
                /* psql -tA suppresses SELECT/EXPLAIN/SHOW/COPY tags,
                 * but prints SELECT N from CREATE TABLE AS (no DataRows) */
                int suppress = 0;
                if (strncmp(tag, "EXPLAIN", 7) == 0 ||
                    strncmp(tag, "SHOW", 4) == 0 ||
                    strncmp(tag, "COPY", 4) == 0) {
                    suppress = 1;
                } else if (strncmp(tag, "SELECT", 6) == 0 && had_row_desc) {
                    suppress = 1;
                }
                if (!suppress) {
                    fputs(tag, stdout);
                    putchar('\n');
                }
            }
            had_row_desc = 0;
            break;
        }

        case 'E': { /* ErrorResponse */
            had_error = 1;
            if (!opts->quiet) {
                size_t pos = 0;
                while (pos < body_len) {
                    uint8_t field = body[pos++];
                    if (field == 0) break;
                    const char *val = (const char *)(body + pos);
                    size_t vlen = strlen(val);
                    pos += vlen + 1;
                    if (field == 'M') {
                        /* print to stdout to match psql inline error ordering */
                        printf("ERROR:  %s\n", val);
                    }
                }
            }
            break;
        }

        case 'N': /* NoticeResponse — ignore in quiet mode */
            break;

        case 'S': /* ParameterStatus — ignore */
            break;

        case 'H': /* CopyOutResponse — start of COPY TO STDOUT */
            break;

        case 'd': { /* CopyData — one row of COPY output */
            if (body_len > 0) {
                /* body is the raw row data including trailing newline */
                fwrite(body, 1, body_len, stdout);
            }
            break;
        }

        case 'c': /* CopyDone */
            break;

        case 'I': /* EmptyQueryResponse */
            break;

        case 'Z': /* ReadyForQuery */
            free(drain_heap);
            return had_error ? -1 : 0;

        default:
            /* unknown message type — skip */
            break;
        }
    }
}

/* ------------------------------------------------------------------ */
/*  Multi-statement execution (split by semicolons, like psql -f)       */
/* ------------------------------------------------------------------ */

/* Execute SQL that may contain multiple semicolon-separated statements.
 * Splits on ';' outside of string literals and sends each statement as
 * a separate Simple Query message. This matches psql -f behavior where
 * each statement gets its own query cycle. */
static int pg_exec_multi(struct readbuf *rb, const char *sql, const struct cli_opts *opts)
{
    int had_error = 0;
    const char *p = sql;

    while (*p) {
        /* skip leading whitespace */
        while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
        if (*p == '\0') break;

        /* find end of statement (semicolon outside quotes) */
        const char *start = p;
        int in_single = 0;
        int in_double = 0;
        int in_dollar = 0;
        const char *end = NULL;

        while (*p) {
            if (in_single) {
                if (*p == '\'' && p[1] == '\'') { p += 2; continue; }
                if (*p == '\'') in_single = 0;
            } else if (in_double) {
                if (*p == '"') in_double = 0;
            } else if (in_dollar) {
                /* simplified: $$ ... $$ */
                if (*p == '$' && p[1] == '$') { p += 2; in_dollar = 0; continue; }
            } else {
                if (*p == '\'') { in_single = 1; }
                else if (*p == '"') { in_double = 1; }
                else if (*p == '$' && p[1] == '$') { p += 2; in_dollar = 1; continue; }
                else if (*p == '-' && p[1] == '-') {
                    /* line comment — skip to end of line */
                    while (*p && *p != '\n') p++;
                    continue;
                }
                else if (*p == '/' && p[1] == '*') {
                    /* block comment — skip to close */
                    p += 2;
                    while (*p && !(*p == '*' && p[1] == '/')) p++;
                    if (*p) p += 2;
                    continue;
                }
                else if (*p == ';') {
                    end = p;
                    p++;
                    break;
                }
            }
            p++;
        }

        /* if no semicolon found, end is at the end of string */
        if (!end) end = p;

        /* extract statement */
        size_t stmt_len = (size_t)(end - start);
        if (stmt_len == 0) continue;

        /* skip whitespace-only statements */
        int has_content = 0;
        for (size_t i = 0; i < stmt_len; i++) {
            char c = start[i];
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
                has_content = 1;
                break;
            }
        }
        if (!has_content) continue;

        /* build statement with semicolon appended */
        char stack_stmt[8192];
        char *stmt;
        if (stmt_len + 2 <= sizeof(stack_stmt)) {
            stmt = stack_stmt;
        } else {
            stmt = malloc(stmt_len + 2);
            if (!stmt) return -1;
        }
        memcpy(stmt, start, stmt_len);
        /* ensure it ends with semicolon */
        if (stmt[stmt_len - 1] != ';') {
            stmt[stmt_len] = ';';
            stmt[stmt_len + 1] = '\0';
        } else {
            stmt[stmt_len] = '\0';
        }

        if (pg_simple_query(rb, stmt, opts) != 0)
            had_error = 1;

        if (stmt != stack_stmt) free(stmt);
    }

    return had_error ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/*  File / stdin reading                                               */
/* ------------------------------------------------------------------ */

static char *read_file_contents(const char *path)
{
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "mskqlcli: cannot open '%s': %s\n", path, strerror(errno));
        return NULL;
    }
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (sz < 0) { fclose(f); return NULL; }

    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t nread = fread(buf, 1, (size_t)sz, f);
    buf[nread] = '\0';
    fclose(f);
    return buf;
}

static char *read_stdin_contents(void)
{
    size_t cap = 65536;
    size_t len = 0;
    char *buf = malloc(cap);
    if (!buf) return NULL;

    for (;;) {
        size_t avail = cap - len;
        if (avail < 4096) {
            cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) { free(buf); return NULL; }
            buf = nb;
        }
        size_t n = fread(buf + len, 1, cap - len, stdin);
        if (n == 0) break;
        len += n;
    }
    buf[len] = '\0';
    return buf;
}

/* ------------------------------------------------------------------ */
/*  Argument parsing                                                   */
/* ------------------------------------------------------------------ */

static void usage(void)
{
    fprintf(stderr,
        "Usage: mskqlcli [OPTIONS]\n"
        "  -h HOST    host (default: 127.0.0.1)\n"
        "  -p PORT    port (default: 5432)\n"
        "  -U USER    user (default: $USER)\n"
        "  -d DB      database\n"
        "  -f FILE    execute SQL from file\n"
        "  -c SQL     execute SQL command\n"
        "  -t         tuples only (no headers)\n"
        "  -A         unaligned output mode\n"
        "  -X         no-op (psql compat)\n"
        "  -q         quiet\n"
        "  --no-psqlrc  no-op (psql compat)\n"
    );
}

static int parse_args(int argc, char **argv, struct cli_opts *opts)
{
    memset(opts, 0, sizeof(*opts));
    opts->host = "127.0.0.1";
    opts->port = 5432;
    opts->user = getenv("USER");
    if (!opts->user) opts->user = "postgres";
    opts->tuples_only = 0;
    opts->unaligned = 0;
    opts->quiet = 0;

    for (int i = 1; i < argc; i++) {
        const char *arg = argv[i];

        /* long options */
        if (strcmp(arg, "--no-psqlrc") == 0) continue;
        if (strcmp(arg, "--help") == 0) { usage(); return -1; }

        /* short options */
        if (arg[0] == '-' && arg[1] != '\0' && arg[1] != '-') {
            /* handle combined flags like -tA or -XqtA */
            const char *p = arg + 1;
            while (*p) {
                switch (*p) {
                case 'h':
                    if (p[1]) { opts->host = p + 1; goto next_arg; }
                    if (i + 1 < argc) opts->host = argv[++i];
                    goto next_arg;
                case 'p':
                    if (p[1]) { opts->port = atoi(p + 1); goto next_arg; }
                    if (i + 1 < argc) opts->port = atoi(argv[++i]);
                    goto next_arg;
                case 'U':
                    if (p[1]) { opts->user = p + 1; goto next_arg; }
                    if (i + 1 < argc) opts->user = argv[++i];
                    goto next_arg;
                case 'd':
                    if (p[1]) { opts->database = p + 1; goto next_arg; }
                    if (i + 1 < argc) opts->database = argv[++i];
                    goto next_arg;
                case 'f':
                    if (p[1]) { opts->file = p + 1; goto next_arg; }
                    if (i + 1 < argc) opts->file = argv[++i];
                    goto next_arg;
                case 'c':
                    if (p[1]) { opts->command = p + 1; goto next_arg; }
                    if (i + 1 < argc) opts->command = argv[++i];
                    goto next_arg;
                case 't': opts->tuples_only = 1; break;
                case 'A': opts->unaligned = 1; break;
                case 'X': break; /* no-op */
                case 'q': opts->quiet = 1; break;
                default:
                    fprintf(stderr, "mskqlcli: unknown option '-%c'\n", *p);
                    return -1;
                }
                p++;
            }
        }
        next_arg:;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/*  main                                                               */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
    struct cli_opts opts;
    if (parse_args(argc, argv, &opts) != 0)
        return 1;

    /* determine SQL to execute */
    char *sql = NULL;
    int free_sql = 0;

    if (opts.command) {
        sql = (char *)opts.command;
    } else if (opts.file) {
        sql = read_file_contents(opts.file);
        if (!sql) return 1;
        free_sql = 1;
    } else if (!isatty(STDIN_FILENO)) {
        sql = read_stdin_contents();
        if (!sql) return 1;
        free_sql = 1;
    } else {
        fprintf(stderr, "mskqlcli: no input (use -f, -c, or pipe SQL via stdin)\n");
        return 1;
    }

    /* connect */
    int fd = tcp_connect(opts.host, opts.port);
    if (fd < 0) {
        fprintf(stderr, "mskqlcli: cannot connect to %s:%d: %s\n",
                opts.host, opts.port, strerror(errno));
        if (free_sql) free(sql);
        return 1;
    }

    /* startup */
    if (pg_startup(fd, opts.user, opts.database) != 0) {
        fprintf(stderr, "mskqlcli: startup failed on %s:%d\n", opts.host, opts.port);
        close(fd);
        if (free_sql) free(sql);
        return 1;
    }

    /* set up buffered reader for response processing */
    struct readbuf *rb = malloc(sizeof(*rb));
    if (!rb) {
        fprintf(stderr, "mskqlcli: out of memory\n");
        close(fd);
        if (free_sql) free(sql);
        return 1;
    }
    readbuf_init(rb, fd);

    /* execute: -c sends as single query, -f/stdin splits by semicolons */
    if (opts.command) {
        pg_simple_query(rb, sql, &opts);
    } else {
        pg_exec_multi(rb, sql, &opts);
    }

    /* terminate */
    uint8_t term[5] = { 'X', 0, 0, 0, 4 };
    send_all(fd, term, 5);
    free(rb);
    close(fd);

    if (free_sql) free(sql);
    /* match psql: return 0 even on SQL errors (exit 2 only for connection failures) */
    return 0;
}
