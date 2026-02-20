#ifndef ARENA_H
#define ARENA_H

#include <stdint.h>
#include <stdarg.h>
#include <stdio.h>
#include "dynamic_array.h"
#include "row.h"
#include "column.h"
#include "stringview.h"

#define IDX_NONE ((uint32_t)0xFFFFFFFF)

/* Bump allocator: a chain of fixed-size slabs.
 * New slabs are malloc'd — old slabs are never moved or freed until
 * bump_destroy, so pointers into any slab remain valid.
 * On reset, rewind to the first slab (keep all memory). */

struct bump_slab {
    char             *buf;
    size_t            used;
    size_t            capacity;
    struct bump_slab *next;  /* newer slab, or NULL */
};

struct bump_alloc {
    struct bump_slab *head;    /* first (oldest) slab */
    struct bump_slab *current; /* slab currently being allocated from */
};

static inline void bump_init(struct bump_alloc *b)
{
    b->head = NULL;
    b->current = NULL;
}

static inline void bump_reset(struct bump_alloc *b)
{
    /* Rewind all slabs to used=0, keep memory allocated. */
    for (struct bump_slab *s = b->head; s; s = s->next)
        s->used = 0;
    b->current = b->head;
}

/* Watermark for partial reset: save/restore allocator position.
 * Everything allocated after bump_save can be freed by bump_restore
 * while preserving earlier allocations. */
struct bump_mark {
    struct bump_slab *slab;
    size_t            used;
};

static inline struct bump_mark bump_save(const struct bump_alloc *b)
{
    struct bump_mark m;
    m.slab = b->current;
    m.used = b->current ? b->current->used : 0;
    return m;
}

static inline void bump_restore(struct bump_alloc *b, struct bump_mark m)
{
    /* Zero slabs after the saved slab, then restore saved slab position */
    if (m.slab) {
        for (struct bump_slab *s = m.slab->next; s; s = s->next)
            s->used = 0;
        m.slab->used = m.used;
        b->current = m.slab;
    } else {
        bump_reset(b);
    }
}

static inline void bump_destroy(struct bump_alloc *b)
{
    struct bump_slab *s = b->head;
    while (s) {
        struct bump_slab *next = s->next;
        free(s->buf);
        free(s);
        s = next;
    }
    b->head = NULL;
    b->current = NULL;
}

/* Allocate n bytes from the bump slab chain (8-byte aligned).
 * Never moves existing allocations. */
static inline void *bump_alloc(struct bump_alloc *b, size_t n)
{
    size_t aligned = (n + 7) & ~(size_t)7;

    /* Try current slab */
    if (b->current && b->current->used + aligned <= b->current->capacity) {
        void *ptr = b->current->buf + b->current->used;
        b->current->used += aligned;
        return ptr;
    }

    /* Try next existing slab (from a previous reset cycle) */
    if (b->current && b->current->next) {
        struct bump_slab *ns = b->current->next;
        if (ns->capacity >= aligned) {
            b->current = ns;
            void *ptr = ns->buf + ns->used;
            ns->used += aligned;
            return ptr;
        }
    }

    /* Allocate a new slab */
    size_t slab_cap = 4096;
    if (b->current) slab_cap = b->current->capacity * 2;
    if (slab_cap < aligned) slab_cap = aligned;

    struct bump_slab *ns = (struct bump_slab *)malloc(sizeof(struct bump_slab));
    if (!ns) { fprintf(stderr, "bump_alloc: OOM\n"); abort(); }
    ns->buf = (char *)malloc(slab_cap);
    if (!ns->buf) { fprintf(stderr, "bump_alloc: OOM\n"); abort(); }
    ns->capacity = slab_cap;
    ns->used = aligned;
    ns->next = NULL;

    if (b->current) {
        /* Insert after current, before current->next */
        ns->next = b->current->next;
        b->current->next = ns;
    } else if (b->head) {
        /* Shouldn't happen, but handle gracefully */
        ns->next = b->head;
        b->head = ns;
    } else {
        b->head = ns;
    }
    b->current = ns;
    return ns->buf;
}

/* Allocate n zero-initialized bytes from the bump slab chain. */
static inline void *bump_calloc(struct bump_alloc *b, size_t count, size_t size)
{
    size_t total = count * size;
    void *ptr = bump_alloc(b, total);
    memset(ptr, 0, total);
    return ptr;
}

/* Copy a string of known length into the bump slab, NUL-terminated. */
static inline char *bump_strndup(struct bump_alloc *b, const char *s, size_t len)
{
    char *dst = (char *)bump_alloc(b, len + 1);
    memcpy(dst, s, len);
    dst[len] = '\0';
    return dst;
}

/* Copy a NUL-terminated string into the bump slab. */
static inline char *bump_strdup(struct bump_alloc *b, const char *s)
{
    return bump_strndup(b, s, strlen(s));
}

/* forward declarations — full definitions in query.h */
struct expr;
struct condition;
struct case_when_branch;
struct set_clause;
struct select_column;
struct select_expr;
struct agg_expr;
struct order_by_item;
struct join_info;
struct cte_def;
struct plan_node;

/* Pool-based arena: each parsed type gets its own flat dynamic array.
 * Structures reference items by uint32_t index instead of pointers.
 * Freeing the entire query is a single query_arena_destroy() call.
 * For connection-scoped reuse, query_arena_reset() sets counts to 0
 * but keeps all backing memory allocated. */
struct query_arena {
    DYNAMIC_ARRAY(struct expr)             exprs;
    DYNAMIC_ARRAY(struct condition)        conditions;
    DYNAMIC_ARRAY(struct cell)             cells;       /* literal values, IN lists, etc. */
    DYNAMIC_ARRAY(char *)                  strings;     /* pointers into bump slab */
    DYNAMIC_ARRAY(struct case_when_branch) branches;
    DYNAMIC_ARRAY(struct join_info)        joins;
    DYNAMIC_ARRAY(struct cte_def)          ctes;
    DYNAMIC_ARRAY(struct set_clause)       set_clauses;
    DYNAMIC_ARRAY(struct order_by_item)    order_items;
    DYNAMIC_ARRAY(struct select_column)    select_cols;
    DYNAMIC_ARRAY(struct select_expr)      select_exprs;
    DYNAMIC_ARRAY(struct agg_expr)         aggregates;
    DYNAMIC_ARRAY(struct row)              rows;        /* INSERT value tuples, literal SELECT */
    DYNAMIC_ARRAY(sv)                      svs;         /* sv values (multi_columns, group_by) */
    DYNAMIC_ARRAY(struct column)           columns;     /* CREATE TABLE columns */
    DYNAMIC_ARRAY(uint32_t)                arg_indices; /* func call arg expr indices */
    DYNAMIC_ARRAY(struct plan_node)         plan_nodes;  /* query plan tree nodes */
    /* bump slab for strings, scratch buffers, result row cells */
    struct bump_alloc bump;
    /* result rows: bump-allocated cells, reset with arena */
    struct rows result;
    /* bump slab for result row text — bulk-freed instead of per-cell free */
    struct bump_alloc result_text;
    /* scratch area for temporary per-query allocations */
    struct bump_alloc scratch;
    /* error reporting: first-error-wins */
    char errmsg[256];
    char sqlstate[6]; /* 5-char SQLSTATE + NUL */
};

/* Set an error message on the arena (first-error-wins: only the first
 * call actually writes, so the root cause is preserved). */
static inline void arena_set_error(struct query_arena *a,
                                   const char *state,
                                   const char *fmt, ...)
{
    if (a->errmsg[0] != '\0') return; /* first error wins */
    if (state) {
        memcpy(a->sqlstate, state, sizeof(a->sqlstate) - 1);
        a->sqlstate[sizeof(a->sqlstate) - 1] = '\0';
    }
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(a->errmsg, sizeof(a->errmsg), fmt, ap);
    va_end(ap);
}

static inline void query_arena_init(struct query_arena *a)
{
    da_init(&a->exprs);
    da_init(&a->conditions);
    da_init(&a->cells);
    da_init(&a->strings);
    da_init(&a->branches);
    da_init(&a->joins);
    da_init(&a->ctes);
    da_init(&a->set_clauses);
    da_init(&a->order_items);
    da_init(&a->select_cols);
    da_init(&a->select_exprs);
    da_init(&a->aggregates);
    da_init(&a->rows);
    da_init(&a->svs);
    da_init(&a->columns);
    da_init(&a->arg_indices);
    da_init(&a->plan_nodes);
    bump_init(&a->bump);
    a->result.data = NULL;
    a->result.count = 0;
    a->result.capacity = 0;
    a->result.arena_owns_text = 0;
    bump_init(&a->result_text);
    bump_init(&a->scratch);
    a->errmsg[0] = '\0';
    a->sqlstate[0] = '\0';
}

/* helpers that only need cell / char* / sv / row / column (complete here) */

static inline uint32_t arena_push_cell(struct query_arena *a, struct cell c)
{
    da_push(&a->cells, c);
    return (uint32_t)(a->cells.count - 1);
}

static inline uint32_t arena_store_string(struct query_arena *a, const char *s, size_t len)
{
    char *copy = bump_strndup(&a->bump, s, len);
    da_push(&a->strings, copy);
    return (uint32_t)(a->strings.count - 1);
}

/* Transfer ownership of a malloc'd string into the bump slab.
 * The original pointer is freed after copying. */
static inline uint32_t arena_own_string(struct query_arena *a, char *s)
{
    char *copy = bump_strdup(&a->bump, s);
    free(s);
    da_push(&a->strings, copy);
    return (uint32_t)(a->strings.count - 1);
}

static inline uint32_t arena_push_sv(struct query_arena *a, sv s)
{
    da_push(&a->svs, s);
    return (uint32_t)(a->svs.count - 1);
}

static inline uint32_t arena_push_row(struct query_arena *a, struct row r)
{
    da_push(&a->rows, r);
    return (uint32_t)(a->rows.count - 1);
}

static inline uint32_t arena_push_column(struct query_arena *a, struct column col)
{
    da_push(&a->columns, col);
    return (uint32_t)(a->columns.count - 1);
}

/* accessor macros for dereferencing arena indices */
#define EXPR(arena, idx)      ((arena)->exprs.items[(idx)])
#define COND(arena, idx)      ((arena)->conditions.items[(idx)])
#define ACELL(arena, idx)     ((arena)->cells.items[(idx)])
#define ASTRING(arena, idx)   ((arena)->strings.items[(idx)])
#define ABRANCH(arena, idx)   ((arena)->branches.items[(idx)])
#define ASV(arena, idx)       ((arena)->svs.items[(idx)])
#define FUNC_ARG(arena, start, i) ((arena)->arg_indices.items[(start) + (i)])
#define PLAN_NODE(arena, idx)  ((arena)->plan_nodes.items[(idx)])

#endif
