#ifndef INDEX_H
#define INDEX_H

#include "dynamic_array.h"
#include "row.h"
#include "stringview.h"
#include <stdlib.h>

#define BTREE_ORDER 64
#define MAX_INDEX_COLS 8

struct btree_entry {
    struct cell keys[MAX_INDEX_COLS];
    DYNAMIC_ARRAY(size_t) row_ids;
};

struct btree_node {
    int is_leaf;
    size_t count;
    struct btree_entry entries[BTREE_ORDER - 1];
    struct btree_node *children[BTREE_ORDER];
};

struct index {
    char *name;
    int   ncols;
    int   is_unique;  /* 1 if created from PRIMARY KEY or UNIQUE constraint */
    char *column_names[MAX_INDEX_COLS];
    int   column_indices[MAX_INDEX_COLS];
    struct btree_node *root;
};

void index_init(struct index *idx, const char *name,
                const char *const *col_names, const int *col_indices, int ncols);
void index_init_sv(struct index *idx, sv name,
                   const sv *col_names, const int *col_indices, int ncols);
void index_insert(struct index *idx, const struct cell *keys, size_t row_id);
int  index_lookup(struct index *idx, const struct cell *keys,
                  size_t **out_ids, size_t *out_count);
void index_remove(struct index *idx, const struct cell *keys, size_t row_id);
void index_reset(struct index *idx);
void index_free(struct index *idx);

#endif
