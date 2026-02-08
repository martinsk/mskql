#ifndef INDEX_H
#define INDEX_H

#include "dynamic_array.h"
#include "row.h"
#include "stringview.h"
#include <stdlib.h>

#define BTREE_ORDER 64

struct btree_entry {
    struct cell key;
    DYNAMIC_ARRAY(size_t) row_ids;
};

struct btree_node {
    int is_leaf;
    size_t count;
    struct btree_entry entries[BTREE_ORDER - 1];
    struct btree_node *children[BTREE_ORDER];
};

struct index {
    // TODO: STRINGVIEW OPPORTUNITY: name and column_name are char* that get strdup'd
    // and freed repeatedly (e.g. rebuild_indexes does strdup → free → strdup cycle).
    // Could be sv if the schema had a persistent backing store.
    char *name;
    char *column_name;
    int   column_idx;
    struct btree_node *root;
};

void index_init(struct index *idx, const char *name, const char *col_name, int col_idx);
void index_init_sv(struct index *idx, sv name, sv col_name, int col_idx);
void index_insert(struct index *idx, const struct cell *key, size_t row_id);
int  index_lookup(struct index *idx, const struct cell *key,
                  size_t **out_ids, size_t *out_count);
void index_reset(struct index *idx);
void index_free(struct index *idx);

#endif
