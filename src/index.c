#include "index.h"
#include <string.h>
#include <stdio.h>

/* cell_compare / cells_compare → shared from row.h */

static struct btree_node *node_alloc(int is_leaf)
{
    struct btree_node *n = calloc(1, sizeof(*n));
    if (!n) { fprintf(stderr, "node_alloc: out of memory\n"); abort(); }
    n->is_leaf = is_leaf;
    n->count = 0;
    return n;
}

static void entry_free(struct btree_entry *e, int ncols)
{
    for (int c = 0; c < ncols; c++) {
        if (column_type_is_text(e->keys[c].type) && e->keys[c].value.as_text)
            free(e->keys[c].value.as_text);
    }
    da_free(&e->row_ids);
}

/* ---- insert into a non-full node ---- */

static void insert_nonfull(struct btree_node *node, const struct cell *keys, int ncols, size_t row_id);
static void split_child(struct btree_node *parent, size_t idx);

static void split_child(struct btree_node *parent, size_t idx)
{
    struct btree_node *full = parent->children[idx];
    size_t mid = (BTREE_ORDER - 1) / 2;
    struct btree_node *right = node_alloc(full->is_leaf);

    right->count = full->count - mid - 1;
    for (size_t i = 0; i < right->count; i++)
        right->entries[i] = full->entries[mid + 1 + i];

    if (!full->is_leaf) {
        for (size_t i = 0; i <= right->count; i++)
            right->children[i] = full->children[mid + 1 + i];
    }

    /* shift parent entries right */
    for (size_t i = parent->count; i > idx; i--) {
        parent->entries[i] = parent->entries[i - 1];
        parent->children[i + 1] = parent->children[i];
    }

    parent->entries[idx] = full->entries[mid];
    parent->children[idx + 1] = right;
    parent->count++;
    full->count = mid;
}

static void insert_nonfull(struct btree_node *node, const struct cell *keys, int ncols, size_t row_id)
{
    int i = (int)node->count - 1;

    if (node->is_leaf) {
        /* find position, check for duplicate key */
        while (i >= 0 && cells_compare(keys, node->entries[i].keys, ncols) < 0)
            i--;

        if (i >= 0 && cells_compare(keys, node->entries[i].keys, ncols) == 0) {
            /* duplicate key — add row_id to existing entry */
            da_push(&node->entries[i].row_ids, row_id);
            return;
        }

        /* shift entries right and insert */
        i++;
        for (int j = (int)node->count; j > i; j--)
            node->entries[j] = node->entries[j - 1];

        memset(&node->entries[i], 0, sizeof(node->entries[i]));
        for (int c = 0; c < ncols; c++)
            cell_copy(&node->entries[i].keys[c], &keys[c]);
        da_init(&node->entries[i].row_ids);
        da_push(&node->entries[i].row_ids, row_id);
        node->count++;
    } else {
        while (i >= 0 && cells_compare(keys, node->entries[i].keys, ncols) < 0)
            i--;

        if (i >= 0 && cells_compare(keys, node->entries[i].keys, ncols) == 0) {
            da_push(&node->entries[i].row_ids, row_id);
            return;
        }

        i++;
        if (node->children[i]->count == BTREE_ORDER - 1) {
            split_child(node, (size_t)i);
            if (cells_compare(keys, node->entries[i].keys, ncols) == 0) {
                da_push(&node->entries[i].row_ids, row_id);
                return;
            }
            if (cells_compare(keys, node->entries[i].keys, ncols) > 0)
                i++;
        }
        insert_nonfull(node->children[i], keys, ncols, row_id);
    }
}

/* ---- lookup ---- */

static struct btree_entry *node_lookup(struct btree_node *node, const struct cell *keys, int ncols)
{
    if (!node) return NULL;

    size_t i = 0;
    while (i < node->count && cells_compare(keys, node->entries[i].keys, ncols) > 0)
        i++;

    if (i < node->count && cells_compare(keys, node->entries[i].keys, ncols) == 0)
        return &node->entries[i];

    if (node->is_leaf)
        return NULL;

    return node_lookup(node->children[i], keys, ncols);
}

/* ---- free ---- */

static void node_free(struct btree_node *node, int ncols)
{
    if (!node) return;
    for (size_t i = 0; i < node->count; i++)
        entry_free(&node->entries[i], ncols);
    if (!node->is_leaf) {
        for (size_t i = 0; i <= node->count; i++)
            node_free(node->children[i], ncols);
    }
    free(node);
}

/* ---- public API ---- */

void index_init(struct index *idx, const char *name,
                const char *const *col_names, const int *col_indices, int ncols)
{
    idx->name = strdup(name);
    idx->ncols = ncols;
    for (int i = 0; i < ncols; i++) {
        idx->column_names[i] = strdup(col_names[i]);
        idx->column_indices[i] = col_indices[i];
    }
    idx->root = node_alloc(1);
}

void index_init_sv(struct index *idx, sv name,
                   const sv *col_names, const int *col_indices, int ncols)
{
    idx->name = sv_to_cstr(name);
    idx->ncols = ncols;
    for (int i = 0; i < ncols; i++) {
        idx->column_names[i] = sv_to_cstr(col_names[i]);
        idx->column_indices[i] = col_indices[i];
    }
    idx->root = node_alloc(1);
}

void index_insert(struct index *idx, const struct cell *keys, size_t row_id)
{
    if (idx->root->count == BTREE_ORDER - 1) {
        struct btree_node *old_root = idx->root;
        struct btree_node *new_root = node_alloc(0);
        new_root->children[0] = old_root;
        split_child(new_root, 0);
        idx->root = new_root;
    }
    insert_nonfull(idx->root, keys, idx->ncols, row_id);
}

int index_lookup(struct index *idx, const struct cell *keys,
                 size_t **out_ids, size_t *out_count)
{
    struct btree_entry *e = node_lookup(idx->root, keys, idx->ncols);
    if (!e) {
        *out_ids = NULL;
        *out_count = 0;
        return 0;
    }
    *out_ids = e->row_ids.items;
    *out_count = e->row_ids.count;
    return 0;
}

void index_remove(struct index *idx, const struct cell *keys, size_t row_id)
{
    struct btree_entry *e = node_lookup(idx->root, keys, idx->ncols);
    if (!e) return;
    for (size_t i = 0; i < e->row_ids.count; i++) {
        if (e->row_ids.items[i] == row_id) {
            e->row_ids.items[i] = e->row_ids.items[e->row_ids.count - 1];
            e->row_ids.count--;
            return;
        }
    }
}

void index_reset(struct index *idx)
{
    node_free(idx->root, idx->ncols);
    idx->root = node_alloc(1);
}

void index_free(struct index *idx)
{
    free(idx->name);
    for (int i = 0; i < idx->ncols; i++)
        free(idx->column_names[i]);
    node_free(idx->root, idx->ncols);
    idx->root = NULL;
}
