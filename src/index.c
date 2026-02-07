#include "index.h"
#include <string.h>
#include <stdio.h>

static int cell_compare(const struct cell *a, const struct cell *b)
{
    /* promote INT <-> FLOAT for numeric comparison */
    if ((a->type == COLUMN_TYPE_INT && b->type == COLUMN_TYPE_FLOAT) ||
        (a->type == COLUMN_TYPE_FLOAT && b->type == COLUMN_TYPE_INT)) {
        double da = (a->type == COLUMN_TYPE_FLOAT) ? a->value.as_float : (double)a->value.as_int;
        double db = (b->type == COLUMN_TYPE_FLOAT) ? b->value.as_float : (double)b->value.as_int;
        if (da < db) return -1;
        if (da > db) return  1;
        return 0;
    }
    if (a->type != b->type) return (int)a->type - (int)b->type;
    switch (a->type) {
        case COLUMN_TYPE_INT:
            if (a->value.as_int < b->value.as_int) return -1;
            if (a->value.as_int > b->value.as_int) return  1;
            return 0;
        case COLUMN_TYPE_FLOAT:
            if (a->value.as_float < b->value.as_float) return -1;
            if (a->value.as_float > b->value.as_float) return  1;
            return 0;
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
            if (!a->value.as_text && !b->value.as_text) return 0;
            if (!a->value.as_text) return -1;
            if (!b->value.as_text) return  1;
            return strcmp(a->value.as_text, b->value.as_text);
    }
    return 0;
}

static struct btree_node *node_alloc(int is_leaf)
{
    // TODO: CRASH RISK: calloc return value is not checked for NULL. If allocation
    // fails, the next line dereferences NULL, causing a segfault.
    struct btree_node *n = calloc(1, sizeof(*n));
    n->is_leaf = is_leaf;
    n->count = 0;
    return n;
}

static void cell_dup(struct cell *dst, const struct cell *src)
{
    dst->type = src->type;
    if ((src->type == COLUMN_TYPE_TEXT || src->type == COLUMN_TYPE_ENUM)
        && src->value.as_text)
        dst->value.as_text = strdup(src->value.as_text);
    else
        dst->value = src->value;
}

static void entry_free(struct btree_entry *e)
{
    if ((e->key.type == COLUMN_TYPE_TEXT || e->key.type == COLUMN_TYPE_ENUM)
        && e->key.value.as_text)
        free(e->key.value.as_text);
    da_free(&e->row_ids);
}

/* ---- insert into a non-full node ---- */

static void insert_nonfull(struct btree_node *node, const struct cell *key, size_t row_id);
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

static void insert_nonfull(struct btree_node *node, const struct cell *key, size_t row_id)
{
    int i = (int)node->count - 1;

    if (node->is_leaf) {
        /* find position, check for duplicate key */
        while (i >= 0 && cell_compare(key, &node->entries[i].key) < 0)
            i--;

        if (i >= 0 && cell_compare(key, &node->entries[i].key) == 0) {
            /* duplicate key — add row_id to existing entry */
            da_push(&node->entries[i].row_ids, row_id);
            return;
        }

        /* shift entries right and insert */
        i++;
        for (int j = (int)node->count; j > i; j--)
            node->entries[j] = node->entries[j - 1];

        memset(&node->entries[i], 0, sizeof(node->entries[i]));
        cell_dup(&node->entries[i].key, key);
        da_init(&node->entries[i].row_ids);
        da_push(&node->entries[i].row_ids, row_id);
        node->count++;
    } else {
        while (i >= 0 && cell_compare(key, &node->entries[i].key) < 0)
            i--;

        if (i >= 0 && cell_compare(key, &node->entries[i].key) == 0) {
            da_push(&node->entries[i].row_ids, row_id);
            return;
        }

        i++;
        if (node->children[i]->count == BTREE_ORDER - 1) {
            split_child(node, (size_t)i);
            if (cell_compare(key, &node->entries[i].key) == 0) {
                da_push(&node->entries[i].row_ids, row_id);
                return;
            }
            if (cell_compare(key, &node->entries[i].key) > 0)
                i++;
        }
        insert_nonfull(node->children[i], key, row_id);
    }
}

/* ---- lookup ---- */

static struct btree_entry *node_lookup(struct btree_node *node, const struct cell *key)
{
    if (!node) return NULL;

    size_t i = 0;
    while (i < node->count && cell_compare(key, &node->entries[i].key) > 0)
        i++;

    if (i < node->count && cell_compare(key, &node->entries[i].key) == 0)
        return &node->entries[i];

    if (node->is_leaf)
        return NULL;

    return node_lookup(node->children[i], key);
}

/* ---- free ---- */

static void node_free(struct btree_node *node)
{
    if (!node) return;
    for (size_t i = 0; i < node->count; i++)
        entry_free(&node->entries[i]);
    if (!node->is_leaf) {
        for (size_t i = 0; i <= node->count; i++)
            node_free(node->children[i]);
    }
    free(node);
}

/* ---- public API ---- */

void index_init(struct index *idx, const char *name, const char *col_name, int col_idx)
{
    idx->name = strdup(name);
    idx->column_name = strdup(col_name);
    idx->column_idx = col_idx;
    idx->root = node_alloc(1);
}

void index_insert(struct index *idx, const struct cell *key, size_t row_id)
{
    if (idx->root->count == BTREE_ORDER - 1) {
        struct btree_node *old_root = idx->root;
        struct btree_node *new_root = node_alloc(0);
        new_root->children[0] = old_root;
        split_child(new_root, 0);
        idx->root = new_root;
    }
    insert_nonfull(idx->root, key, row_id);
}

int index_lookup(struct index *idx, const struct cell *key,
                 size_t **out_ids, size_t *out_count)
{
    struct btree_entry *e = node_lookup(idx->root, key);
    if (!e) {
        *out_ids = NULL;
        *out_count = 0;
        return 0;
    }
    *out_ids = e->row_ids.items;
    *out_count = e->row_ids.count;
    return 0;
}

void index_free(struct index *idx)
{
    free(idx->name);
    free(idx->column_name);
    node_free(idx->root);
    idx->root = NULL;
}
