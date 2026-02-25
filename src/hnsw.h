#ifndef HNSW_H
#define HNSW_H

#include <stdint.h>
#include <stdlib.h>

/* ---- HNSW distance types ---- */

enum hnsw_dist_type {
    HNSW_L2,
    HNSW_COSINE,
    HNSW_IP
};

/* ---- HNSW node: one entry in the graph ---- */

struct hnsw_node {
    size_t   row_id;
    uint16_t level;           /* max level this node lives on */
    uint32_t *neighbors;      /* heap: flat array indexed by layer_offset */
    uint32_t *layer_offset;   /* heap: [level+2] cumulative neighbor offsets */
    uint16_t *neighbor_count; /* heap: [level+1] actual count per layer */
};

/* ---- HNSW index ---- */

struct hnsw_index {
    uint16_t dim;
    uint16_t M;               /* max neighbors per layer (default 16) */
    uint16_t M0;              /* max neighbors at layer 0 (2*M) */
    uint16_t ef_construction; /* beam width during build (default 200) */
    float    ml;              /* level multiplier: 1/ln(M) */
    enum hnsw_dist_type dist;
    uint32_t max_level;       /* current highest level in graph */
    uint32_t entry_point;     /* node index of entry point, UINT32_MAX = empty */
    uint32_t count;           /* number of active nodes */
    uint32_t capacity;        /* allocated size of nodes/vectors arrays */
    struct hnsw_node *nodes;  /* heap: [capacity] */
    float *vectors;           /* heap: [capacity * dim] contiguous storage */
    int col_idx;              /* table column index for the vector column */
};

/* ---- HNSW search result ---- */

struct hnsw_result {
    uint32_t *ids;    /* node indices (caller-allocated) */
    float    *dists;  /* distances (caller-allocated) */
    uint32_t  count;  /* number of results written */
};

/* ---- Public API ---- */

void hnsw_init(struct hnsw_index *idx, uint16_t dim, uint16_t M,
               uint16_t ef_construction, enum hnsw_dist_type dist);
void hnsw_free(struct hnsw_index *idx);
void hnsw_insert(struct hnsw_index *idx, const float *vec, size_t row_id);
void hnsw_search(const struct hnsw_index *idx, const float *query,
                 uint32_t k, uint32_t ef_search,
                 size_t *out_row_ids, float *out_dists, uint32_t *out_count);
void hnsw_remove(struct hnsw_index *idx, size_t row_id);

#endif
