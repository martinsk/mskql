#include "hnsw.h"
#include "vector.h"
#include <string.h>
#include <math.h>
#include <float.h>

/* ---- Distance function dispatch ---- */

typedef float (*hnsw_dist_fn)(const float *, const float *, uint16_t);

static hnsw_dist_fn hnsw_get_dist_fn(enum hnsw_dist_type t)
{
    switch (t) {
    case HNSW_L2:     return vector_l2_distance;
    case HNSW_COSINE: return vector_cosine_distance;
    case HNSW_IP:     return vector_inner_product;
    }
    __builtin_unreachable();
}

/* ---- Priority queue (min-heap by distance) ---- */

struct hnsw_pq {
    uint32_t *ids;
    float    *dists;
    uint32_t  count;
    uint32_t  cap;
};

static void pq_init(struct hnsw_pq *pq, uint32_t cap)
{
    pq->ids   = (uint32_t *)malloc(cap * sizeof(uint32_t));
    pq->dists = (float *)malloc(cap * sizeof(float));
    pq->count = 0;
    pq->cap   = cap;
}

static void pq_free(struct hnsw_pq *pq)
{
    free(pq->ids);
    free(pq->dists);
    pq->ids = NULL;
    pq->dists = NULL;
    pq->count = 0;
    pq->cap = 0;
}

static void pq_swap(struct hnsw_pq *pq, uint32_t a, uint32_t b)
{
    uint32_t ti = pq->ids[a]; pq->ids[a] = pq->ids[b]; pq->ids[b] = ti;
    float td = pq->dists[a]; pq->dists[a] = pq->dists[b]; pq->dists[b] = td;
}

static void pq_sift_up(struct hnsw_pq *pq, uint32_t i)
{
    while (i > 0) {
        uint32_t parent = (i - 1) / 2;
        if (pq->dists[i] < pq->dists[parent]) {
            pq_swap(pq, i, parent);
            i = parent;
        } else break;
    }
}

static void pq_sift_down(struct hnsw_pq *pq, uint32_t i)
{
    for (;;) {
        uint32_t smallest = i;
        uint32_t l = 2 * i + 1, r = 2 * i + 2;
        if (l < pq->count && pq->dists[l] < pq->dists[smallest]) smallest = l;
        if (r < pq->count && pq->dists[r] < pq->dists[smallest]) smallest = r;
        if (smallest == i) break;
        pq_swap(pq, i, smallest);
        i = smallest;
    }
}

static void pq_push(struct hnsw_pq *pq, uint32_t id, float dist)
{
    if (pq->count == pq->cap) {
        pq->cap = pq->cap ? pq->cap * 2 : 16;
        pq->ids   = (uint32_t *)realloc(pq->ids, pq->cap * sizeof(uint32_t));
        pq->dists = (float *)realloc(pq->dists, pq->cap * sizeof(float));
    }
    pq->ids[pq->count] = id;
    pq->dists[pq->count] = dist;
    pq_sift_up(pq, pq->count);
    pq->count++;
}

static void pq_pop(struct hnsw_pq *pq, uint32_t *out_id, float *out_dist)
{
    *out_id = pq->ids[0];
    *out_dist = pq->dists[0];
    pq->count--;
    if (pq->count > 0) {
        pq->ids[0] = pq->ids[pq->count];
        pq->dists[0] = pq->dists[pq->count];
        pq_sift_down(pq, 0);
    }
}

/* Max-heap variant: negate distances to reuse min-heap */

static void pq_push_max(struct hnsw_pq *pq, uint32_t id, float dist)
{
    pq_push(pq, id, -dist);
}

static void pq_pop_max(struct hnsw_pq *pq, uint32_t *out_id, float *out_dist)
{
    pq_pop(pq, out_id, out_dist);
    *out_dist = -(*out_dist);
}

static float pq_peek_max_dist(const struct hnsw_pq *pq)
{
    return -pq->dists[0];
}

/* ---- Helper: get vector pointer for a node ---- */

static const float *node_vec(const struct hnsw_index *idx, uint32_t node_idx)
{
    return &idx->vectors[(size_t)node_idx * idx->dim];
}

/* ---- Helper: get neighbors of node at layer ---- */

static uint32_t *node_neighbors(const struct hnsw_index *idx, uint32_t node_idx, uint16_t layer)
{
    const struct hnsw_node *n = &idx->nodes[node_idx];
    (void)idx;
    return &n->neighbors[n->layer_offset[layer]];
}

static uint16_t node_neighbor_count(const struct hnsw_index *idx, uint32_t node_idx, uint16_t layer)
{
    (void)idx;
    return idx->nodes[node_idx].neighbor_count[layer];
}

/* ---- Helper: allocate node neighbor storage ---- */

static void node_alloc_layers(struct hnsw_node *n, uint16_t level, uint16_t M, uint16_t M0)
{
    n->level = level;
    n->layer_offset = (uint32_t *)calloc(level + 2, sizeof(uint32_t));
    n->neighbor_count = (uint16_t *)calloc(level + 1, sizeof(uint16_t));
    if (!n->layer_offset || !n->neighbor_count) { fprintf(stderr, "OOM: node_alloc_layers\n"); abort(); }
    /* compute offsets: layer 0 gets M0 slots, layers 1..level get M slots */
    uint32_t total = 0;
    n->layer_offset[0] = 0;
    total += M0;
    for (uint16_t l = 1; l <= level; l++) {
        n->layer_offset[l] = total;
        total += M;
    }
    n->layer_offset[level + 1] = total;
    n->neighbors = (uint32_t *)malloc(total * sizeof(uint32_t));
    if (!n->neighbors) { fprintf(stderr, "OOM: node_alloc_layers\n"); abort(); }
}

static void node_free_layers(struct hnsw_node *n)
{
    free(n->neighbors);
    free(n->layer_offset);
    free(n->neighbor_count);
    n->neighbors = NULL;
    n->layer_offset = NULL;
    n->neighbor_count = NULL;
}

/* ---- Random level generation ---- */

static uint16_t hnsw_random_level(float ml)
{
    /* -log(uniform(0,1)) * ml */
    double r = (double)rand() / ((double)RAND_MAX + 1.0);
    if (r == 0.0) r = 1e-9;
    int level = (int)(-log(r) * (double)ml);
    if (level < 0) level = 0;
    if (level > 32) level = 32; /* cap to avoid pathological cases */
    return (uint16_t)level;
}

/* ---- search_layer: beam search at a single layer ---- */

static void search_layer(const struct hnsw_index *idx, const float *query,
                         const uint32_t *ep_ids, uint32_t ep_count,
                         uint32_t ef, uint16_t layer,
                         struct hnsw_pq *results)
{
    hnsw_dist_fn dist_fn = hnsw_get_dist_fn(idx->dist);

    /* visited set */
    uint8_t *visited = (uint8_t *)calloc(idx->count, 1);
    if (!visited) { fprintf(stderr, "OOM: search_layer visited\n"); abort(); }

    /* candidates: min-heap (closest first) */
    struct hnsw_pq candidates;
    pq_init(&candidates, ef + 1);

    /* results: max-heap (farthest on top, for pruning) */
    results->count = 0;

    for (uint32_t i = 0; i < ep_count; i++) {
        uint32_t ep = ep_ids[i];
        if (ep >= idx->count) continue;
        float d = dist_fn(query, node_vec(idx, ep), idx->dim);
        visited[ep] = 1;
        pq_push(&candidates, ep, d);
        pq_push_max(results, ep, d);
    }

    while (candidates.count > 0) {
        uint32_t closest_id;
        float closest_dist;
        pq_pop(&candidates, &closest_id, &closest_dist);

        /* if closest candidate is farther than farthest result, stop */
        if (results->count >= ef && closest_dist > pq_peek_max_dist(results))
            break;

        /* expand neighbors */
        uint16_t ncount = node_neighbor_count(idx, closest_id, layer);
        uint32_t *nbrs = node_neighbors(idx, closest_id, layer);
        for (uint16_t j = 0; j < ncount; j++) {
            uint32_t nbr = nbrs[j];
            if (nbr >= idx->count || visited[nbr]) continue;
            visited[nbr] = 1;
            float d = dist_fn(query, node_vec(idx, nbr), idx->dim);
            if (results->count < ef || d < pq_peek_max_dist(results)) {
                pq_push(&candidates, nbr, d);
                pq_push_max(results, nbr, d);
                /* prune results if over ef */
                if (results->count > ef) {
                    uint32_t dummy_id; float dummy_dist;
                    pq_pop_max(results, &dummy_id, &dummy_dist);
                }
            }
        }
    }

    free(visited);
    pq_free(&candidates);
}

/* ---- select_neighbors: simple closest-M selection ---- */

static void select_neighbors(const struct hnsw_index *idx, const float *query,
                             struct hnsw_pq *candidates, uint16_t M,
                             uint32_t *out_ids, uint16_t *out_count)
{
    /* candidates is a max-heap — pop farthest until M remain */
    while (candidates->count > M) {
        uint32_t dummy_id; float dummy_dist;
        pq_pop_max(candidates, &dummy_id, &dummy_dist);
    }
    /* extract remaining (they are in max-heap order, but order doesn't matter for neighbors) */
    *out_count = 0;
    while (candidates->count > 0) {
        uint32_t id; float dist;
        pq_pop_max(candidates, &id, &dist);
        out_ids[*out_count] = id;
        (*out_count)++;
    }
    (void)idx;
    (void)query;
}

/* ---- add_connection: add bidirectional edge, prune if over capacity ---- */

static void add_connection(struct hnsw_index *idx, uint32_t from, uint32_t to,
                           uint16_t layer, float dist)
{
    struct hnsw_node *n = &idx->nodes[from];
    uint16_t max_nbrs = (layer == 0) ? idx->M0 : idx->M;
    uint16_t cnt = n->neighbor_count[layer];

    if (cnt < max_nbrs) {
        n->neighbors[n->layer_offset[layer] + cnt] = to;
        n->neighbor_count[layer] = cnt + 1;
        return;
    }

    /* full — find farthest neighbor and replace if new is closer */
    hnsw_dist_fn dist_fn = hnsw_get_dist_fn(idx->dist);
    const float *from_vec = node_vec(idx, from);
    uint16_t worst_i = 0;
    float worst_dist = dist_fn(from_vec, node_vec(idx, n->neighbors[n->layer_offset[layer]]), idx->dim);
    for (uint16_t i = 1; i < cnt; i++) {
        float d = dist_fn(from_vec, node_vec(idx, n->neighbors[n->layer_offset[layer] + i]), idx->dim);
        if (d > worst_dist) {
            worst_dist = d;
            worst_i = i;
        }
    }
    if (dist < worst_dist) {
        n->neighbors[n->layer_offset[layer] + worst_i] = to;
    }
}

/* ---- Public API ---- */

void hnsw_init(struct hnsw_index *idx, uint16_t dim, uint16_t M,
               uint16_t ef_construction, enum hnsw_dist_type dist)
{
    memset(idx, 0, sizeof(*idx));
    idx->dim = dim;
    idx->M = M;
    idx->M0 = M * 2;
    idx->ef_construction = ef_construction;
    idx->ml = 1.0f / logf((float)M);
    idx->dist = dist;
    idx->max_level = 0;
    idx->entry_point = UINT32_MAX;
    idx->count = 0;
    idx->capacity = 0;
    idx->nodes = NULL;
    idx->vectors = NULL;
    idx->col_idx = -1;
}

void hnsw_free(struct hnsw_index *idx)
{
    for (uint32_t i = 0; i < idx->count; i++)
        node_free_layers(&idx->nodes[i]);
    free(idx->nodes);
    free(idx->vectors);
    idx->nodes = NULL;
    idx->vectors = NULL;
    idx->count = 0;
    idx->capacity = 0;
    idx->entry_point = UINT32_MAX;
}

void hnsw_insert(struct hnsw_index *idx, const float *vec, size_t row_id)
{
    /* grow arrays if needed */
    if (idx->count == idx->capacity) {
        uint32_t new_cap = idx->capacity ? idx->capacity * 2 : 64;
        idx->nodes = (struct hnsw_node *)realloc(idx->nodes, new_cap * sizeof(struct hnsw_node));
        idx->vectors = (float *)realloc(idx->vectors, (size_t)new_cap * idx->dim * sizeof(float));
        idx->capacity = new_cap;
    }

    uint32_t new_idx = idx->count;
    uint16_t new_level = hnsw_random_level(idx->ml);

    /* store vector */
    memcpy(&idx->vectors[(size_t)new_idx * idx->dim], vec, idx->dim * sizeof(float));

    /* allocate node */
    struct hnsw_node *nn = &idx->nodes[new_idx];
    memset(nn, 0, sizeof(*nn));
    nn->row_id = row_id;
    node_alloc_layers(nn, new_level, idx->M, idx->M0);
    idx->count++;

    /* first node: set as entry point */
    if (idx->entry_point == UINT32_MAX) {
        idx->entry_point = new_idx;
        idx->max_level = new_level;
        return;
    }

    hnsw_dist_fn dist_fn = hnsw_get_dist_fn(idx->dist);
    uint32_t cur_ep = idx->entry_point;

    /* greedy descent from top level to new_level + 1 */
    for (int l = (int)idx->max_level; l > (int)new_level; l--) {
        uint16_t ncount = node_neighbor_count(idx, cur_ep, (uint16_t)l);
        uint32_t *nbrs = node_neighbors(idx, cur_ep, (uint16_t)l);
        float cur_dist = dist_fn(vec, node_vec(idx, cur_ep), idx->dim);
        int changed = 1;
        while (changed) {
            changed = 0;
            ncount = node_neighbor_count(idx, cur_ep, (uint16_t)l);
            nbrs = node_neighbors(idx, cur_ep, (uint16_t)l);
            for (uint16_t j = 0; j < ncount; j++) {
                uint32_t nbr = nbrs[j];
                if (nbr >= idx->count) continue;
                float d = dist_fn(vec, node_vec(idx, nbr), idx->dim);
                if (d < cur_dist) {
                    cur_ep = nbr;
                    cur_dist = d;
                    changed = 1;
                    break;
                }
            }
        }
    }

    /* insert at layers min(new_level, max_level) down to 0 */
    uint32_t ep_ids[1] = { cur_ep };
    for (int l = (int)(new_level < idx->max_level ? new_level : idx->max_level); l >= 0; l--) {
        struct hnsw_pq results;
        pq_init(&results, idx->ef_construction + 1);

        search_layer(idx, vec, ep_ids, 1, idx->ef_construction, (uint16_t)l, &results);

        /* select neighbors */
        uint16_t max_nbrs = (l == 0) ? idx->M0 : idx->M;
        uint32_t selected[512];
        uint16_t nsel = 0;
        select_neighbors(idx, vec, &results, max_nbrs, selected, &nsel);
        pq_free(&results);

        /* add bidirectional connections */
        for (uint16_t j = 0; j < nsel; j++) {
            uint32_t nbr = selected[j];
            float d = dist_fn(vec, node_vec(idx, nbr), idx->dim);
            /* new_node -> nbr */
            nn->neighbors[nn->layer_offset[l] + nn->neighbor_count[l]] = nbr;
            if (nn->neighbor_count[l] < max_nbrs)
                nn->neighbor_count[l]++;
            /* nbr -> new_node */
            add_connection(idx, nbr, new_idx, (uint16_t)l, d);
        }

        /* use closest result as entry point for next layer */
        if (nsel > 0) {
            float best_d = FLT_MAX;
            uint32_t best_id = ep_ids[0];
            for (uint16_t j = 0; j < nsel; j++) {
                float d = dist_fn(vec, node_vec(idx, selected[j]), idx->dim);
                if (d < best_d) { best_d = d; best_id = selected[j]; }
            }
            ep_ids[0] = best_id;
        }
    }

    /* update entry point if new node has higher level */
    if (new_level > idx->max_level) {
        idx->entry_point = new_idx;
        idx->max_level = new_level;
    }
}

void hnsw_search(const struct hnsw_index *idx, const float *query,
                 uint32_t k, uint32_t ef_search,
                 size_t *out_row_ids, float *out_dists, uint32_t *out_count)
{
    *out_count = 0;
    if (idx->count == 0 || idx->entry_point == UINT32_MAX) return;
    if (ef_search < k) ef_search = k;

    hnsw_dist_fn dist_fn = hnsw_get_dist_fn(idx->dist);
    uint32_t cur_ep = idx->entry_point;

    /* greedy descent from top level to layer 1 */
    for (int l = (int)idx->max_level; l > 0; l--) {
        float cur_dist = dist_fn(query, node_vec(idx, cur_ep), idx->dim);
        int changed = 1;
        while (changed) {
            changed = 0;
            uint16_t ncount = node_neighbor_count(idx, cur_ep, (uint16_t)l);
            uint32_t *nbrs = node_neighbors(idx, cur_ep, (uint16_t)l);
            for (uint16_t j = 0; j < ncount; j++) {
                uint32_t nbr = nbrs[j];
                if (nbr >= idx->count) continue;
                float d = dist_fn(query, node_vec(idx, nbr), idx->dim);
                if (d < cur_dist) {
                    cur_ep = nbr;
                    cur_dist = d;
                    changed = 1;
                    break;
                }
            }
        }
    }

    /* search at layer 0 */
    uint32_t ep_ids[1] = { cur_ep };
    struct hnsw_pq results;
    pq_init(&results, ef_search + 1);
    search_layer(idx, query, ep_ids, 1, ef_search, 0, &results);

    /* extract top-k sorted by distance (ascending) */
    /* results is a max-heap — pop all, then take closest k */
    uint32_t total = results.count;
    uint32_t *all_ids = (uint32_t *)malloc(total * sizeof(uint32_t));
    float *all_dists = (float *)malloc(total * sizeof(float));
    for (uint32_t i = 0; i < total; i++)
        pq_pop_max(&results, &all_ids[i], &all_dists[i]);
    pq_free(&results);

    /* simple insertion sort by distance (total is small, ≤ ef_search) */
    for (uint32_t i = 1; i < total; i++) {
        uint32_t ti = all_ids[i];
        float td = all_dists[i];
        uint32_t j = i;
        while (j > 0 && all_dists[j - 1] > td) {
            all_ids[j] = all_ids[j - 1];
            all_dists[j] = all_dists[j - 1];
            j--;
        }
        all_ids[j] = ti;
        all_dists[j] = td;
    }

    uint32_t out_n = total < k ? total : k;
    for (uint32_t i = 0; i < out_n; i++) {
        out_row_ids[i] = idx->nodes[all_ids[i]].row_id;
        out_dists[i] = all_dists[i];
    }
    *out_count = out_n;

    free(all_ids);
    free(all_dists);
}

void hnsw_remove(struct hnsw_index *idx, size_t row_id)
{
    /* find node by row_id */
    uint32_t target = UINT32_MAX;
    for (uint32_t i = 0; i < idx->count; i++) {
        if (idx->nodes[i].row_id == row_id) {
            target = i;
            break;
        }
    }
    if (target == UINT32_MAX) return;

    /* disconnect from all neighbors */
    struct hnsw_node *tn = &idx->nodes[target];
    for (uint16_t l = 0; l <= tn->level; l++) {
        uint16_t ncount = tn->neighbor_count[l];
        uint32_t *nbrs = &tn->neighbors[tn->layer_offset[l]];
        for (uint16_t j = 0; j < ncount; j++) {
            uint32_t nbr = nbrs[j];
            if (nbr >= idx->count) continue;
            struct hnsw_node *nn = &idx->nodes[nbr];
            /* remove target from nbr's neighbor list at layer l */
            if (l <= nn->level) {
                uint16_t nc = nn->neighbor_count[l];
                uint32_t *nn_nbrs = &nn->neighbors[nn->layer_offset[l]];
                for (uint16_t k = 0; k < nc; k++) {
                    if (nn_nbrs[k] == target) {
                        nn_nbrs[k] = nn_nbrs[nc - 1];
                        nn->neighbor_count[l] = nc - 1;
                        break;
                    }
                }
            }
        }
    }

    /* swap with last node to keep array compact */
    uint32_t last = idx->count - 1;
    if (target != last) {
        /* update all neighbors of last node to point to target's new index */
        struct hnsw_node *ln = &idx->nodes[last];
        for (uint16_t l = 0; l <= ln->level; l++) {
            uint16_t ncount = ln->neighbor_count[l];
            uint32_t *nbrs = &ln->neighbors[ln->layer_offset[l]];
            for (uint16_t j = 0; j < ncount; j++) {
                uint32_t nbr = nbrs[j];
                if (nbr >= idx->count || nbr == target) continue;
                struct hnsw_node *nn = &idx->nodes[nbr];
                if (l <= nn->level) {
                    uint16_t nc = nn->neighbor_count[l];
                    uint32_t *nn_nbrs = &nn->neighbors[nn->layer_offset[l]];
                    for (uint16_t k = 0; k < nc; k++) {
                        if (nn_nbrs[k] == last) {
                            nn_nbrs[k] = target;
                            break;
                        }
                    }
                }
            }
        }

        /* free target's layers, move last into target's slot */
        node_free_layers(&idx->nodes[target]);
        idx->nodes[target] = idx->nodes[last];
        memcpy(&idx->vectors[(size_t)target * idx->dim],
               &idx->vectors[(size_t)last * idx->dim],
               idx->dim * sizeof(float));

        /* update entry point if it was the last node */
        if (idx->entry_point == last)
            idx->entry_point = target;
    } else {
        node_free_layers(&idx->nodes[target]);
    }

    idx->count--;

    /* update entry point if we removed it */
    if (idx->count == 0) {
        idx->entry_point = UINT32_MAX;
        idx->max_level = 0;
    } else if (target == idx->entry_point || idx->entry_point >= idx->count) {
        /* find new entry point: node with highest level */
        uint32_t best = 0;
        uint16_t best_level = idx->nodes[0].level;
        for (uint32_t i = 1; i < idx->count; i++) {
            if (idx->nodes[i].level > best_level) {
                best_level = idx->nodes[i].level;
                best = i;
            }
        }
        idx->entry_point = best;
        idx->max_level = best_level;
    }
}
