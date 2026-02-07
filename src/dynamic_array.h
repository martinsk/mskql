#ifndef DYNAMIC_ARRAY_H
#define DYNAMIC_ARRAY_H

#include <stdlib.h>
#include <string.h>

#define DYNAMIC_ARRAY(T) \
    struct {             \
        T *items;        \
        size_t count;    \
        size_t capacity; \
    }

#define da_init(da) \
    do { (da)->items = NULL; (da)->count = 0; (da)->capacity = 0; } while (0)

#define da_push(da, item)                                                       \
    do {                                                                         \
        if ((da)->count >= (da)->capacity) {                                     \
            (da)->capacity = (da)->capacity == 0 ? 8 : (da)->capacity * 2;      \
            (da)->items = realloc((da)->items, (da)->capacity * sizeof(*(da)->items)); \
        }                                                                        \
        (da)->items[(da)->count++] = (item);                                     \
    } while (0)

#define da_free(da) \
    do { free((da)->items); (da)->items = NULL; (da)->count = 0; (da)->capacity = 0; } while (0)

#define da_get(da, index) ((da)->items[(index)])

#endif
