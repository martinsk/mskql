#include "column.h"
#include <string.h>
#include <stdlib.h>

void enum_type_free(struct enum_type *et)
{
    free(et->name);
    for (size_t i = 0; i < et->values.count; i++)
        free(et->values.items[i]);
    da_free(&et->values);
}

int enum_type_valid(struct enum_type *et, const char *value)
{
    for (size_t i = 0; i < et->values.count; i++) {
        if (strcmp(et->values.items[i], value) == 0)
            return 1;
    }
    return 0;
}
