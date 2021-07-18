#include "memory.h"
#include "catalog.h"

#define GLOBAL_MEMORY_SIZE    (1L<<31)
#define GLOBAL_MEMORY_MINIMUM (1L<< 3)

#define BNODE_POINTERS_NUM    (16)      //  shoule be 2*m, it's a default value and strongly advised

extern Memory g_memory;
extern Catalog g_catalog;

/**
 * init memory and catalog.
 */
int global_init();

/**
 * shut down catalog and memory.
 */
int global_shut();
