#include "global.h"

int global_init()
{
    g_catalog.init();
    return g_memory.init(GLOBAL_MEMORY_SIZE, GLOBAL_MEMORY_MINIMUM);
}

int global_shut()
{
    g_catalog.shut();
    g_memory.shut();
    return 0;
}
