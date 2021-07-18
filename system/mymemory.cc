/**
 * @file    mymemory.cc
 * @autrhor liugang(liugang@ict.ac.cn)
 * @version 0.1
 * 
 * @section DESCRIPTION
 *
 *  Memory is system own manager to alloc and free slab of different size
 *  the minmux size is sizeof(void*), maxsize is given by m_array_list size
 *    
 *  interface: int64_t alloc (char *&p, int64_t size)
 *             int64_t free  (char  *p, int64_t size)
 *  you should put down the size of memory allocated, when you free back, you need this data
 *
 */

#include "mymemory.h"

Memory g_memory;

int Memory::init(int64_t total, int64_t mins)
{
    if ((uint64_t)mins < sizeof(void *)) {
        printf("[Memory][ERROR][init]: mins size is small! -1\n");
        return -1;
    }
    m_total = total;
    m_mins = mins;
    m_table_addr= (char*)TABLE_MEMORY_INIT_ADDR;
    // m_head = new char[m_total];
    m_head = (char*) mmap (NON_TABLE_MEMORY_ADDR, total, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_FIXED, -1, 0);
    if (m_head == MAP_FAILED) {
        printf("[Memory][SERIOUS][init]: m_total: %ld, malloc fail!\n", m_total);
        return -1;
    }
    m_curr = m_head;
    m_tail = m_head + total;
    unsigned int slot_val = slot(m_total);
    m_array_list = new char *[slot_val + 1];
    for (unsigned int ii = 0; ii < slot_val + 1; ii++)
        m_array_list[ii] = NULL;
    return MEMORY_OK;
}

int Memory::shut(void)
{
    // delete [] m_head;
    if (munmap (NON_TABLE_MEMORY_ADDR, m_total)) {
        printf("[Memory][ERROR][shut]: munmap non-table error!-50\n");
        return -1;
    }
    // not deal with m_table_addr because not mmap here
    delete [] m_array_list;
    return MEMORY_OK;
}

int64_t Memory::alloc(char *&p, int64_t size)
{
    // printf ("alloc size: %ld\n", size);
    if (size < m_mins) {
        printf("[Memory][ERROR][alloc]: size is less than m_mins! -2\n");
        return -2;
    }
    unsigned int slot_val = slot(size);
    if ((1L << slot_val) * m_mins != size) {
        printf("[Memory][ERROR][alloc]: size is not power of 2! -3\n");
        return -3;
    }
    if (m_array_list[slot_val]) {
        p = m_array_list[slot_val];
        m_array_list[slot_val] = *(char **) m_array_list[slot_val];
        return size;
    }
    return alloc_default(p, size);
}

int64_t Memory::free(char *p, int64_t size)
{
    unsigned int slot_val = slot(size);
    *(char **) p = m_array_list[slot_val];
    m_array_list[slot_val] = p;
    return size;
}

unsigned int Memory::slot(int64_t size)
{
    unsigned int slot = 0;
    int64_t mask = m_mins;
    while (!(size & mask)) {
        slot++;
        mask = mask << 1;
    }
    return slot;
}

int64_t Memory::alloc_default(char *&p, int64_t size)
{
    if (m_curr + size > m_tail) {
        printf("[Memory][SERIOUS][alloc_default]: exceed memory! -4\n");
        return -4;
    }
    p = m_curr;
    m_curr += size;
    return size;
}

int Memory::print(void)
{
    printf("\n-------------------------------------------\n");
    printf("total: %ld mins: %ld used: %lu\n", m_total, m_mins,
           (uint64_t) (m_curr - m_head));
    std::vector < int >print;
    int slot_val = slot(m_total);
    for (int ii = 0; ii < slot_val + 1; ii++) {
        int cnt = 0;
        char *p = m_array_list[ii];
        while (p) {
            cnt++;
            p = *(char **) p;
        }
        print.push_back(cnt);
    }
    printf("free slab of different size: \n");
    for (int ii = 0; ii < slot_val + 1; ii++) {
        printf("%d\t", print[ii]);
    }
    printf("\n-------------------------------------------\n");
    return MEMORY_OK;
}

int Memory::allocTableAddr (char *&p) {
    p = m_table_addr;
    if (m_table_addr > TABLE_MEMORY_MAX_ADDR)
        return -1;
    m_table_addr += TABLE_MEMORY_ALLOC_MAX;
    return 0;
}
