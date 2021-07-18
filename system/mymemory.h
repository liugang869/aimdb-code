/**
 * @file    mymemory.h
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
#ifndef _MYMEMORY_H
#define _MYMEMORY_H

#include <stdint.h>
#include <stdio.h>
#include <vector>
#include <sys/mman.h>

#define MEMORY_OK 0

#define NON_TABLE_MEMORY_ADDR  ((void*)0x600000000000)
#define TABLE_MEMORY_INIT_ADDR ((void*)0x700000000000)
#define TABLE_MEMORY_MAX_ADDR  ((void*)0x7f0000000000)
#define TABLE_MEMORY_ALLOC_MAX (1L<<34)  //  maximum   16  GB per table
#define TABLE_MEMORY_ALLOC_INC (1L<<28)  //  fixed     256 MB per increment

class Memory {
  private:
    char *m_head;          /**< db memory pointer, pointer of a large memory allocated from operate system */
    char *m_curr;          /**< pointer of db memory already in use */
    char *m_tail;          /**< end pointer of db memory allocated from operate system */
    char **m_array_list;   /**< free arrray list */
    int64_t m_total;       /**< total size of database system */
    int64_t m_mins;        /**< minimux size to alloc, at least sizeof(void*), recommend 8 */
    char *m_table_addr;    /**< current mmap table addr usage */

  public:
    /**
     * init db memory.
     * @param total  total size allocated from operate system, usually large enough
     * @param mins   minimux size db object allocated from db memory
     * @retval ==0   success
     * @retval <0    failure
     */
    int init(int64_t total, int64_t mins);
    /**
     * alloc db memory for inside usage.
     * @param  p      store the pointer result allocated from db memory
     * @param  size   required size by caller, power of 2
     * @retval ==size successfuly allocated from db memory
     * @retval <=0    failure
     */
    int64_t alloc(char *&p, int64_t size);
    /**
     * free memory to db memory.
     * @param  p      the pointer of memory to free
     * @param  size   provided by caller, power of 2
     * @retval ==size successfuly free to db memory
     * @retval <=0    failure
     */
    int64_t free(char *p, int64_t size);
    /**
     * free db memory to operate system.
     */
    int shut(void);
    /**
     * print memory usage.
     */
    int print(void);
    /**
     * alloc table address
     * @param  p      the pointer of memory to free
     * @retval 0 successfuly free to db memory
     */
    int allocTableAddr (char *&p);
  private:
    /**
     * calculate position of free list.
     */
    unsigned int slot(int64_t size);
    /**
     * default alloc from db memory when free list has no free memory of this size
     * @param  size   required size by caller, power of 2
     * @retval ==size successfuly allocated from db memory
     * @retval <=0    failure
     */
    int64_t alloc_default(char *&p, int64_t size);
};  // class Memory

extern Memory g_memory;
#endif
