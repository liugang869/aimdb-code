/**
 *  File Name: baseline/hashTable.h
 *  Written By: Shimin Chen, Sept, 2002
 *  Description:
 *	in-memory hash table implementation.
 *
 *	The hash table stores hash codes and pointers to the tuples.
 *	It has an array of hash cells, which contains a hash code and a pointer.
 *	In case of confliction, a variable sized array of hash code and
 *	pointer is allocated.
 *
 *  Modified By liugang (liugang@ict.ac.cn)
 */

#ifndef _HASH_TABLE_H
#define _HASH_TABLE_H
/* ------------------------------------------------------------------------- */

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unordered_map>
#include "mymemory.h"

/* ------------------------------------------------------------------------- */
/** definition of class Hashcode_Ptr. */
class Hashcode_Ptr {
  public:
    int64_t hash_code;  /**< hash_code of specific data  */
    char *tuple;        /**< pointer of a record tuple */
};  // class Hashcode_Ptr

/** definition of class HashCell.  */
class HashCell {
  public:
    int hc_num;   /**< Hashcode_Ptr number in this HashCell  */
    union {
        Hashcode_Ptr ent;        /**< Hashcode_Ptr, hit,decrease cache miss */
        struct {                 // not hit,seach a array,not a link-list,decrease cache miss
            int capacity;        /**< maximun number of array in this structure */
            Hashcode_Ptr *ents;  /**< pointer of Hashcode_Ptr array */
        } num_2_or_more;         /**< struct of a Hashcode_Ptr array,not hit,seach a array,not a link-list,decrease cache miss */
    } hc_union;  /**< if hc_num == 1,store one Hashcode_Ptr; if hc_num>1,store num_2_or_more */
};  // class HashCell

#define hc_ent		hc_union.ent
#define hc_capacity	hc_union.num_2_or_more.capacity
#define hc_ents		hc_union.num_2_or_more.ents

/** definition of class HashTable. */
class HashTable {
  public:
    int estimated_num_distinct_keys;     /**< estimated number of distinct keys,pre-knowledge for this HashTable usage */
    double estimated_duplicates_per_key; /**< estimated number of dupicate keys in average,pre-knowledge for this HashTable usage */
    int initial_array_size;              /**< when hc_num of HashCell exceeds 1 at the fist time, number of Hashcode_Ptr allcated for HashCell */
    char *begin;                         /**< start pointer of HashCells */
    Hashcode_Ptr *free_header[16];       /**< free memory in the list with different number of Hashcode_Ptr,link-list */
                                         // allocate the array
                                         // free_header[0]: all of initial_array_size
                                         // free_header[1]: 2*initial_array_size
                                         // ...
                                         // free_header[k]: 2^k * initial_array_size
    Hashcode_Ptr *avail;                 /**< pointer of next available Hashcode_Ptr */
    Hashcode_Ptr *end;                   /**< the end pointer of Hashcode_Ptr in array */
    HashCell *table;                     /**< pointer of an array of HashCell */
    int table_size;                      /**< the number of HashCells in this table*/
    int more_allocated;                  /**< analysis of more memory allocated from g_memory */
  private:
    std::unordered_map<void*, int> pointer2size;  /**< unordered map, memory pointer to its size,an adapter for mymemory component */
    void *allocate(int size);                     /**< mymemory alloc interface like malloc */
    void  free(void *mem);                        /**< mymemory free interface like free in stdlib */
    int size_to_slot(int array_size);             /**< find the offset in free_header arrray to get suitable free memory used in this HashTable */
    
  public:
    /**
     * constructor.
     * @param estimatedNumDistinctKeys estimated number of distinct keys,pre-knowledge for this HashTable usage
     * @param estimatedDupPerKey       estimated number of dupicate keys in average,pre-knowledge for this HashTable usage
     * @param num_partitions           leave it 0, unuseable
     */
    HashTable(int estimatedNumDistinctKeys, double estimatedDupPerKey,
               int num_partitions);
    /**
     * destructor, free HashTable memory to g_memory.
     */
    ~HashTable();

    /**
     * add an entry.
     * @param  hashCode hash code of specified data
     * @param  tup      pointer of a record tuple
     * @retval true     success
     * @retval false    failure
     */
    bool add(int64_t hashCode, char *tup);
    /**
     * del an entry.
     * @param  hashCode hash code of specified data
     * @param  tup      pointer of a record tuple
     * @retval true     success
     * @retval false    failure
     */
    bool del(int64_t hashCode, char *tup);

    // return num matched
    // <0: means capacity has been reached, there could be more
    // -ret is the last position probed
    /**
     * probe(lookup) entries with specified hashCode.
     * @param  hashCode the specified hashCode to find
     * @param  match    the buffer to store the result matched
     * @param  capacity the maximum number of tuple pointers in this buffer
     * @retval <0       means capacity has been reached, there could be more
     * @retval >=0      means this probe has finished all searching work,retval is the number of result
     */
    int probe(int64_t hashCode, char *match[], int capacity);
    /**
     * probe_contd(lookup) more entries with specified hashCode.
     * @param  hashCode the specified hashCode to find
     * @param  last     inverse number of the retval returned by last call of probe or probe_contd function
     * @param  match    the buffer to store the result matched
     * @param  capacity the maximum number of tuple pointers in this buffer
     * @retval <0       means capacity has been reached, there could be more
     * @retval >=0      means this probe has finished all searching work,retval is the number of result
     */
    int probe_contd(int64_t hashCode, int last, char *match[],
                    int capacity);
    /**
     * display usage analysis of this hash table, for debug use.
     */
    void utilization();
    /**
     * display data in this hash table, for debug use.
     */
    void show();

};                              //HashTable

/* ------------------------------------------------------------------------- */
#endif                          /* _HASH_TABLE_H */
