/**
 * @file    hashindex.h 
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *
 *  hash index, here we support all type data. and you can use multi-keys, and I will guarantee it's right
 *  the value accessed at Element is the pointer of related recored.
 *  this implementation permits duplicated key with different value to be inserted, but not element with same key and value
 *  del operation will only del the first one which meet requirement, if you want delete all, you can call it many times till it returns false
 *
 *  @basic usage:
 *
 *  for each insert,del,look,scan, this file provides 2 same name method to handle 2 type data format you can use 
 *  (1) for lookup, you should all use set_ls to set HashInfo with proper value, the first param is valid,
 *      leave the second to be NULL, HashInfo can help you iterately get values you need.
 *  (2) call lookup to get the value iterately.
 *
 */

#ifndef _HASHINDEX_H
#define _HASHINDEX_H

#include "schema.h"
#include "hashtable.h"

// support INT and CHARN, id type data
#define HASHINFO_CAPICITY (8)

/** definition of HashInfo.  */
struct HashInfo {
    char *result[HASHINFO_CAPICITY]; /**< buffer for value of void *pointer */
    int     rnum;                    /**< current result number */
    int     ppos;                    /**< pair with last, |last| */
    int     last;                    /**< retval of HashTable lookup */
    int64_t hash;                    /**< hashcell position in HashTable */
};

/** definition of HashIndex.  */
class HashIndex:public Index {
  private:
    HashTable *ih_hashtable;  /**< main part hash table */
    int64_t ih_cell_capbits;  /**< as cellnum is power of 2, so the number of bits can use is log2(cellnum) */
    int64_t *ih_hash_bits;    /**< each column assigend bits when hashing */
    BasicType **ih_datatype;  /**< each column data type */
    int64_t ih_column_num;    /**< current number of added columns */
    int64_t ih_column_cap;    /**< got from parent class, the number of columns in the key */
    int64_t *ih_table_offset; /**< offsets of column key in rowtable */

  public:
    /**
     *  constructor.
     *  @param h_id hash index identifier
     *  @param i_name index name
     *  @param i_key key of this index
     */
    HashIndex(int64_t h_id, const char *i_name, Key & i_key)
        :Index(h_id,i_name,HASHINDEX,i_key)
    {
    }
    // init process bool init(void);
    /**
     * init hashindex, to calculate initial value.
     * @retval true  init success
     */
    bool init (void);
    /**
     * set hashtable cell capicity.
     * @param cell_capbits the number of cells in hashtable is 2^cell_capbits
     */
    void setCellCap(int64_t cell_capbits) {  // cell szie is power of cell_capbits by 2
        this->ih_cell_capbits = cell_capbits;
    }
    /**
     * add indexed column's data type.
     * @param i_dt  data type of indexed column
     * @param offset offset of column in a rowtable
     */
    bool addIndexDTpye(BasicType * i_dt,int64_t offset);
    /**
     * init of hash table, heart of hash index ,most important.
     * @retval true  success
     * @retval false failure
     */
    bool finish(void);
    /**
     * free memory of hash table and other strucure.
     * @retval true  success
     */
    bool shut(void);

    //  data operator
    /**
     * insert an entry to hash index.
     * @param  i_data buffer of column data in pattren
     * @param  p_in   pointer of record to make index
     * @retval true   success
     * @retval false  failure
     */
    bool insert(void *i_data, void *p_in);
    /**
     * insert an entry to hash index.
     * @param  i_data each element of i_data pointed to a column data
     * @param  p_in   pointer of record to make index
     * @retval true   success
     * @retval false  failure
     */
    bool insert(void *i_data[], void *p_in);
    /**
     * setup for hash index lookup.
     * @param  i_data1 buffer of column data for lookup or scan(">=")
     * @param  i_data2 set NULL
     * @param  info    HashInfo pointer
     * @retval true    success
     * @retval false   failure
     */
    bool set_ls(void *i_data1, void *i_data2, void *info);
    /**
     * setup for hash index lookup.
     * @param  i_data1 pointers of column data for lookup
     * @param  i_data2 set NULL when call
     * @param  info    HashInfo pointer
     * @retval true    success
     * @retval false   failure
     */
    bool set_ls(void *i_data1[], void *i_data2[], void *info);
    /**
     * lookup hash index.
     * @param  i_data  buffer of column data
     * @param  info    HashInfo pointer processed by set_ls
     * @param  result  reference of record pointer
     * @retval true    found
     * @retval false   not found
     */
    bool lookup(void *i_data, void *info, void *&result);
    /**
     * lookup hash index.
     * @param  i_data  pointers of column data
     * @param  info    HashInfo pointer processed by set_ls
     * @param  result  reference of record pointer
     * @retval true    found
     * @retval false   not found
     */
    bool lookup(void *i_data[], void *info, void *&result);
    /**
     * del an entry in hash index.
     * @param  i_data  buffer of column data
     * @retval true    success
     * @retval false   failure
     */
    bool del(void *i_data);
    /**
     * del an entry in hash index.
     * @param  i_data  pointers of column data
     * @retval true    success
     * @retval false   failure
     */
    bool del(void *i_data[]);
    /**
     * print hash index information.
     */
    void print(void);
 
  private:
    /**
     * assemble hash keys, INT and CHARN.
     * @param  i_data buffer of column data
     * @retval int64  hash index code
     */
    int64_t tranToInt64(void *i_data);
    /**
     * assemble hash keys, INT and CHARN.
     * @param  i_data pointers of column data
     * @retval int64  hash index code
     */
    int64_t tranToInt64(void *i_data[]);
    /**
     * hash method for CHARN.
     * @param str    pointer of string
     * @param maxlen maximum length of str
     * @retval int64 hash value of CHARN
     */
    int64_t hash(char *str, int64_t maxlen);
    /**
     * whether i_data is result got from hash table
     * @param  i_data pointers of columns 
     * @param  result got from hash table
     * @retval true   equal
     * @retval false  not equal
     */
    bool cmpEQ(void *i_data[], void *result);
    /**
     * whether i_data is result got from hash table
     * @param  i_data buffer data of columns 
     * @param  result got from hash table
     * @retval true   equal
     * @retval false  not equal
     */
    bool cmpEQ(void *i_data, void *result);
};
#endif
