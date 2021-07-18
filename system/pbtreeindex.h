/**
 * @file    pbtreeindex.h 
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *
 *  pbtree index, here we support ID data type(INT8/16/32/64/DATE/TIME/DATETIME)
 *  the value accessed at Element is the pointer of related recored.
 *  this implementation permits duplicated key with different value to be inserted, but not element with same key and value
 *  del operation will only del the first one which meet requirement, if you want delete all, you can call it many times till it returns false
 *
 *  @basic usage:
 *
 *  for each insert,del,look,scan, this file provides 2 same name method to handle 2 type data format you can use 
 *  (1) for lookup, you should all use set_ls to set BptreeInfo with proper value, the first param is valid,
 *      leave the second to be NULL, BptreeInfo can help you iterately get values you need.
 *  (2) call lookup to get the value iterately.
 *  (3) scan you can scan to iterately get the value you need
 *
 */

#ifndef _PBTREEINDEX_H
#define _PBTREEINDEX_H

#include "schema.h"
#include "pbtree.h"

// support INT, id type data
#define PBTREEINFO_CAPICITY (16)

/** definition of PbtreeInfo.  */
struct PbtreeInfo {
    key_type left;   /**< lookup key or scan left edge */
    key_type right;  /**< scan right edge */
    void *l_ptr;     /**< lookup elements ptr */
    void *s_ptr;     /**< scan bnode ptr, acquired by lookup_s */
    int   s_pos;     /**< scan pos in bnode, acquired by lookup_s */ 
    int   s_num;     /**< scan area buffer number, should be init, return scan num */
    int   s_end;     /**< scan tag, scan has more? 0 means more */
    void *area[PBTREEINFO_CAPICITY];   /**< buffer for Pbtree */
    void *result[PBTREEINFO_CAPICITY]; /**< buffer for element */
    int   cr_area;   /**< current area array pos in use */
    int   cr_resu;   /**< current result array pos in use */
    int   pos_resu;  /**< pos in match array, init 0 */
    int   le_resu;   /**< len of current result */
};

class PbtreeIndex : public Index {
  private:
    Pbtree     pi_pbtree;    /**< Pbtree, allow duplicated elements */
    BasicType *pi_datatype;  /**< datatype of this column */

  public:
    /**
     *  constructor.
     *  @param pi_id pbtree index identifier
     *  @param i_name index name
     *  @param i_key key of this index
     */
    PbtreeIndex (int64_t pi_id, const char *i_name, Key &i_key)
        :Index (pi_id, i_name, BPTREEINDEX, i_key)
    {
    }
    /**
     *  init PbtreeIndex
     *  @retval true successfully inited
     */
    bool init (void);
    /**
     * add indexed column's data type.
     * @param i_dt  data type of indexed column
     * @param offset offset of column in a rowtable
     */
    bool setIndexDTpye(BasicType * i_dt);
    /**
     * free memory of Pbtree and other strucure.
     * @retval true  success
     */
    bool shut(void);

    //  data operator
    /**
     * insert an entry to pbtree index.
     * @param  i_data buffer of column data in pattren
     * @param  p_in   pointer of record to make index
     * @retval true   success
     * @retval false  failure
     */
    bool insert(void *i_data, void *p_in);
    /**
     * setup for pbtree index lookup.
     * @param  i_data1 buffer of column data for lookup or scan(">=")
     * @param  i_data2 set NULL
     * @param  info    PbtreeInfo pointer
     * @retval true    success
     * @retval false   failure
     */
    bool set_ls(void *i_data1, void *i_data2, void *info);
    /**
     * setup for pbtree index lookup.
     * @param  i_data1 pointers of column data for lookup
     * @param  i_data2 set NULL when call
     * @param  info    PbtreeInfo pointer
     * @retval true    success
     * @retval false   failure
     */
    bool lookup(void *i_data, void *info, void *&result);
    /**
     * iterate on calling to scan for values, > left value & < right value
     * @param  info    pointer of an index info
     * @param  result  return the pointer to the indexed row
     * @retval true    has more values
     * @retval false   no more values
     */
    bool scan (void *info, void *&result);
    /**
     * del an entry pbtree index.
     * @param  i_data  buffer of column data
     * @param  p_del   pointer of row to delete
     * @retval true    success
     * @retval false   failure
     */
    bool del(void *i_data, void *p_del);
    /**
     * print Pbtree index information.
     */
    void print(void);
    
};

#endif
