#include "pbtreeindex.h"

/**
 *  init PbtreeIndex
 *  @retval true successfully inited
 */
bool PbtreeIndex::init (void) {
    pi_pbtree.init();
    return true;
}
/**
 * set indexed column's data type.
 * @param i_dt  data type of indexed column
 * @param offset offset of column in a rowtable
 */
bool PbtreeIndex::setIndexDTpye(BasicType * i_dt) {
    pi_datatype = i_dt;
    return true;
}
/**
 * free memory of hash table and other strucure.
 * @retval true  success
 */
bool PbtreeIndex::shut(void) {
    return false;
}

//  data operator
/**
 * insert an entry to hash index.
 * @param  i_data buffer of column data in pattren
 * @param  p_in   pointer of record to make index
 * @retval true   success
 * @retval false  failure
 */
bool PbtreeIndex::insert(void *i_data, void *p_in) {
    key_type key = 0;
    pi_datatype->copy(&key,i_data);
    return pi_pbtree.insert (key, p_in);
}
/**
 * setup for pbtree index lookup.
 * @param  i_data1 buffer of column data for lookup or scan(">=")
 * @param  i_data2 set NULL
 * @param  info    PbtreeInfo pointer
 * @retval true    success
 * @retval false   failure
 */
bool PbtreeIndex::set_ls(void *i_data1, void *i_data2, void *info) {
    PbtreeInfo *pb_info = (PbtreeInfo*)info;
    pb_info->left = 0;
    pb_info->right= 0;
    if (i_data1)
        pi_datatype->copy(&(pb_info->left),i_data1);
    if (i_data2)
        pi_datatype->copy(&(pb_info->right),i_data2);
    if (i_data2) {
        pb_info->s_end = 0;
        pb_info->s_num = 0;
        pb_info->s_ptr = pi_pbtree.lookup_s (pb_info->left, &(pb_info->s_pos));
    }
    else {
        pb_info->pos_resu = 0;
        pb_info->l_ptr = pi_pbtree.lookup (pb_info->left);
        pb_info->le_resu = pi_pbtree.get_recptr (pb_info->l_ptr,pb_info->result,PBTREEINFO_CAPICITY,(pb_info->pos_resu));
    }
    pb_info->cr_area = 0;
    pb_info->cr_resu = 0;
    return true;
}
/**
 * lookup pbtree index.
 * @param  i_data  buffer of column data
 * @param  info    PbtreeInfo pointer processed by set_ls
 * @param  result  reference of record pointer
 * @retval true    found
 * @retval false   not found
 */
bool PbtreeIndex::lookup(void *i_data, void *info, void *&result) {
    PbtreeInfo *pb_info = (PbtreeInfo*)info;
    while (1) {
        if (pb_info->cr_resu < pb_info->le_resu) {
            result = pb_info->result[pb_info->cr_resu++];
            return true;
        }
        pb_info->le_resu = pi_pbtree.get_recptr (pb_info->l_ptr,pb_info->result,PBTREEINFO_CAPICITY,(pb_info->pos_resu));
        if (pb_info->le_resu == 0) return false;
        pb_info->cr_resu = 0;
    }
    return false;
}
/**
 * iterate on calling to scan for values.
 * @param  info    pointer of an index info
 * @param  result  return the pointer to the indexed row
 * @retval true    has more values
 * @retval false   no more values
 */
bool PbtreeIndex::scan (void *info, void *&result) {
    PbtreeInfo *pb_info = (PbtreeInfo*)info;
    while (1) {
        if (pb_info->cr_resu < pb_info->le_resu) {
            result = pb_info->result[pb_info->cr_resu++];
            return true;
        }
        pb_info->le_resu = pi_pbtree.get_recptr (pb_info->area[pb_info->cr_area],pb_info->result,PBTREEINFO_CAPICITY,pb_info->pos_resu);
        if (pb_info->le_resu) {
            pb_info->cr_resu = 0;
            continue;
        }
        pb_info->cr_area += 1;
        if (pb_info->cr_area < pb_info->s_num) {
            pb_info->pos_resu = 0;
            pb_info->le_resu = pi_pbtree.get_recptr (pb_info->area[pb_info->cr_area],pb_info->result,PBTREEINFO_CAPICITY,pb_info->pos_resu);
            pb_info->cr_resu = 0;
            continue;
        }
        if (pb_info->s_end) return false;
        pb_info->s_num = PBTREEINFO_CAPICITY;
        pb_info->s_end = pi_pbtree.scan (&(pb_info->s_ptr),&(pb_info->s_pos),pb_info->left,pb_info->right,pb_info->area,&(pb_info->s_num));
        pb_info->cr_area = 0;
    }
    return false;
}
/**
 * del an entry pbtree index.
 * @param  i_data  buffer of column data
 * @param  p_del   pointer of row to delete
 * @retval true    success
 * @retval false   failure
 */
bool PbtreeIndex::del(void *i_data, void *p_del) {
    key_type key = 0;
    pi_datatype->copy(&key, i_data);
    return pi_pbtree.del (key, p_del);
}
/**
 * print pbtree index information.
 */
void PbtreeIndex::print(void) {
    pi_pbtree.print ();
}
 
