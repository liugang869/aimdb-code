/**
 * @file    hashindex.cc
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

#include "hashindex.h"

bool HashIndex::init(void)
{
    ih_column_num = 0L;
    ih_column_cap = getIKey().getKey().size();
    ih_datatype = new BasicType *[ih_column_cap];
    ih_table_offset = new int64_t[ih_column_cap];
    ih_hash_bits = new int64_t[ih_column_cap];
    return true;
}

bool HashIndex::addIndexDTpye(BasicType * i_dt,int64_t offset)
{
    if (ih_column_num >= ih_column_cap) {
        printf("[HashIndex][ERROR][addIndexDTpye]: exceed column number! -1\n");
        return false;
    }
    ih_datatype[ih_column_num] = i_dt;
    ih_table_offset[ih_column_num] = offset;
    ih_column_num ++;
    return true;
}

bool HashIndex::finish(void)
{                               // for hash setting
    int64_t average = ih_cell_capbits / ih_column_cap;
    average = (average == 0 ? 1 : average);
    int64_t leftover = ih_cell_capbits;
    for (int64_t ii = 0; ii < ih_column_cap; ii++) {
        int64_t bits = ih_datatype[ii]->getTypeSize() << 3;     // *8bits
        int64_t actu = bits <= average ? bits : average;
        actu = actu <= leftover ? actu : leftover;
        ih_hash_bits[ii] = actu;
        leftover -= actu;
    }
    ih_hashtable = new HashTable(1<<ih_cell_capbits,1.1,0);
    return true;
}

bool HashIndex::shut(void)
{
    delete [] ih_datatype;
    delete [] ih_hash_bits;
    delete    ih_hashtable;
    delete [] ih_table_offset;
    return true;
}

bool HashIndex::insert(void *i_data, void *p_in)
{
    // printf ("hashindex insert void *i_data\n");
    int64_t key = tranToInt64(i_data);
    return ih_hashtable->add(key,(char*)p_in);
}

bool HashIndex::insert(void *i_data[], void *p_in)
{
    // debug --------------------------------
//    char buffer[1024];
//    for (int ii = 0; ii< i_key.getKey().size(); ii++) {
//        ih_datatype[ii]->formatTxt (buffer,i_data[ii]);
//        printf ("%s\t", buffer);
//    } 
//    printf ("\n");
    // --------------------------------------
    int64_t key = tranToInt64(i_data);
//    printf ("%ld\n", key);
    return ih_hashtable->add(key,(char*)p_in);
}

bool HashIndex::del(void *i_data)
{
    HashInfo info;
    bool opok = set_ls(i_data, NULL, &info);
    if (opok == false) {
        printf("[HashIndex][ERROR][del]: set_ls error! -1\n");
        return false;
    }
    void *result = NULL;
    opok = lookup(i_data, &info, result);
    if (opok == false) {
        printf("[HashIndex][INFO][del]: not found error! -2\n");
        return false;
    }
    return ih_hashtable->del(info.hash, (char*)result);
}

bool HashIndex::del(void *i_data[])
{
    HashInfo info;
    bool opok = set_ls(i_data, NULL, &info);
    if (opok == false) {
        printf("[HashIndex][ERROR][del]: set_ls error! -1\n");
        return false;
    }
    void *result = NULL;
    opok = lookup(i_data, &info, result);
    if (opok == false) {
        printf("[HashIndex][INFO][del]: not found error! -2\n");
        return false;
    }
    return ih_hashtable->del(info.hash, (char*)result);
}

// the following function can pull one by one 
// info is pointed to HashInfo, you should init the value by setting ppos = HASHINFO_CAPICITY
// when you first call it
// i_data2 leave it NULL, for uniform interface of lookup & scan 
bool HashIndex::set_ls(void *i_data1, void *i_data2, void *info)
{
    HashInfo *hi = (HashInfo *) info;
    hi->ppos = 0;
    hi->hash = tranToInt64(i_data1);
    hi->last = ih_hashtable->probe(hi->hash,(char**)hi->result,HASHINFO_CAPICITY);
    hi->rnum = hi->last >= 0 ? hi->last : HASHINFO_CAPICITY;
    return  true;
}

bool HashIndex::set_ls(void *i_data1[], void *i_data2[], void *info)
{
    HashInfo *hi = (HashInfo *) info;
    hi->ppos = 0;
    hi->hash = tranToInt64(i_data1);
    hi->last = ih_hashtable->probe(hi->hash,(char**)hi->result,HASHINFO_CAPICITY);
    hi->rnum = hi->last >= 0 ? hi->last : HASHINFO_CAPICITY;
    return  true;
}

bool HashIndex::lookup(void *i_data, void *info, void *&result)
{
    HashInfo *hi = (HashInfo *) info;
    while (1) {
        while (hi->ppos < hi->rnum) {
            result = hi->result[hi->ppos];
            if (cmpEQ(i_data, result)) {
                hi->ppos++;
                return true;
            } else
                hi->ppos++;
        }
        if (hi->last >= 0)
            return false;
        hi->last =
            ih_hashtable->probe_contd(hi->hash, -hi->last,(char**)hi->result, HASHINFO_CAPICITY);
        if (hi->last == 0)
            return false;
        hi->rnum = hi->last >= 0 ? hi->last : HASHINFO_CAPICITY;
        hi->ppos = 0L;
    }
    return false;
}

bool HashIndex::lookup(void *i_data[], void *info, void *&result)
{
    HashInfo *hi = (HashInfo *) info;
    while (1) {
        while (hi->ppos < hi->rnum) {
            result = hi->result[hi->ppos];
            if (cmpEQ(i_data, result)) {
                hi->ppos++;
                return true;
            } else
                hi->ppos++;
        }
        if (hi->last >= 0)
            return false;
        hi->last =
            ih_hashtable->probe_contd(hi->hash,-hi->last, (char**)hi->result, HASHINFO_CAPICITY);
        if (hi->last == 0)
            return false;
        hi->rnum = hi->last >= 0 ? hi->last : HASHINFO_CAPICITY;
        hi->ppos = 0L;
    }
    return false;
}

bool HashIndex::cmpEQ(void *i_data[], void *result)
{
    for (int64_t ii = 0, pos = 0; ii < ih_column_cap; ii++) {
        if (ih_datatype[ii]->cmpEQ(i_data[ii], ((char *) result) + ih_table_offset[ii]) == false)
            return false;
        pos += ih_datatype[ii]->getTypeSize();
    }
    return true;
}

bool HashIndex::cmpEQ(void *i_data, void *result)
{
    for (int64_t ii = 0, pos = 0; ii < ih_column_cap; ii++) {
        if (ih_datatype[ii]->cmpEQ (((char *) i_data) + pos, ((char *) result) + ih_table_offset[ii]) == false)
            return false;
        pos += ih_datatype[ii]->getTypeSize();
    }
    return true;
}

int64_t HashIndex::tranToInt64(void *i_data)
{
    int64_t offset = 0;
    int64_t result = 0;
    for (int64_t ii = 0; ii < ih_column_cap; ii++) {
        int64_t tmpvar = 0L;
        TypeCode tc = ih_datatype[ii]->getTypeCode();
        int64_t sz = ih_datatype[ii]->getTypeSize();
        char *p = ((char *) i_data) + offset;
        if (tc == CHARN_TC) {
            tmpvar = hash(p, sz);
            tmpvar = tmpvar & ((1L << ih_hash_bits[ii]) - 1);
        } else {
            // only for int not float, float is rarely used in id
            switch (sz) {
            case 1:
                tmpvar = (*(int8_t *) p) & ((1L << ih_hash_bits[ii]) - 1);
                break;
            case 2:
                tmpvar = (*(int16_t *) p) & ((1L << ih_hash_bits[ii]) - 1);
                break;
            case 4:
                tmpvar = (*(int32_t *) p) & ((1L << ih_hash_bits[ii]) - 1);
                break;
            case 8:
                tmpvar = (*(int64_t *) p) & ((1L << ih_hash_bits[ii]) - 1);
                break;
            }
        }
        result = result | (tmpvar << offset);
        offset += ih_hash_bits[ii];
    }
    return result;
}

int64_t HashIndex::tranToInt64(void *i_data[])
{
    int64_t offset = 0;
    int64_t result = 0;
    for (int64_t ii = 0; ii < ih_column_cap; ii++) {
        int64_t tmpvar = 0L;
        TypeCode tc = ih_datatype[ii]->getTypeCode();
        int64_t sz = ih_datatype[ii]->getTypeSize();
        if (tc == CHARN_TC) {
            tmpvar = hash((char *) i_data[ii], sz);
            tmpvar = tmpvar & ((1L << ih_hash_bits[ii]) - 1);
        } else {
            // only for int not float, float is rarely used in id
            switch (sz) {
            case 1:
                tmpvar =
                    (*(int8_t *) i_data[ii]) & ((1L << ih_hash_bits[ii]) - 1);
                break;
            case 2:
                tmpvar =
                    (*(int16_t *) i_data[ii]) & ((1L << ih_hash_bits[ii]) - 1);
                break;
            case 4:
                tmpvar =
                    (*(int32_t *) i_data[ii]) & ((1L << ih_hash_bits[ii]) -1);
                break;
            case 8:
                tmpvar =
                    (*(int64_t *) i_data[ii]) & ((1L << ih_hash_bits[ii]) -1);
                break;
            }
        }
        result = result | (tmpvar << offset);
        offset += ih_hash_bits[ii];
    }
    return result;
}

void HashIndex::print(void)
{
    // print struct infomation
    printf("HashIndex- id: %ld table: %ld name: %s keys: ", getOid(),getIndexTid(), getOname());
    getIKey().print();
    printf("\n");
}

int64_t HashIndex::hash(char *str, int64_t maxlen)
{
    int64_t hash = 5381L;
    int64_t c, ii = 0L;
    while (ii < maxlen && (c = *(unsigned char *) (str + ii))) {
        hash = ((hash << 5) + hash) + c;        /* hash * 33 + c */
        ii++;
    }
    return hash;
}
