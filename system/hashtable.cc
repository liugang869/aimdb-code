#include "hashtable.h"

#define ESTIMATE_ERROR (1024)
HashTable::HashTable(int estimatedNumDistinctKeys,
                     double estimatedDupPerKey, int num_partitions = 0)
{
    estimated_num_distinct_keys = estimatedNumDistinctKeys;
    estimated_duplicates_per_key = estimatedDupPerKey;
    table_size = estimatedNumDistinctKeys;

    int num_in_array;
    num_in_array =
        (int) (estimated_num_distinct_keys * estimated_duplicates_per_key);
    num_in_array += ESTIMATE_ERROR;
    int size = table_size * sizeof(HashCell)
        + num_in_array * sizeof(Hashcode_Ptr);
    if ((begin = (char *) allocate(size)) == NULL) {
        printf("error: allocate!\n");
        exit(1);
    }
    // printf("hashTable size: %d\n", size);
    table = (HashCell *) begin;

#ifndef HASHTABLE_CLASS_PREFETCH_BUCKET_HEADER
    for (int ii = 0; ii < table_size; ii++)
        table[ii].hc_num = 0;

#else
    // We prefetch for sizeof(HashCell)*L2_CACHE_LINE every time.
    // There are sizeof(HashCell) cache lines and L2_CACHE_LINE
    // hash cells.
    int jj;
    char *pp;
    UNROLL_PREF(pfld, table, L2_CACHE_LINE, sizeof(HashCell));
    pp = ((char *) table) + L2_CACHE_LINE * sizeof(HashCell);
    pfld(pp[-1]);
    for (jj = L2_CACHE_LINE; jj < table_size; jj += L2_CACHE_LINE) {
        UNROLL_PREF(pfld, pp, L2_CACHE_LINE, sizeof(HashCell));
        pp += L2_CACHE_LINE * sizeof(HashCell);
        pfld(pp[-1]);
        for (int ii = jj - L2_CACHE_LINE; ii < jj; ii++) {
            table[ii].hc_num = 0;
    }} for (int ii = jj - L2_CACHE_LINE; ii < table_size; ii++) {
        table[ii].hc_num = 0;
    }
#endif   /* HASHTABLE_CLASS_PREFETCH_BUCKET_HEADER */
    avail = (Hashcode_Ptr *) (begin + table_size * sizeof(HashCell));
    end = (Hashcode_Ptr *) (begin + size);
    for (int ii = 0; ii < 16; ii++)
        free_header[ii] = NULL;
    initial_array_size = (int) (estimated_duplicates_per_key + 0.5);
    if (initial_array_size < 2)
        initial_array_size = 2;
    more_allocated = 0;
}

HashTable::~HashTable()
{
    for (auto it = pointer2size.begin(); it != pointer2size.end(); it++) {
        g_memory.free ((char*)it->first,it->second);
    }
}

// array_size = 2^k * initial_array_size
// return k;
int HashTable::size_to_slot(int array_size)
{
    int ret = 0;
    for (int shift = 16; shift >= 1; shift >>= 1) {
        int temp = (array_size >> shift);
        if (temp >= initial_array_size) {
            array_size = temp;
            ret += shift;
        }
    }
    return ret;
}

bool HashTable::add(int64_t hashCode, char *tup)
{
    int which = hashCode % table_size;
    HashCell *hcp = &table[which];
    Hashcode_Ptr *pp;
    switch (hcp->hc_num) {
    case 0:
        hcp->hc_ent.hash_code = hashCode;
        hcp->hc_ent.tuple = tup;
        break;
    case 1:

        // allocate a 2-entry array
        if ((pp = free_header[0])) {
            free_header[0] = (Hashcode_Ptr *) (pp->tuple);
        }
        else {
            pp = avail;
            if (pp + initial_array_size > end) {
                pp = (Hashcode_Ptr *)
                    allocate(sizeof(Hashcode_Ptr) * initial_array_size);
                if (pp == NULL) {
                    fprintf(stderr, "HashTable: more memory is needed!\n");
                    return false;
                }
                more_allocated += initial_array_size;
            }

            else
                avail += initial_array_size;
        }
        pp[0] = hcp->hc_ent;
        pp[1].hash_code = hashCode;
        pp[1].tuple = tup;
        hcp->hc_capacity = initial_array_size;
        hcp->hc_ents = pp;
        break;
    default:
        pp = hcp->hc_ents;
        if (hcp->hc_num == hcp->hc_capacity) {

            // allocate an array of size hcp->hc_capacity * 2
            int allocslot = size_to_slot(hcp->hc_capacity) + 1;
            int allocsize = (hcp->hc_capacity) << 1;
            Hashcode_Ptr *qq;
            if ((qq = free_header[allocslot])) {
                free_header[allocslot] = (Hashcode_Ptr *) (qq->tuple);
            }

            else {
                qq = avail;
                if (qq + allocsize > end) {
                    qq = (Hashcode_Ptr *)
                        allocate(sizeof(Hashcode_Ptr) * allocsize);
                    if (qq == NULL) {
                        fprintf(stderr,
                                "HashTable: more memory is needed!\n");
                        return false;
                    }
                    more_allocated += allocsize;
                }

                else
                    avail += allocsize;
            }
            for (int jj = hcp->hc_capacity - 1; jj >= 0; jj--)
                qq[jj] = pp[jj];
            hcp->hc_ents = qq;
            hcp->hc_capacity = allocsize;
            pp->tuple = (char *) (free_header[allocslot - 1]);
            free_header[allocslot - 1] = pp;
            pp = qq;
        }
        pp += hcp->hc_num;
        pp->hash_code = hashCode;
        pp->tuple = tup;
    } 
    hcp->hc_num++;
    return true;
}

bool HashTable::del(int64_t hashCode, char *tup) {
    // del function, added by liugang
    int which = hashCode % table_size;
    HashCell *hcp = &table[which];
    Hashcode_Ptr *pp;
    switch (hcp->hc_num) {
    case 0:
        return false;
    case 1:
        if (hcp->hc_ent.hash_code == hashCode && hcp->hc_ent.tuple == tup) {
            hcp->hc_num = 0;
            return true;
        }
        return false;
    case 2: {
            pp = hcp->hc_ents;
            int pos = 0;
            while (pos < hcp->hc_num) {
                if (pp[pos].hash_code == hashCode && pp[pos].tuple == tup)
                    break;
                else pos ++;
            }
            if (pos >= hcp->hc_num)
                return false;
            Hashcode_Ptr qq = (pos==0 ? pp[1] : pp[0]);
            int allocslot = size_to_slot(hcp->hc_capacity);
            pp->tuple = (char*)free_header[allocslot];
            free_header[allocslot] = pp;
            hcp->hc_ent = qq;
            hcp->hc_num --;
            return true;
        }
    default: {
            pp = hcp->hc_ents;
            int pos = 0;
            while (pos < hcp->hc_num) {
                if (pp[pos].hash_code == hashCode && pp[pos].tuple == tup)
                    break;
                else pos ++;
            }
            if (pos >= hcp->hc_num)
                return false;
            for (int ii = pos; ii< hcp->hc_num-1; ii++)
                pp[ii] = pp[ii+1];
            hcp->hc_num --;
            return true;
        }
    } 
    return false;
}

// assume capacity > 1
// return:
//      >= 0, the number of matches found
//      <0, found capacity matches and there are more matches
//          use -ret as "last" to call the probe_contd
int HashTable::probe(int64_t hashCode, char *match[], int capacity)
{
    int which = hashCode % table_size;
    HashCell *hcp = &table[which];
    Hashcode_Ptr *pp;
    switch (hcp->hc_num) {
    case 0:
        return 0;
    case 1:
        if (hashCode == hcp->hc_ent.hash_code) {
            match[0] = hcp->hc_ent.tuple;
            return 1;
        }

        else
            return 0;
    default:
        pp = hcp->hc_ents;
        int jj = 0;
        for (int ii = 0; ii < hcp->hc_num; ii++, pp++)
            if (pp->hash_code == hashCode) {
                match[jj++] = pp->tuple;
                if (jj == capacity) {
                    while (1) {
                        ii++;
                        pp++;
                        if (ii >= hcp->hc_num)
                            break;
                        if (pp->hash_code == hashCode)
                            return -ii;
                    }
                }
            }
        return jj;
    }
}


// return: same as probe
int HashTable::probe_contd(int64_t hashCode, int last, char *match[],
                           int capacity)
{
    int which = hashCode % table_size;
    HashCell *hcp = &table[which];
    if (hcp->hc_num > last) {
        Hashcode_Ptr *pp = &(hcp->hc_ents[last]);
        int jj = 0;
        for (int ii = last; ii < hcp->hc_num; ii++, pp++)
            if (pp->hash_code == hashCode) {
                match[jj++] = pp->tuple;
                if (jj == capacity) {
                    while (1) {
                        ii++;
                        pp++;
                        if (ii >= hcp->hc_num)
                            break;
                        if (pp->hash_code == hashCode)
                            return -ii;
                    }
                }
            }
        return jj;
    }
    return 0;
}

void* HashTable::allocate(int size)
{
    int allocate_size = 1;
    while(allocate_size < size)
        allocate_size = allocate_size << 1;
    if(allocate_size <= 0) {
        printf("error: too large memory!\n");
        return NULL;
    }
    char *p = NULL;
    if(g_memory.alloc(p, allocate_size) != allocate_size) {
        printf("error: db system memory error!\n");
        return NULL;
    }
    pointer2size.insert(std::pair<void*,int>(p,allocate_size)); 
    return p;
}

void  HashTable::free(void *mem) 
{
    auto it = pointer2size.find(mem);
    if(it == pointer2size.end())
        return ;
    g_memory.free((char*)it->first,it->second);
}

void HashTable::utilization()
{
    int count = 0;
    for (int ii = 0; ii < table_size; ii++) {
        HashCell *hcp = &table[ii];
        if (hcp->hc_num == 0)
            count++;
    }
    printf("%d out of %d are empty!\n", count, table_size);
    printf("allocated %ld bytes more memory!\n",
           more_allocated * sizeof(Hashcode_Ptr));
}

void HashTable::show()
{
    printf("tablesize: %d\n", table_size);
    for (int ii = 0; ii < table_size; ii++) {
        HashCell *hcp = &table[ii];
        if (hcp->hc_num != 0) {
            if (hcp->hc_num == 1) {
                printf("cell[%d]: num(%d) (%ld, %p)\n", ii, hcp->hc_num,
                       hcp->hc_ent.hash_code, hcp->hc_ent.tuple);
            }

            else {
                printf("cell[%d]: num(%d) capacity(%d) at %p\n", ii,
                       hcp->hc_num, hcp->hc_capacity, hcp->hc_ents);
                Hashcode_Ptr *pp = hcp->hc_ents;
                printf("\t");
                for (int jj = 0; jj < hcp->hc_num; jj++, pp++) {
                    printf("(%ld, %p) ", pp->hash_code, pp->tuple);
                } printf("\n");
        }}
    } printf("free list:\n");
    for (int ii = 0; ii < 16; ii++)
        if (free_header[ii]) {
            Hashcode_Ptr *pp;
            printf("[%d]: ", ii);
            pp = free_header[ii];

            do {
                printf("%p->", pp);
                pp = (Hashcode_Ptr *) (pp->tuple);
            } while (pp != NULL);
            printf("/\n");
        }
    printf("avail pool size: %ld Hashcode_Ptr\n", end - avail);
}

