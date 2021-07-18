/**
 * @file memdiff.h
 * @author  Shimin Chen <shimin.chen@gmail.com>
 * @version 1.0
 *
 * @section LICENSE
 * 
 * TBD
 *       
 * @section DESCRIPTION
 *                      
 * This file defines macros to prefetch btree nodes
 */  

#ifndef _PBTREE_NODEPREF_H
#define _PBTREE_NODEPREF_H
/* ---------------------------------------------------------------------- */
/**--X86_64 prefetch file--*/
#include "gcc_pf_p3.h"

#define ITEM_SIZE     ( 8)
#define NODE_LINE_NUM ( 4)
#define L3_CACHE_LINE (64)
// node size
#define CACHE_LINE_SIZE         L3_CACHE_LINE
#define BNODE_SIZE		(CACHE_LINE_SIZE * NODE_LINE_NUM)
#define AREA_LINE_NUM   ((NODE_LINE_NUM+1)/2)

static void inline NODE_PREF(register void *bbp)
{
     pfld (* ((char *)bbp));
#    if NODE_LINE_NUM >= 2
     pfld (* ((char *)bbp + CACHE_LINE_SIZE));
#    endif
#    if NODE_LINE_NUM >= 3
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*2));
#    endif
#    if NODE_LINE_NUM >= 4
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*3));
#    endif
#    if NODE_LINE_NUM >= 5
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*4));
#    endif
#    if NODE_LINE_NUM >= 6
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*5));
#    endif
#    if NODE_LINE_NUM >= 7
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*6));
#    endif
#    if NODE_LINE_NUM >= 8
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*7));
#    endif
#    if NODE_LINE_NUM >= 9
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*8));
#    endif
#    if NODE_LINE_NUM >= 10
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*9));
#    endif
#    if NODE_LINE_NUM >= 11
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*10));
#    endif
#    if NODE_LINE_NUM >= 12
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*11));
#    endif
#    if NODE_LINE_NUM >= 13
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*12));
#    endif
#    if NODE_LINE_NUM >= 14
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*13));
#    endif
#    if NODE_LINE_NUM >= 15
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*14));
#    endif
#    if NODE_LINE_NUM >= 16
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*15));
#    endif
#    if NODE_LINE_NUM >= 17
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*16));
#    endif
#    if NODE_LINE_NUM >= 18
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*17));
#    endif
#    if NODE_LINE_NUM >= 19
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*18));
#    endif
#    if NODE_LINE_NUM >= 20
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*19));
#    endif
#    if NODE_LINE_NUM >= 21
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*20));
#    endif
#    if NODE_LINE_NUM >= 22
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*21));
#    endif
#    if NODE_LINE_NUM >= 23
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*22));
#    endif
#    if NODE_LINE_NUM >= 24
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*23));
#    endif
#    if NODE_LINE_NUM >= 25
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*24));
#    endif
#    if NODE_LINE_NUM >= 26
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*25));
#    endif
#    if NODE_LINE_NUM >= 27
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*26));
#    endif
#    if NODE_LINE_NUM >= 28
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*27));
#    endif
#    if NODE_LINE_NUM >= 29
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*28));
#    endif
#    if NODE_LINE_NUM >= 30
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*29));
#    endif
#    if NODE_LINE_NUM >= 31
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*30));
#    endif
#    if NODE_LINE_NUM >= 32
     pfld (* ((char *)bbp + CACHE_LINE_SIZE*31));
#    endif
#    if NODE_LINE_NUM >= 33
#    error "NODE_LINE_NUM must be <= 32!"
#    endif
}

static void inline NODE_PREF_ST(register void *bbp)
{
     pfst (* ((char *)bbp));
#    if NODE_LINE_NUM >= 2
     pfst (* ((char *)bbp + CACHE_LINE_SIZE));
#    endif
#    if NODE_LINE_NUM >= 3
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*2));
#    endif
#    if NODE_LINE_NUM >= 4
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*3));
#    endif
#    if NODE_LINE_NUM >= 5
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*4));
#    endif
#    if NODE_LINE_NUM >= 6
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*5));
#    endif
#    if NODE_LINE_NUM >= 7
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*6));
#    endif
#    if NODE_LINE_NUM >= 8
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*7));
#    endif
#    if NODE_LINE_NUM >= 9
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*8));
#    endif
#    if NODE_LINE_NUM >= 10
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*9));
#    endif
#    if NODE_LINE_NUM >= 11
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*10));
#    endif
#    if NODE_LINE_NUM >= 12
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*11));
#    endif
#    if NODE_LINE_NUM >= 13
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*12));
#    endif
#    if NODE_LINE_NUM >= 14
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*13));
#    endif
#    if NODE_LINE_NUM >= 15
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*14));
#    endif
#    if NODE_LINE_NUM >= 16
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*15));
#    endif
#    if NODE_LINE_NUM >= 17
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*16));
#    endif
#    if NODE_LINE_NUM >= 18
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*17));
#    endif
#    if NODE_LINE_NUM >= 19
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*18));
#    endif
#    if NODE_LINE_NUM >= 20
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*19));
#    endif
#    if NODE_LINE_NUM >= 21
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*20));
#    endif
#    if NODE_LINE_NUM >= 22
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*21));
#    endif
#    if NODE_LINE_NUM >= 23
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*22));
#    endif
#    if NODE_LINE_NUM >= 24
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*23));
#    endif
#    if NODE_LINE_NUM >= 25
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*24));
#    endif
#    if NODE_LINE_NUM >= 26
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*25));
#    endif
#    if NODE_LINE_NUM >= 27
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*26));
#    endif
#    if NODE_LINE_NUM >= 28
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*27));
#    endif
#    if NODE_LINE_NUM >= 29
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*28));
#    endif
#    if NODE_LINE_NUM >= 30
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*29));
#    endif
#    if NODE_LINE_NUM >= 31
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*30));
#    endif
#    if NODE_LINE_NUM >= 32
     pfst (* ((char *)bbp + CACHE_LINE_SIZE*31));
#    endif
#    if NODE_LINE_NUM >= 33
#    error "NODE_LINE_NUM must be <= 32!"
#    endif
}

#define LEAF_PREF           NODE_PREF
#define LEAF_PREF_ST        NODE_PREF_ST

// prefetch AREA_LINE_NUM cache lines starting from &(area[i])
static void inline AREA_PREF(void * area[], int i, int total)
{   
   if ((i) + AREA_LINE_NUM*CACHE_LINE_SIZE/ITEM_SIZE <= (total)) {
      register char *p = (char *)(&(area[i]));
      pfst (* p);
#     if AREA_LINE_NUM >= 2                             
      pfst (* (p + CACHE_LINE_SIZE));
#     endif
#     if AREA_LINE_NUM >= 3                             
      pfst (* (p + CACHE_LINE_SIZE*2));
#     endif
#     if AREA_LINE_NUM >= 4                             
      pfst (* (p + CACHE_LINE_SIZE*3));
#     endif
#     if AREA_LINE_NUM >= 5                             
      pfst (* (p + CACHE_LINE_SIZE*4));
#     endif
#     if AREA_LINE_NUM >= 6                             
      pfst (* (p + CACHE_LINE_SIZE*5));
#     endif
#     if AREA_LINE_NUM >= 7                             
      pfst (* (p + CACHE_LINE_SIZE*6));
#     endif                 
#     if AREA_LINE_NUM >= 8                             
      pfst (* (p + CACHE_LINE_SIZE*7));
#     endif                 
#     if AREA_LINE_NUM >= 9                             
      pfst (* (p + CACHE_LINE_SIZE*8));
#     endif                 
#     if AREA_LINE_NUM >= 10                    
      pfst (* (p + CACHE_LINE_SIZE*9));
#     endif                 
#     if AREA_LINE_NUM >= 11
      pfst (* (p + CACHE_LINE_SIZE*10));
#     endif                 
#     if AREA_LINE_NUM >= 12
      pfst (* (p + CACHE_LINE_SIZE*11));
#     endif                 
#     if AREA_LINE_NUM >= 13
      pfst (* (p + CACHE_LINE_SIZE*12));
#     endif                 
#     if AREA_LINE_NUM >= 14
      pfst (* (p + CACHE_LINE_SIZE*13));
#     endif                 
#     if AREA_LINE_NUM >= 15
      pfst (* (p + CACHE_LINE_SIZE*14));
#     endif                 
#     if AREA_LINE_NUM >= 16
      pfst (* (p + CACHE_LINE_SIZE*15));
#     endif                 
#     if AREA_LINE_NUM >= 17
#     error "AREA_LINE_NUM must be <= 16!"
#     endif
    }                                            
}

/* ---------------------------------------------------------------------- */
#endif /* _PBTREE_NODEPREF_H */
