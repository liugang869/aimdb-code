/**
 * @file pbtree.h
 * @author  Shimin Chen <shimin.chen@gmail.com>
 * @version 1.0
 *
 * @section LICENSE
 *
 * TBD
 *
 * @section DESCRIPTION
 *
 * The pbtree class implements prefetching B+-tree (without jump pointer
 * arrays).  with NO_PREFETCH, this becomes an B+-tree implementation.
 */

/* B+ tree:

   non_leaf nodes:  
    - structure:
       k[0] k[1] k[2] ... k[N-1]
        ch[0] ch[1] ch[2] ... ch[N-1]

    - property
       k[0] is the actual number of keys in the node
       k[1] < k[2] < ... < k[N-1]
       ch[] are pointers to subtrees.

       can hold at most (N-1) keys and N pointers.

    - implementation specific:
      assert(k[0]>=1)
 
   leaf nodes: 
    - structure:
        k[0]    k[1]  k[2] ... k[N-1]
        ch[0]   ch[1] ch[2]... ch[N-1]

    - property:
        (k[i], ch[i]) 1<=i<=N-1, are index entries
        k[0] is the actual number of entries in the node.
        ch[0] is leaf sibling pointer.

	can hold at most (N-1) index entries.

    - implementation specific:
      assert(k[0]>=1)
 */

#ifndef _PBTREE_PBTREE_H
#define _PBTREE_PBTREE_H

#include <assert.h>
#include <stdlib.h>
#include "mymemory.h"

#include "nodepref.h"
/* ---------------------------------------------------------------------- */
// #include "tree.h" based on x86_64 prefetch
//---------from tree.h---------------------------------------------------
#include <stdio.h>

   typedef long long key_type;
   #define KEY_SIZE             8
   #define POINTER_SIZE         8
   #define POINTER8B_SIZE       8
   #define MAX_KEY		((key_type)(0x7fffffffffffffff))
   #define MIN_KEY		((key_type)(0x8000000000000000))

/* ---------------------------------------------------------------------- */

// BKEY_NUM = N-1
#define BKEY_NUM		(BNODE_SIZE/(KEY_SIZE+POINTER8B_SIZE) - 1)
#define NON_LEAF_KEY_NUM        BKEY_NUM
#define LEAF_KEY_NUM            BKEY_NUM

/* ---------------------------------------------------------------------- */
class Pointer8B {
 public:
   // unsigned int num;
   unsigned long long value;

 public:
   Pointer8B & operator= (const void * ptr)
   {
       value= (unsigned long long)ptr;
       return *this;
   }

   Pointer8B & operator= (const Pointer8B & p)
   {
       value= p.value;
       return *this;
   }

   operator void*() { return (void *)value; }
   operator char*() { return (char *)value; }
   operator struct bnode *() { return (struct bnode *)value;}
   operator struct bleaf *() { return (struct bleaf *)value;}
   operator unsigned long long () { return value;}

   void print(void) {
       if (value & (1UL<<63)) {
           void *p = (void*)(value & ((1UL<<63)-1));
           int num = *(int*) p;
           void** vptrs = (void**)((char*)p+2*sizeof(int));
           printf ("[");
           for (int ii=0; ii< num-1; ii++)
               printf("%p,", vptrs[ii]);
           printf ("%p]\n", vptrs[num-1]);
       }
       else {
           printf("[%p]\n", (void*)value);
       }
   }

}; // Pointer8B

#ifdef UNIFIED_IDX_ENTRY
typedef struct IdxEntry{
  key_type   k;
  Pointer8B  ch;
} IdxEntry;

class bnode {
 public:
  IdxEntry   ent[BKEY_NUM + 1];
 public:
  key_type  & k(int idx)  { return ent[idx].k; }
  Pointer8B & ch(int idx) { return ent[idx].ch; }
  char * chEndAddr(int idx)
  {return (char *)&(ent[idx].ch)+sizeof(Pointer8B)-1;}
}; // bnode
#else
class bnode {
 public:
  key_type   key[BKEY_NUM + 1];
  Pointer8B  child[BKEY_NUM + 1];
 public:
  key_type  & k(int idx)  { return key[idx]; }
  Pointer8B & ch(int idx) { return child[idx]; }
  char * chEndAddr(int idx)
  {return (char *)&(child[idx])+sizeof(Pointer8B)-1;}
}; // bnode
#endif

#define bleaf  bnode
#define bnum(ptr)	(((bleaf *)(ptr))->k(0))
#define bnext(ptr)	(((bleaf *)(ptr))->ch(0))

/* ---------------------------------------------------------------------- */
class pbtree {
 private: // tree nodes definition

   bnode * get_node ()
   {
    char *p = NULL;
	int64_t sz = g_memory.alloc (p, BNODE_SIZE);
    return sz == BNODE_SIZE? (bnode*)p: NULL;
   }

   void delete_node (void *p)
   {
	g_memory.free((char*)p, BNODE_SIZE);
   }

 public:  // root and level

   bnode *  tree_root;
   int      root_level;		// leaf: level 0, parent of leaf: level 1

 private:
   /* don't need to be prefetched!!! */
   bnode * parray[32];
   int     ppos[32];

 public:

   int init (void);
   int shut (void);
   // given a search key, perform the search operation
   // return the leaf node pointer and the position with leaf node
   void * lookup (key_type key, int *pos);

   void * get_recptr (void *p, int pos)
   {
	if (p == NULL) return NULL;
        return ((bleaf *)p)->ch(pos);
   }

   key_type get_key (void *p, int pos) 
   {
    return ((bleaf *)p)->k(pos);
   }

   void** get_recptr_sp (void *p, int pos)
   {
	if (p == NULL) return NULL;
        return (void**)&(((bleaf *)p)->ch(pos));
   }

   // insert (key, ptr)
   void insert (key_type key, void *ptr);

   // delete key
   void del (key_type key);

   // range scan
   // Input:  *p is the starting leaf node
   //         *spos is the starting position
   //         endkey is the end key
   //         *num is the buffer size of area[]
   // Operation: store record pointers into area[]
   // Output:
   //    return 0: endkey is not reached
   //              should call again with (*p, *spos)
   //              *num is the number of pointers retrieved into area[]
   //    return non-0: endkey is reached
   //              *num is the number of pointers retrieved into area[]
   int scan (void **p, int *spos, key_type startkey, key_type endkey, 
             void * area[], int *num);

 private:
   void print (bnode *p, int level);
   void check (bnode *p, int level, key_type *start, key_type *end, void **ptr);

 public:
   void print ()
   {
        printf("Printing pbtree\n");
	    print (tree_root, root_level);
   }

   void check (key_type *start, key_type *end)
   {
	assert (sizeof(bnode)==BNODE_SIZE);
	assert (sizeof(bleaf)==BNODE_SIZE);
	void * ptr = NULL;
	check (tree_root, root_level, start, end, &ptr);
   }

   int level () {return root_level;}

 public:
   void save (char *filename)
   {
      FILE *fp = fopen (filename, "w");
      if (fp == NULL) {
        perror (filename); exit (1);
      } 
      fprintf (fp, "tree_root=%p\n", tree_root);
      fprintf (fp, "root_level=%d\n\n", root_level);
      fclose (fp);
   }

   void load (char *filename)
   {
      FILE *fp = fopen (filename, "r");
      if (fp == NULL) {
        perror (filename); exit (1);
      }
      if ((fscanf (fp, "tree_root=%p\n", &tree_root) != 1) ||
          (fscanf (fp, "root_level=%d\n", &root_level) != 1)) {

        fprintf (stderr, "%s format error!\n", filename);
        exit (1);
      }
      fclose (fp);
   }

}; // pbtree

/* ----------------------------------------------------------------------- */
class Pbtree {   // support duplicated elements
  private:
    pbtree p_pbtree;
    char*  p_free_header[16]; // minimum size is 32 Byte

  public:
    void  init   (void);
    bool  insert (key_type key, void* ptr);
    bool  del    (key_type key, void* ptr);
    void* lookup (key_type);
    int   get_recptr (void* p, void* match[], int capicity, int &pos);
   
    void* lookup_s (key_type key, int *pos);
    int   scan (void **p, int *spos, key_type startkey, key_type endkey, 
                void * area[], int *num);

    void  shut   (void);
    void  print  (void);

  public:
    bool  allocate (char *&p,int leve) {
        if (p_free_header[leve]) {
            p = p_free_header[leve];
            p_free_header[leve] = *(char**)p_free_header[leve];
            *(int*)(p) = 0;
            *(int*)(p+sizeof(int)) = leve2cap (leve);
            return true;
        }
        else {
            int64_t size = leve2size (leve);
            int64_t sz = g_memory.alloc (p, size);
            if (sz != size) {
                printf ("[Pbtree][ERROR][allocate]: g_memory is not enough! 296\n");
                return false;
            }
            *(int*)(p) = 0;
            *(int*)(p+sizeof(int)) = leve2cap (leve);
            return true;
        }
        return false;
    }
    bool  free (char *p, int leve) {
        *(char**)(p) = p_free_header[leve];
        p_free_header[leve] = p;
        return true;
    }
    int cap2leve(int cap) {
        int t = ((cap+1)>>2);
        int leve = 0;
        while (t>1) {
            leve ++;
            t = t>>1;
        }
        return leve;
    }
    int leve2cap(int leve) {
        return (1<<(leve+2))-1;
    }
    int leve2size (int leve) {
        return 1<<(leve+5);
    }
    int size2leve (int size) {
        int leve = 0;
        int sz = size;
        while (sz > (1<<5)) {
            leve ++;
            sz = sz >> 1;
        }
        return leve;
    }
};

/* ---------------------------------------------------------------------- */
#endif /* _PBTREE_H */
