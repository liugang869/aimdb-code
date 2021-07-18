/**     
 * @file pbtree.cc
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

#include "pbtree.h"
/* ----------------------------------------------------------------- *
   init
 * ----------------------------------------------------------------- */
int pbtree::init (void) {
    root_level = 0;
    tree_root = get_node ();
    bnum(tree_root) = 0;
    bnext(tree_root) = NULL;
    return 0;
}
/* ----------------------------------------------------------------- *
   shut
 * ----------------------------------------------------------------- */
int pbtree::shut (void) {
    return 0;
}
/* ----------------------------------------------------------------- *
   look up
 * ----------------------------------------------------------------- */

   /* leaf is level 0, root is level depth-1 */

void * pbtree::lookup (key_type key, int *pos)
{
   register bnode *p;
   register int i,t,m,b;
   register key_type r;

   if (pos == NULL) return NULL;

   p = tree_root;

   for (i=root_level; i>0; i--) {

     NODE_PREF(p);

     b=1; t=p->k(0);
     while (b+7<=t) {
      m=(b+t) >>1;
      r= key - p->k(m);
      if (r>0) b=m+1;
      else if (r<0) t = m-1;
      else {p=p->ch(m); goto inner_done;}
     }

     for (; b<=t; b++)
        if (key < p->k(b)) break;
     p = p->ch(b-1);

 inner_done: ;
   }

     pfst (*pos);
     #if   defined(NO_PREFETCH) && defined(CELLO)
    pref(1, *pos);
     #endif

     LEAF_PREF ((bleaf *)p);

     b=1; t=bnum(p);
     while (b+7<=t) {
      m=(b+t) >>1;
      r= key - ((bleaf *)p)->k(m);
      if (r>0) b=m+1;
      else if (r<0) t = m-1;
      else {*pos=m; goto leaf_done;}
     }

     for (; b<=t; b++)
        if (key < p->k(b)) break;
   *pos = b-1;

 leaf_done:
   return (void *)p;
}

/* ---------------------------------------------------------- *

   insertion: insert (key, ptr) pair into pbtree

 * ---------------------------------------------------------- */

void pbtree::insert (register key_type key, register void *ptr)
{

  if ((ptr == NULL)&&(key == -1)) return;

  /* Part 1. get the positions to insert the key */
  {register bnode *p;
   register int i,t,m,b;
   register key_type r;

   p = tree_root;
   for (i=root_level; i>0; i--) {

     parray[i] = p;

     NODE_PREF(p);

     b=1; t=p->k(0);
     while (b+7<=t) {
      m=(b+t) >>1;
      r= key - p->k(m);
      if (r>0) b=m+1;
      else if (r<0) t = m-1;
      else {p=p->ch(m); ppos[i]=m; goto inner_done;}
     }

     for (; b<=t; b++)
        if (key < p->k(b)) break;
     p = p->ch(b-1);
     ppos[i]= b-1;

 inner_done: ;
   }

     parray[0] = p;
     LEAF_PREF_ST ((bleaf *)p);
     b=1; t=bnum(p);
     while (b+7<=t) {
      m=(b+t) >>1;
      r= key - ((bleaf *)p)->k(m);
      if (r>0) b=m+1;
      else if (r<0) t = m-1;
      else {return;}
     }
      
     for (; b<=t; b++)
        if (key < p->k(b)) break;

     if (key == p->k(b-1)) return; /* found */

     ppos[0] = b-1;

 leaf_done:;
  }

#ifdef INSTRUMENT_INSERTION
  insert_total ++;
#endif // INSTRUMENT_INSERTION

  /* parray[0..level] and ppos[0..level] contains the node, positions
     to insert the (key,ptr)
     parray[0], ppos[0] is the key in front of key at the leaf node
     parray[1..level] and ppos[1..level] are the non-leaf node path from root */

 /* Part 2. leaf node */

 {register bleaf *p, *newp;
  register int    n, i, pos, r;

  p = (bleaf *)parray[0];
  n = bnum(p);
  pos = ppos[0] + 1;

       /* if the leaf is not full, simply put the key into the leaf */
  if (n < LEAF_KEY_NUM) {
    for (i=n; i>=pos; i--)
       {p->k(i+1) = p->k(i); p->ch(i+1) = p->ch(i); }
    p->k(pos) = key; p->ch(pos) = ptr;

    bnum(p) = n+1;

#ifdef INSTRUMENT_INSERTION
    insert_no_split ++;
#endif // INSTRUMENT_INSERTION

    return;
  }

       /* otherwise, we allocate a new leaf node, and redistribute
          the keys in the two nodes */

  newp = (bleaf *)get_node();

#ifdef INSTRUMENT_INSERTION
    total_node_splits ++;
#endif // INSTRUMENT_INSERTION

       /* when new key should be put in the left node */
#define   LEFT_KEY_NUM		((LEAF_KEY_NUM+1)/2)
#define   RIGHT_KEY_NUM		((LEAF_KEY_NUM+1) - LEFT_KEY_NUM)

  if(pos <= LEFT_KEY_NUM) {
    for (r=RIGHT_KEY_NUM, i=LEAF_KEY_NUM; r>=1; r--, i--) {
       newp->k(r) = p->k(i); newp->ch(r) = p->ch(i);
    }
    for (i=LEFT_KEY_NUM-1; i>=pos; i--) {
       p->k(i+1) = p->k(i); p->ch(i+1) = p->ch(i);
    }
     p->k(pos) = key; p->ch(pos) = ptr;
  }
       /* when new key should be put in the right node */
  else {
    for (r=RIGHT_KEY_NUM, i=LEAF_KEY_NUM; i>=pos; r--, i--) {
       newp->k(r) = p->k(i); newp->ch(r) = p->ch(i);
    }
    newp->k(r) = key; newp->ch(r) = ptr; r--;
    for (;r>=1; r--, i--) {
       newp->k(r) = p->k(i); newp->ch(r) = p->ch(i);
    }
  }
       /* set other fields in the leaves */
  bnum(p) = LEFT_KEY_NUM;
  bnum(newp) = RIGHT_KEY_NUM;
  bnext(newp) = bnext(p);
  bnext(p) = newp;

#undef RIGHT_KEY_NUM
#undef LEFT_KEY_NUM

       /* (key, ptr) to be inserted in the parent non-leaf */
  key = newp->k(1);
  ptr = newp;

 }

 /* Part 3. nonleaf node */
 {register bnode *p, *newp;
  register int    n, i, pos, r, lev, total_level;

#define   LEFT_KEY_NUM		((NON_LEAF_KEY_NUM)/2)
#define   RIGHT_KEY_NUM		((NON_LEAF_KEY_NUM) - LEFT_KEY_NUM)

  total_level = root_level;
  lev = 1;

  while (lev <= total_level) {

    p = parray[lev];
    n = p->k(0);
    pos = ppos[lev] + 1;

         /* if the non-leaf is not full, simply insert key ptr */

    if (n < NON_LEAF_KEY_NUM) {
      for (i=n; i>=pos; i--) {
         p->k(i+1) = p->k(i); p->ch(i+1) = p->ch(i);
      }
         /* pos >= 1 !!! */
      p->k(pos) = key; p->ch(pos) = (bnode *)ptr; p->k(0) = n+1;

#ifdef INSTRUMENT_INSERTION
      if (lev == 1) {
        insert_leaf_split ++;
      }
      else {
        insert_nonleaf_split ++;
      }
#endif // INSTRUMENT_INSERTION

      return;
    }

         /* otherwise allocate a new non-leaf and redistribute the keys */
    newp = (bnode *)get_node();

#ifdef INSTRUMENT_INSERTION
    total_node_splits ++;
#endif // INSTRUMENT_INSERTION

         /* if key should be in the left node */
    if (pos <= LEFT_KEY_NUM) {
      for (r=RIGHT_KEY_NUM, i=NON_LEAF_KEY_NUM; r>=0; r--, i--) {
         newp->k(r) = p->k(i); newp->ch(r) = p->ch(i);
      }
      /* newp->key[0] actually is the key to be pushed up !!! */
      for (i=LEFT_KEY_NUM-1; i>=pos; i--) {
	 p->k(i+1) = p->k(i); p->ch(i+1) = p->ch(i);
      }
      p->k(pos) = key; p->ch(pos) = (bnode *)ptr;
    } 
         /* if key should be in the right node */
    else {
      for (r=RIGHT_KEY_NUM, i=NON_LEAF_KEY_NUM; i>=pos; i--, r--){
         newp->k(r) = p->k(i); newp->ch(r) = p->ch(i);
      }
      newp->k(r) = key; newp->ch(r) = (bnode *)ptr; r--;
      for (;r>=0; r--, i--) {
         newp->k(r) = p->k(i); newp->ch(r) = p->ch(i);
      }
    } /* end of else */

    key = newp->k(0); ptr = newp;

    p->k(0) = LEFT_KEY_NUM;
    newp->k(0) = RIGHT_KEY_NUM;

    lev ++;
  } /* end of while loop */

#ifdef INSTRUMENT_INSERTION
  insert_nonleaf_split ++;
  total_node_splits ++;
#endif // INSTRUMENT_INSERTION

  /* root was splitted !! add another level */
  newp = (bnode *)get_node();

  newp->k(0) = 1;
  newp->ch(0) = tree_root; newp->ch(1) = (bnode *)ptr; newp->k(1) = key;

  tree_root = newp; root_level = lev;
  return;

#undef RIGHT_KEY_NUM
#undef LEFT_KEY_NUM
 }
}

/* ---------------------------------------------------------- *

   deletion: delete key from pbtree

   lazy delete - insertions >= deletions in most cases
             so no need to change the tree structure frequently

   So unless there is no key in a leaf or non-leaf, the leaf
   and non-leaf won't be deleted.

 * ---------------------------------------------------------- */
void pbtree::del (key_type key)
{

  /* Part 1. get the positions of the key */
  {register bnode *p;
   register int i,t,m,b;
   register key_type r;

   p = tree_root;
   for (i=root_level; i>0; i--) {
     parray[i] = p;

     NODE_PREF(p);

     b=1; t=p->k(0);
     while (b+7<=t) {
      m=(b+t) >>1;
      r= key - p->k(m);
      if (r>0) b=m+1;
      else if (r<0) t = m-1;
      else {p=p->ch(m); ppos[i]=m; goto inner_done;}
     }

     for (; b<=t; b++)
        if (key < p->k(b)) break;
     p = p->ch(b-1);
     ppos[i]= b-1;

 inner_done: ;
   }

     parray[0] = p;

     LEAF_PREF_ST ((bleaf *)p);

     b=1; t=bnum(p);
     while (b+7<=t) {
      m=(b+t) >>1;
      r= key - ((bleaf *)p)->k(m);
      if (r>0) b=m+1;
      else if (r<0) t = m-1;
      else {ppos[0]=m; goto leaf_done;}
     }

     for (; b<=t; b++)
        if (key < p->k(b)) break;

     if (key != p->k(b-1)) return; /* not found */

     ppos[0] = b-1;

 leaf_done:;
  }

  // my revise: if the only one element in root&leaf, should not free the bnode
  if (parray[0] == tree_root && bnum(tree_root) == 1) {
    bnum(tree_root) = 0;
    bnext(tree_root) = NULL;
    return ;
  }

  /* parray[0..level] and ppos[0..level] contains the node, positions
     of the (key,ptr)
     parray[0..level] and ppos[0..level] are the node path from root */

 /* Part 2. leaf node */
 {register bleaf *p, *sibp;
  register bnode *parp;
  register int    n, i, pos, r;

  p = (bleaf *)parray[0];
  n = bnum(p);
  pos = ppos[0];

       /* the leaf contains more than 1 keys, simply delete key */
  if (n > 1) {
    for (i=pos; i<n; i++) {
       p->k(i) = p->k(i+1); p->ch(i) = p->ch(i+1);
    }
    bnum(p) = n - 1;
    return;
  }

        /* Otherwise, the leaf is empty, get a sibling (there is at least 1) */
  parp = parray[1];
  pos = ppos[1];
 
       /* if it has a left sibling */
   if (pos > 0) {
     sibp = (bleaf *)(parp->ch(pos-1)); LEAF_PREF(sibp);
     n = bnum(sibp);

          /* if the left sibling has some keys, redistribute */
     if (n >= 2) {

       bnum(sibp) = n/2;
       bnum(p) = r = n - n/2;

       for (i=n; r>=1; r--, i--) {
          p->k(r) = sibp->k(i); p->ch(r) = sibp->ch(i);
       }

       parp->k(pos) = p->k(1);
       return;
     }
          /* otherwise delete the node */
     bnext(sibp) = bnext(p);
   }
      /* or it is the first child, so it has right sibling */
   else {

     sibp = (bleaf *)(parp->ch(1));
     LEAF_PREF(sibp);
     n = bnum(sibp);

          /* if the right sibling has some keys, redistribute */
     if (n >= 2) {

       bnum(p) = n/2;
       bnum(sibp) = n - n/2;

       for (i=n/2; i>=1; i--) {
          p->k(i) = sibp->k(i); p->ch(i) = sibp->ch(i);
       }
       for (r=1, i=n/2+1; i<=n; r++, i++) {
          sibp->k(r) = sibp->k(i); sibp->ch(r) = sibp->ch(i);
       }

       parp->k(1) = sibp->k(1);
       return;
     }
          /* otherwise n == 1, move the keys of right sibling to the node
             and delete the right sibling */
       p->k(1) = sibp->k(1); p->ch(1) = sibp->ch(1);
       bnum(p) = 1;
       bnext(p) = bnext(sibp);
       p = sibp;
       ppos[1] = 1;
   }

   delete_node (p);
 }

 /* Part 3: non-leaf node */
 {register bnode *p, *sibp, *parp;
  register int    n, i, pos, r, lev, total_level;

  total_level = root_level;
  lev = 1;

   while (1) {
        p = parray[lev];
        n = p->k(0);
        pos = ppos[lev];   /* pos >= 1*/

               /* if the node has more than 1 keys, simply delete */
        if (n > 1) {
          for (i=pos; i<n; i++) {
             p->k(i) = p->k(i+1); p->ch(i) = p->ch(i+1);
          }
          p->k(0) = n - 1;
          return;
        }

        if (lev >= total_level)
          break;

               /* otherwise only 2 ptr, 1 key */
        parp = parray[lev+1];
        pos = ppos[lev+1];
               /* if it has left sibling */
        if (pos > 0) {
          sibp = parp->ch(pos-1); NODE_PREF(sibp);
          n = sibp->k(0);

                  /* if the left sibling has >= 2 keys, redistribute */
          if (n >= 2) {
            sibp->k(0) = n/2;
            p->k(0) = r = n - n/2;

            p->k(r) = parp->k(pos); p->ch(r) = p->ch(0);

            for (i=n, r--; r>=1; r--, i--) {
               p->k(r) = sibp->k(i); p->ch(r) = sibp->ch(i);
            }
            p->ch(0) = sibp->ch(i);
            parp->k(pos) = sibp->k(i);
            return;
          }
                  /* otherwise only 1 key delete this node */
          sibp->k(2) = parp->k(pos); sibp->ch(2) = p->ch(0);
          sibp->k(0) = 2;
          delete_node (p);
        }
               /* then it has right sibling */
        else {
            sibp = parp->ch(1); NODE_PREF(sibp);
            n = sibp->k(0);

                  /* if the right sibling has >=2 keys, redistribute */
            if (n >= 2) {
              p->k(0) = n/2;
              sibp->k(0) = n - n/2;

			/* pos == 0 */
              p->k(1) = parp->k(1); p->ch(1) = sibp->ch(0);
              for (i=n/2; i>=2; i--) {
                 p->k(i) = sibp->k(i-1); p->ch(i) = sibp->ch(i-1);
              }
              i=n/2;
              sibp->ch(0) = sibp->ch(i); parp->k(1) = sibp->k(i);
              for (i++, r=1; i<=n; i++, r++) {
                 sibp->k(r) = sibp->k(i); sibp->ch(r) = sibp->ch(i);
              }
              return;
            }
                  /* otherwise move keys to this node and delete right sibling*/
            p->k(2) = sibp->k(1); p->ch(2) = sibp->ch(1);
            p->k(1) = parp->k(1); p->ch(1) = sibp->ch(0);
	    p->k(0) = 2;
            delete_node (sibp);
	    ppos[lev+1] = 1;
        }

      lev++;
   } /* end of while */

   tree_root = p->ch(0); root_level = total_level - 1;
   delete_node (p);
   return;
  }
}


/* ----------------------------------------------------------------- *
   scan
 * ----------------------------------------------------------------- */
   /* Assumption:
         (*p)->k(*spos) < endkey
	 *num > 0
    */

   /* scan from (*p)->k(*spos) until >= endkey or *num pointers have
      been retrieved. Put the pointers into area[]. 
      (*p, *spos) is set to the next key position.
		  or *p == NULL if the end of the tree is hit
       *num is the number of pointers retrieved and put into area[].
       return 0: endkey is not reached
          non-0: endkey is reached
    */

int pbtree::scan (void **p, int *spos, key_type startkey, key_type endkey, 
                  void * area[], int *num)
{
 register bleaf *curp;
 register int i, j, n, total;

    total = *num;
    if (total <= 0) return 0;

    i = 0;     /* area index */
    curp = (bleaf *) (*p); /* current bleaf */
    j = *spos; /* start key position in curp */

    while (curp) {
      LEAF_PREF(curp);
      AREA_PREF(area, i, total);
      n = bnum(curp);
      if (i + n > total) {
        if (i+n-j+1 > total)
          goto area_full;
      }

      if (curp->k(n) >= endkey)
        goto hit_endkey;

      for (; j<=n; j++)
         area[i++] = curp->ch(j);

      curp = (bleaf *)bnext(curp); j=1;
    }
                     /* hit end of tree */
    *p = NULL; *num = i; return 1;

area_full:
    n = total-i+j-1;
    if (n>0) {
      if (curp->k(n) >= endkey)
        goto hit_endkey;

      for (; j<=n; j++)
         area[i++] = curp->ch(j);
    }
    *p = curp; *spos = n+1; *num = i;
    return 0;

hit_endkey:
    for (; curp->k(j)<endkey; j++)
       area[i++] = curp->ch(j);
    *p = curp; *spos = j; *num = i; return 1;
}

/* ----------------------------------------------------------------- *
   print
 * ----------------------------------------------------------------- */

void pbtree::print (bnode *p, int level)
{
   int i;
   if (level > 0) {
      print (p->ch(0), level-1);
      for (i=1; i<=p->k(0); i++) {
	     printf ("%*c%lld\n", level*8, '+', (long long)(p->k(i)));
	     print (p->ch(i), level-1);
      }
   }
   else {
      for (i=1; i<=bnum(p); i++) {
	     printf ("%*c%lld-", level*8, '+', (long long)(((bleaf *)p)->k(i)));
         (((bleaf *)p)->ch(i)).print();
      }
      if ((bleaf *)bnext(p) != NULL) {
         printf ("(%lld)-", (long long)(((bleaf *)bnext(p))->k(1)));
         (((bleaf *)p)->ch(1)).print();
      }
      else {
         printf ("(null)\n");
      }
   }
}

/* ----------------------------------------------------------------- *
   check structure integrity
 * ----------------------------------------------------------------- */

/* *ptr is the leaf before this subtree
   return *ptr the last leaf of this subtree
   return *start *end, the first and the last keys of the subtree
 */
void pbtree::check (bnode *p, int level, key_type *start, key_type *end, void **ptr)
{
   if (p == NULL) {
     printf ("level %d: null child pointer\n", level + 1);
     exit (1);
   }

   if (level == 0) {
     bleaf *lp;
     int i;

     lp = (bleaf *) p;

     if (bnum(lp)<=0) {
       printf ("leaf(%lld):negative num\n", (long long)(lp->k(1))); exit (1);
     }
     for (i=1; i<bnum(lp); i++)
        if (lp->k(i)>=lp->k(i+1))
          {printf ("leaf(%lld):key order wrong\n", (long long)(lp->k(1))); 
	   printf ("key %d: %lld, key %d: %lld\n", i, (long long)lp->k(i), i+1, (long long)lp->k(i+1));
	   exit(1);
	   }
     if ((*ptr) && (bnext(*ptr) != lp))
       {printf ("leaf(%lld):next broken from prevnode\n", (long long)(lp->k(1))); exit(1);}
     *start = lp->k(1);
     *end   = lp->k(bnum(lp));
     *ptr   = lp;
   }
   else {
     key_type curstart, curend;
     int i;
     void *curptr;

     if (p->k(0)<0) {
       printf ("nonleaf level %d(%lld): num<0\n", level, (long long)(p->k(1))); exit (1);
       }
     else if (p->k(0) == 0) {
       curptr = *ptr;
       check (p->ch(0), level-1, &curstart, &curend, &curptr);
       *start = curstart;
       *end = curend;
       *ptr = curptr;
       return;
     }

     curptr = *ptr;
     check (p->ch(0), level-1, &curstart, &curend, &curptr);
     *start = curstart;
     if (curend >= p->k(1)) {
       printf ("nonleaf level %d(%lld): key order wrong\n", level, (long long)(p->k(1)));
       exit (1);
     }

     for (i=1; i<p->k(0); i++) {
        check (p->ch(i), level-1, &curstart, &curend, &curptr);
        if (!((p->k(i)<=curstart)&&(curend<p->k(i+1)))) {
          printf ("nonleaf level %d(%lld): key order wrong at %lld\n", 
	  	   level, (long long)(p->k(1)), (long long)(p->k(i)));
          exit (1);
        }
     }
     check (p->ch(i), level-1, &curstart, &curend, &curptr);
     *end = curend;
     if (curstart < p->k(i)) {
       printf ("nonleaf level %d(%lld): key order wrong\n", level, (long long)(p->k(1)));
       exit (1);
     }
     *ptr = curptr;
   }
}

void Pbtree::init (void) {
    p_pbtree.init ();
    for (int ii= 0; ii<16 ;ii++)
        p_free_header[ii] = NULL;
}

bool Pbtree::insert (key_type key, void *ptr) {
    int pos = -1;
    void* p = p_pbtree.lookup  (key, &pos);
    void* r = (key== p_pbtree.get_key(p,pos))?p_pbtree.get_recptr(p, pos):NULL;
    if (r) {// aleardy exist
        if ((unsigned long long)r & (1UL<<63)) {
            r = (void*)((unsigned long long)r & ((1UL<<63)-1));
            int num = *(int*)(r);
            int cap = *(int*)((char*)r+sizeof(int));
            // printf ("num:%d,cap:%d\n", num,cap);
            void **vptrs = (void**)((char*)r+2*sizeof(int));
            for (int ii= 0; ii< num; ii++) {
                if (vptrs[ii] == ptr) {
                    return false;
                }
            }
            if (num < cap) {
                vptrs[num] = ptr;
                num ++;
                *(int*)((char*)r) = num;
            }
            else {
                int leve = cap2leve (cap);
                int n_leve = leve + 1;
                char *n_p = NULL;
                if (allocate(n_p, n_leve) == false) {
                    printf ("[Pbtree][ERROR][insert]: g_memory is not enough! 785\n");
                    return false;
                }
                *(int*)(n_p+sizeof(int)) = leve2cap(n_leve);
                void **n_vptrs = (void**)(n_p+2*sizeof(int));
                for (int ii=0; ii< num; ii++) 
                    n_vptrs[ii] = vptrs[ii];
                n_vptrs[num++] = ptr;
                *(int*)(n_p) = num;
                void** sp = p_pbtree.get_recptr_sp (p, pos);
                *sp = (void*)((1UL<<63)|((unsigned long long)n_p));
                free ((char*)r, leve);
            }
        }
        else {
            if (r == ptr) 
                return false;
            char *n_p = NULL;
            if (allocate (n_p, 0) == false) {
                printf ("[Pbtree][ERROR][insert]: g_memory is not enough! 802\n");
                return false;
            }
            void **vptrs = (void**)(n_p+2*sizeof(int));
            vptrs[0] = r;
            vptrs[1] = ptr;
            *(int*)(n_p) = 2;
            void** sp = p_pbtree.get_recptr_sp (p, pos);
            *sp = (void*)((1UL<<63)|((unsigned long long)n_p));
        }
    }
    else {
        p_pbtree.insert (key, ptr);
    }
    return true;
}

bool Pbtree::del    (key_type key, void *ptr) {
    int pos = -1;
    void* p = p_pbtree.lookup  (key, &pos);
    void* r = (key== p_pbtree.get_key(p,pos))?p_pbtree.get_recptr(p, pos):NULL;
    if (r) {   // aleardy exist
        if ((unsigned long long)r & (1UL<<63)) {
            r = (void*)((unsigned long long)r & ((1UL<<63)-1));
            int num = *(int*)(r);
            int cap = *(int*)((char*)r+sizeof(int));
            void **vptrs = (void**)((char*)r+2*sizeof(int));
            if (num >= 2) {
                int ii =0;
                while(ii< num) {
                    if (vptrs[ii] == ptr) {
                        break;
                    }
                    else {
                        ii ++;
                    }
                }
                if (ii == num)
                    return false;
                while (ii< num-1) {
                    vptrs[ii] = vptrs[ii+1];
                    ii ++;
                }
                *(int*)(r) = num-1;
                num --;
            }
            if (num == 1) {
                void** sp = p_pbtree.get_recptr_sp (p, pos);
                *sp = vptrs[0];
                free ((char*)r, cap2leve(cap));
            }
        }
        else {
            p_pbtree.del (key);
        }
    }
    else {
        return false;
    }
    return true;
}

void* Pbtree::lookup (key_type key) {
    int pos = 0;
    void *p =  p_pbtree.lookup (key, &pos);
    return (key== p_pbtree.get_key(p,pos))?p_pbtree.get_recptr(p, pos):NULL;
}

int Pbtree::get_recptr (void *p, void* match[], int capicity, int &pos) {
    if (p == NULL)
        return 0;
    void *pp = (void*)(((unsigned long long)p)&(1UL<<63));
    if (pp) {
        pp = (void*)(((unsigned long long)p)&((1UL<<63)-1));
        int num = *(int*)pp;
        void **vptrs = (void**)(((char*)pp)+2*sizeof(int));
        int len = 0;
        while (pos< num) {
            match[len++] = vptrs[pos++];
            if (len >= capicity)
                return len;
        }
        return len;
    }
    else {
        if (pos == 0) {
            match[0] = p;
            pos ++;
            return 1;
        }
        return 0;
    }
}

void* Pbtree::lookup_s (key_type key, int *pos) {
    return p_pbtree.lookup (key, pos);
}
int Pbtree::scan (void **p, int *spos, key_type startkey, key_type endkey,void * area[], int *num)
{
    return p_pbtree.scan (p,spos,startkey,endkey,area,num);
}

void Pbtree::shut (void) {
}

void Pbtree::print (void) {
    p_pbtree.print ();
}
