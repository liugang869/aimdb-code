#include "pbtree.h"
#include <vector>
#include <iterator>

int test1(void) {
    pbtree the_pbtree;
    the_pbtree.init();

    the_pbtree.insert ((key_type)1,(void*)1);
    the_pbtree.insert ((key_type)2,(void*)2);
    the_pbtree.print ();

    the_pbtree.del ((key_type)1);
    the_pbtree.del ((key_type)2);
    the_pbtree.print ();

    the_pbtree.insert ((key_type)3,(void*)3);
    the_pbtree.insert ((key_type)4,(void*)4);
    the_pbtree.insert ((key_type)5,(void*)3);
    the_pbtree.insert ((key_type)6,(void*)4);
    the_pbtree.insert ((key_type)7,(void*)3);
    the_pbtree.insert ((key_type)8,(void*)4);
    the_pbtree.print ();

    int pos = 0;
    void *p = the_pbtree.lookup ((key_type)6, &pos);
    void *ptr = the_pbtree.get_recptr (p, pos);
    printf ("%p\n", ptr);

    the_pbtree.shut();
    return 0;
}

int test2(void) {
    pbtree the_pbtree;
    the_pbtree.init();

    srand (100);
    std::vector<int> intree,outree;
    for (int ii= 1;ii< 1000; ii++)
        intree.push_back (ii);
    for (int ii= 1;ii< 10000000; ii++) {
       printf ("\n");
       int op = rand() % 2;
        if (op> 0) {
            if (intree.size()== 0) continue;
            int pos = rand() % intree.size();
            std::vector<int>::iterator it = intree.begin()+ pos;
            int vv = *it;
//            printf ("insert: %d\n", vv);
            the_pbtree.insert ((key_type)vv,(void*)(unsigned long long)vv);
            intree.erase (it);
            outree.push_back (vv);
        }
        else {
            if (outree.size()== 0) continue;
            int pos = rand() % outree.size();
            std::vector<int>::iterator it = outree.begin()+ pos;
            int vv = *it;
//            printf ("delete: %d\n", vv);
            the_pbtree.del ((key_type)vv);
            outree.erase (it);
            intree.push_back (vv);
       }
    }
    the_pbtree.shut();
    printf ("test2 pass!\n");
    return 0;
}

int test3 (void) {
    Pbtree the_pbtree;
    the_pbtree.init();
    the_pbtree.insert ((key_type)10, (void*)1);
    the_pbtree.insert ((key_type)10, (void*)2);
    the_pbtree.insert ((key_type)10, (void*)3);
    the_pbtree.insert ((key_type)10, (void*)4);
    the_pbtree.insert ((key_type)10, (void*)5);
    the_pbtree.insert ((key_type)10, (void*)6);
    the_pbtree.insert ((key_type)10, (void*)7);
    the_pbtree.insert ((key_type)10, (void*)8);
    the_pbtree.print  ();
 
    the_pbtree.del ((key_type)10, (void*)1);
    the_pbtree.del ((key_type)10, (void*)2);
    the_pbtree.del ((key_type)10, (void*)3);
    the_pbtree.del ((key_type)10, (void*)4);
    the_pbtree.del ((key_type)10, (void*)5);
    the_pbtree.del ((key_type)10, (void*)6);
    the_pbtree.del ((key_type)10, (void*)7);
    the_pbtree.del ((key_type)10, (void*)8);
    the_pbtree.print  ();

    the_pbtree.insert ((key_type)10, (void*)6);
    the_pbtree.insert ((key_type)10, (void*)7);
    the_pbtree.insert ((key_type)10, (void*)8);
    the_pbtree.print  ();
    return 0; 
}

void test4 (void) {
    Pbtree the_pbtree;
    the_pbtree.init ();

    srand (101);
    std::vector<int> intree,outree;
    for (int ii= 1;ii<= 100000; ii++) {
        intree.push_back (ii);
    }
    for (int ii= 1;ii< 10000000; ii++) {
//     printf ("\n");
       int op = rand() % 3;
        if (op> 0) {
            if (intree.size()== 0) continue;
            int pos = rand() % intree.size();
            std::vector<int>::iterator it = intree.begin()+ pos;
            int vv = *it;
//            printf ("insert: %d\n", vv);
            the_pbtree.insert ((key_type)(vv/20),(void*)(unsigned long long)(vv%20));
            intree.erase (it);
            outree.push_back (vv);
        }
        else {
            if (outree.size()== 0) continue;
            int pos = rand() % outree.size();
            std::vector<int>::iterator it = outree.begin()+ pos;
            int vv = *it;
//            printf ("delete: %d\n", vv);
            the_pbtree.del ((key_type)(vv/20),(void*)(unsigned long long)(vv%20));
            outree.erase (it);
            intree.push_back (vv);
       }
    }
    the_pbtree.print ();
    the_pbtree.shut();
    printf ("test4 pass!\n"); 
}

void test5 (void) {
    Pbtree the_pbtree;
    the_pbtree.init ();

    the_pbtree.insert ((key_type)10, (void*)1);
    the_pbtree.insert ((key_type)10, (void*)2);
    the_pbtree.insert ((key_type)10, (void*)3);
    the_pbtree.insert ((key_type)10, (void*)4);
    the_pbtree.insert ((key_type)10, (void*)5);
    the_pbtree.insert ((key_type)10, (void*)6);
    the_pbtree.insert ((key_type)10, (void*)7);
    the_pbtree.insert ((key_type)10, (void*)8);
    
    the_pbtree.insert ((key_type)12, (void*)9);
    the_pbtree.insert ((key_type)13, (void*)10);
    the_pbtree.insert ((key_type)17, (void*)11);
    the_pbtree.insert ((key_type)14, (void*)12);
    the_pbtree.insert ((key_type)7,  (void*)13);
    the_pbtree.insert ((key_type)3,  (void*)14);
    the_pbtree.insert ((key_type)9,  (void*)15);
    the_pbtree.insert ((key_type)100,  (void*)16);
    the_pbtree.print ();

    int pos = 0;
    void *match[16];
    void* ptr = the_pbtree.lookup ((key_type)10);
    int len = the_pbtree.get_recptr (ptr,match,16,pos);
    printf ("len: %d\n", len);
    for (int ii=0;ii<len;ii++) {
        printf ("%p,", match[ii]);
    }

    printf ("\nscan\n");
    int num = 16;
    void *area[16];
    void* p = the_pbtree.lookup_s ((key_type)9, &pos);
    while(1) {
        int ss = the_pbtree.scan (&p, &pos, (key_type)9, (key_type)14, area,&num);
        for (int ii=0;ii< num; ii++) {
            ptr = area[ii];
            pos = 0;
            int len = the_pbtree.get_recptr (ptr,match,16,pos);
            printf ("ptr:%p len: %d\n",ptr,len);
            for (int ii=0;ii<len;ii++) {
                printf ("%p,", match[ii]);
            }
            printf ("\n");
        }
        if (ss != 0) break;
    }
}

int main () {
    printf ("sizeof(LL):%lu\n",sizeof(long long));
    printf ("sizeof(P8):%lu\n",sizeof(Pointer8B));
    g_memory.init (1L<<30,1L<<3);
    test5 ();
    g_memory.shut ();
}

