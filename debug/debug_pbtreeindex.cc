/**
 * @file    debug_pbtreeindex.cc
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 * 
 * @section DESCRIPTION
 *
 * debug pbtree index
 *
 */

#include "pbtreeindex.h"

int test1 () {
    Key key;
    std::vector <int64_t> kk;
    kk.push_back (1);
    key.set (kk);
    PbtreeIndex pbindex(1,"piname",key);
    pbindex.init ();
    printf ("pb index init...\n");

    long long int vv = 10;
    pbindex.insert (&vv, (void*)0x1);
    pbindex.insert (&vv, (void*)0x2);
    pbindex.insert (&vv, (void*)0x3);
    pbindex.insert (&vv, (void*)0x4);
    pbindex.insert (&vv, (void*)0x5);

    pbindex.del (&vv,(void*)0x3);
    pbindex.del (&vv,(void*)0x10);
    pbindex.print ();
    
    return 0;
}

int test2 () {
    Key key;
    std::vector <int64_t> kk;
    kk.push_back (1);
    key.set (kk);
    PbtreeIndex pbindex(1,"piname",key);
    BasicType *dt = new TypeInt64();
    pbindex.setIndexDTpye(dt);
    pbindex.init ();
    printf ("pb index init...\n");

    long long int      vv = 1;
    int                dd = 1;
    while (vv <= 20) {
        for ( ; dd%20 !=0; dd ++)
            pbindex.insert (&vv,(void*)dd);
        dd ++;
        vv ++;
    }
    pbindex.print ();

    printf ("lookup test:\n");
    struct PbtreeInfo pbinfo;
    vv = 8;
    pbindex.set_ls (&vv,NULL,&pbinfo);
    void *result;
    while (pbindex.lookup(&vv,&pbinfo,result)) {
        printf ("%p\t",result);
    }
    
    printf ("\nscan test:\n");
    long long int vv1 = 8,vv2 = 10;
    pbindex.set_ls (&vv1, &vv2, &pbinfo);
    while (pbindex.scan(&pbinfo, result)) {
        printf ("%p\t" ,result);
    }
    printf ("\n");
    return 0;
}

int main () {
    g_memory.init (1L<<30,1L<<3);
    test2 ();
    g_memory.shut ();
}
