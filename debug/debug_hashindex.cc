/**
 * @file    debug_hashindex.cc
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *
 * debug hashindex
 *
 */

#include "hashindex.h"

int test1()
{

    Key key;
    std::vector < int64_t > kk;
    kk.push_back(1);
    kk.push_back(2);
    kk.push_back(3);
    key.set(kk);

    HashIndex hx(1, "hashindex-1", key);
    hx.init();
    hx.setCellCap(9);
    TypeInt16 i16;
    TypeInt32 i32;
    TypeCharN c16(6);
    hx.addIndexDTpye(&i16,0);
    hx.addIndexDTpye(&i32,2);
    hx.addIndexDTpye(&c16,6);
    hx.finish();
    hx.print ();

    int16_t iv16 = 16;
    int32_t iv32 = 32;
    const char cv16[16] = "cv16";
    void *data[3] = { (void *) &iv16, (void *) &iv32, (void *) &cv16 };
    char act_d[10][64] = {
        {0x10, '\0', 0x20, '\0', '\0', '\0', 'c', 'v', '1', '6', '\0'},
        {0x10, '\0', 0x21, '\0', '\0', '\0', 'c', 'v', '1', '6', '\0'},
        {0x10, '\0', 0x20, '\0', '\0', '\0', 'c', 'v', '1', '6', '\0'},
        {0x10, '\0', 0x23, '\0', '\0', '\0', 'c', 'v', '1', '6', '\0'},
        {0x10, '\0', 0x20, '\0', '\0', '\0', 'c', 'v', '1', '6', '\0'},
        {0x10, '\0', 0x25, '\0', '\0', '\0', 'c', 'v', '1', '6', '\0'},
        {0x10, '\0', 0x20, '\0', '\0', '\0', 'c', 'v', '1', '6', '\0'},
        {0x10, '\0', 0x27, '\0', '\0', '\0', 'c', 'v', '1', '6', '\0'},
        {0x10, '\0', 0x28, '\0', '\0', '\0', 'c', 'v', '1', '6', '\0'},
        {0x10, '\0', 0x29, '\0', '\0', '\0', 'c', 'v', '1', '6', '\0'}
    };
    void *p = NULL;
    for (int64_t ii = 1; ii <= 10; ii++) {
        p = (void *) act_d[ii - 1];
        printf("insert- %ld result:%d\n", ii, hx.insert(data, p));
    }

    HashInfo hf;
    hx.set_ls(data, NULL, &hf);

    hx.del(data);
    p = (void *) act_d[9];
    printf("insert dup: %d\n", hx.insert(data, p));
    while (hx.lookup(data, &hf, p)) {
        printf("lookup- %p %d\n", p, *(int16_t *) p);
    }

    hx.shut();
    return 0;
}

int main()
{
    printf
        ("------hashindex test(you can check the output of stdout)------\n");
    g_memory.init(1L << 30, 1L << 3);
    test1();
    g_memory.shut();
    printf("test pass!(checked by author)\n");
    return 0;
}
