/**
 * @file    debug_mymemory.cc
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *
 * debug mymemory
 *
 */

#include "mymemory.h"
#include <stdlib.h>
#include <iterator>

int test1()
{
    // test m_array_list
    printf("test 1:\n");
    Memory mem;
    mem.init(1 << 10, 1 << 3);
    std::vector < char *>test;
    for (int ii = 0; ii < 1 << 5; ii++) {
        char *p = NULL;
        int64_t size = 1 << 3;
        size = mem.alloc(p, size);
        if (size == 1 << 3)
            test.push_back(p);
        mem.print();
    }
    mem.print();
    mem.shut();
    return 0;
}

int test2()
{
    // test m_array_list
    printf("test 2:\n");
    Memory mem;
    mem.init(1 << 10, 1 << 3);
    std::vector < char *>test;
    srand(100);
    for (int ii = 0; ii < 1 << 5; ii++) {
        char *p = NULL;
        int64_t size = 1 << 3;
        int t = rand() % 4;
        if (t < 2) {
            size = mem.alloc(p, size);
            if (size == 1 << 3)
                test.push_back(p);
        } else {
            if (test.size() >= 1) {
                size = mem.free(test[test.size() - 1], 1 << 3);
                test.pop_back();
            }
        }
        mem.print();
    }
    mem.print();
    mem.shut();
    return 0;
}

struct Slab {
    char *p;
    int64_t sz;
};

int test3()
{
    printf("test 3:\n");
    Memory mem;
    mem.init(1 << 10, 1 << 3);
    std::vector < Slab > test;
    srand(100);
    for (int ii = 0; ii < 1000; ii++) {
        printf("i: %d\n", ii);
        int t = rand() % 2;
        int s = rand() % 8;
        if (t < 1) {
            char *p = NULL;
            int64_t size = mem.alloc(p, 1 << (s + 3));
            if (size == 1 << s) {
                struct Slab ss = { p, 1 << (s + 3) };
                test.push_back(ss);
            }
        } else {
            if (test.size() == 0)
                continue;
            int c = rand() % test.size();
            struct Slab ss = test[c];
            mem.free(ss.p, ss.sz);
            test.erase(test.begin() + c);
        }
        mem.print();
    }
    mem.print();
    mem.shut();
    return 0;
}

int main()
{
    printf
        ("------mymemory test(you can check the output of stdout)------\n");
    test1();
    test2();
    test3();
    printf("test pass!(checked by author)\n");
    return 0;
}
