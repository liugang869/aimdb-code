/**
 * @file    debug_datatype.cc
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *
 * debug datatype
 *
 */

#include "datatype.h"

int test1()
{
    time_t tt;
    TypeTime dt;
    char cc[32];
    strcpy(cc, "23:18:47");
    char oc[32];
    dt.formatBin(&tt, cc);
    dt.formatTxt(oc, &tt);
    // printf ("oc-1: %s tt:%lu\n", oc,tt);
    if (!strcmp(oc, "23:18:47"))
        printf("test 1 pass!\n");
    else
        printf("test 1 fail!\n");
    return 0;
}

int test2()
{
    time_t tt;
    TypeDateTime dt;
    char cc[32];
    strcpy(cc, "2017-11-22 01:01:01");
    char oc[32];
    dt.formatBin(&tt, cc);
    dt.formatTxt(oc, &tt);
    if (!strcmp(oc, "2017-11-22 01:01:01"))
        printf("test 2 pass!\n");
    else
        printf("test 2 fail!\n");
    return 0;
}

int test3()
{
    TypeCharN tc(10);
    char data[10] = "beijing";
    char buf[10];
    tc.copy(buf, data);
    if (!strcmp(data, "beijing"))
        printf("test 3 pass!\n");
    else
        printf("test 3 fail!\n");
    return 0;
}

int test4()
{

    TypeInt8 t8;
    int8_t v8 = 8, v2 = 0, v3 = 0;
    char c8[8], c9[8];
    t8.copy(&v2, &v8);
    t8.formatTxt(c8, &v2);
    t8.formatBin(&v3, c8);
    t8.formatTxt(c9, &v3);

    if (!strcmp(c9, "8"))
        printf("test 4 pass!\n");
    else
        printf("test 4 fail!\n");

    return 0;
}

int main()
{
    printf("------datatype test------\n");
    test1();
    test2();
    test3();
    test4();
    printf("test pass!(checked by author)\n");
    return 0;
}
