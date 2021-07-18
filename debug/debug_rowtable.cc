/**
 * @file    debug_rowtable.cc
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *
 * debug rowtable
 *
 */

#include "rowtable.h"

int test1(void)
{
    RowTable *rt = new RowTable(1, "rowtable-1");

    rt->addColumn(1);
    rt->addColumn(2);
    rt->addColumn(3);
    rt->addColumn(4);
    rt->addColumn(5);
    rt->addColumn(6);
    rt->addColumn(7);
    rt->addColumn(8);
    rt->addColumn(9);
    rt->addColumn(10);

    RPattern & pattern = rt->getRPattern();
    MStorage & storage = rt->getMStorage();
    pattern.init(11);

    BasicType *c1 = new TypeInt8();
    BasicType *c2 = new TypeInt16();
    BasicType *c3 = new TypeInt32();
    BasicType *c4 = new TypeInt64();
    BasicType *c5 = new TypeFloat32();
    BasicType *c6 = new TypeFloat64();
    BasicType *c7 = new TypeCharN(16);
    BasicType *c8 = new TypeDate();
    BasicType *c9 = new TypeTime();
    BasicType *c10 = new TypeDateTime();
    BasicType *c11 = new TypeCharN(1);

    pattern.addColumn(c1);
    pattern.addColumn(c2);
    pattern.addColumn(c3);
    pattern.addColumn(c4);
    pattern.addColumn(c5);
    pattern.addColumn(c6);
    pattern.addColumn(c7);
    pattern.addColumn(c8);
    pattern.addColumn(c9);
    pattern.addColumn(c10);
    pattern.addColumn(c11);

    storage.init(pattern.getRowSize());

    int8_t i8 = 8;
    int16_t i16 = 16;
    int32_t i32 = 32;
    int64_t i64 = 64;
    float f32 = 32.5;
    double f64 = 64.5;
    char c16[64] = "charn16";
    char sdate[64] = "2017-11-22";
    char stime[64] = "09:54:35";
    char sdatetime[64] = "2017-11-22 01:01:01";
    time_t date, time, datetime;
    char cc = 'Y';

    c8->formatBin(&date, sdate);
    c9->formatBin(&time, stime);
    c10->formatBin(&datetime, sdatetime);

    char *data[11] =
        { (char *) &i8, (char *) &i16, (char *) &i32, (char *) &i64,
        (char *) &f32, (char *) &f64, (char *) c16, (char *) &date,
        (char *) &time, (char *) &datetime, &cc
    };
    rt->insert(data);

    for (int64_t ii = 0; ii < 1000000; ii++) {
        i64++;
        //printf ("%ld\n", ii);
        rt->insert(data);
    }

    // rt->printData  ();
    rt->shut();
    delete rt;
    return 0;
}

int test2()
{
    RowTable *rt = new RowTable(1, "rowtable-1");

    rt->addColumn(1);
    rt->addColumn(2);
    rt->addColumn(3);
    rt->addColumn(4);
    rt->addColumn(5);
    rt->addColumn(6);
    rt->addColumn(7);
    rt->addColumn(8);
    rt->addColumn(9);
    rt->addColumn(10);

    RPattern & pattern = rt->getRPattern();
    MStorage & storage = rt->getMStorage();
    pattern.init(11);

    BasicType *c1 = new TypeInt8();
    BasicType *c2 = new TypeInt16();
    BasicType *c3 = new TypeInt32();
    BasicType *c4 = new TypeInt64();
    BasicType *c5 = new TypeFloat32();
    BasicType *c6 = new TypeFloat64();
    BasicType *c7 = new TypeCharN(16);
    BasicType *c8 = new TypeDate();
    BasicType *c9 = new TypeTime();
    BasicType *c10 = new TypeDateTime();
    BasicType *c11 = new TypeCharN(1);

    pattern.addColumn(c1);
    pattern.addColumn(c2);
    pattern.addColumn(c3);
    pattern.addColumn(c4);
    pattern.addColumn(c5);
    pattern.addColumn(c6);
    pattern.addColumn(c7);
    pattern.addColumn(c8);
    pattern.addColumn(c9);
    pattern.addColumn(c10);
    pattern.addColumn(c11);

    storage.init(pattern.getRowSize());

    int8_t i8 = 8;
    int16_t i16 = 16;
    int32_t i32 = 32;
    int64_t i64 = 64;
    float f32 = 32.5;
    double f64 = 64.5;
    char c16[64] = "charn16";
    char sdate[64] = "2017-11-22";
    char stime[64] = "09:54:35";
    char sdatetime[64] = "2017-11-22 01:01:01";
    time_t date, time, datetime;
    char cc = 'Y';

    c8->formatBin(&date, sdate);
    c9->formatBin(&time, stime);
    c10->formatBin(&datetime, sdatetime);

    char *data[11] =
        { (char *) &i8, (char *) &i16, (char *) &i32, (char *) &i64,
        (char *) &f32, (char *) &f64, (char *) c16, (char *) &date,
        (char *) &time, (char *) &datetime, &cc
    };
    rt->insert(data);

    for (int64_t ii = 0; ii < 40; ii++) {
        i64++;
        rt->insert(data);
    }
    for (int64_t ii = 10; ii < 20; ii++) {
        rt->del(ii);
    }
    for (int64_t ii = 15; ii < 25; ii++) {
        int64_t var = 0;
        int64_t col[3] = { 2, 4, 6 };
        char *ccc[3] = { (char *) &var, (char *) &var, (char *) &var };
        rt->updateCols(ii, 3, col, ccc);
    }

    for (int64_t ii = 5; ii < 15; ii++) {
        int64_t var = -1;
        rt->selectCol(ii, 3, (char *) &var);
        printf("%ld\n", var);
    }
/*
    for (int64_t ii = 5; ii < 15; ii++) {
        int64_t col[3] = { 2, 4, 6 };
        char ccc[1024];
        if (rt->selectCols(ii, 3, col, ccc))
            printf("%d,%f\n", (*(int*)(ccc)),
                   (*(float *) (ccc + 4)));
    }
*/
    rt->printData();
    rt->shut();
    delete rt;
    return 0;
}

int main()
{
    printf("------rowtable test(you can check output of stdout)------\n");
    g_memory.init(1 << 30, 1 << 3);
    test2();
    g_memory.shut();
    printf("test pass!(checked by author)\n");
    return 0;
}
