/**
 * @file    debug_catalog.cc
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *
 * debug catalog
 *
 */

#include "catalog.h"

int test1(void)
{
    int64_t d_id = -1;

    g_catalog.createDatabase("db-0", d_id);
    Database *d_ptr = (Database *) g_catalog.getObjById(d_id);

    int64_t t_id[2];
    Table *t_ptr[2];

    g_catalog.createTable("tb-0", ROWTABLE, t_id[0]);
    g_catalog.createTable("tb-1", ROWTABLE, t_id[1]);
    t_ptr[0] = (Table *) g_catalog.getObjById(t_id[0]);
    t_ptr[1] = (Table *) g_catalog.getObjById(t_id[1]);
    d_ptr->addTable(t_id[0]);
    d_ptr->addTable(t_id[1]);

    int64_t c_id[6];
    g_catalog.createColumn("cl-0", INT32, 4, c_id[0]);
    g_catalog.createColumn("cl-1", INT64, 8, c_id[1]);
    g_catalog.createColumn("cl-2", INT32, 4, c_id[2]);
    g_catalog.createColumn("cl-3", FLOAT64, 8, c_id[3]);
    g_catalog.createColumn("cl-4", DATE, 4, c_id[4]);
    g_catalog.createColumn("cl-5", DATETIME, 4, c_id[5]);

    t_ptr[0]->addColumn(c_id[0]);
    t_ptr[0]->addColumn(c_id[1]);
    t_ptr[0]->addColumn(c_id[2]);
    t_ptr[1]->addColumn(c_id[3]);
    t_ptr[1]->addColumn(c_id[4]);
    t_ptr[1]->addColumn(c_id[5]);

    std::vector < int64_t > k1;
    k1.push_back(4);
    k1.push_back(5);
    Key key1;
    key1.set(k1);
    int64_t i_id;
    g_catalog.createIndex("ix-1", HASHINDEX, key1, i_id);
    t_ptr[0]->addIndex(i_id);
    g_catalog.print();
    g_catalog.initDatabase(d_id);

    int32_t i321 = 32;
    int64_t i64 = 64;
    int32_t i322 = 33;
    char *data[3] = { (char *) &i321, (char *) &i64, (char *) &i322 };
    t_ptr[0]->insert(data);
    i321++;
    i64++;
    i322++;
    t_ptr[0]->insert(data);
    t_ptr[0]->del(0L);
    t_ptr[0]->printData();

    return 0;
}

int main()
{
    printf("------catalog test(you can check stdout)------\n");
    g_memory.init(1 << 30, 1 << 3);
    g_catalog.init();
    test1();
    g_catalog.shut();
    g_memory.shut();
    printf("test pass!(checked by author)\n");
    return 0;
}
