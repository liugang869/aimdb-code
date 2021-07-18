/**
 * @file    debug_schema.cc
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *
 * debug schema 
 *
 */

#include "schema.h"

int test1(void)
{

    Database *d_ptr = new Database(1, "db-1");

    Table *t_ptr[2];

    t_ptr[0] = new Table(2, "tb-1", ROWTABLE);
    t_ptr[1] = new Table(3, "tb-2", ROWTABLE);
    d_ptr->addTable(2);
    d_ptr->addTable(3);

    Column *c_ptr[6];
    c_ptr[0] = new Column(4, "cl-1", INT32, 4);
    c_ptr[1] = new Column(5, "cl-2", INT64, 8);
    c_ptr[2] = new Column(6, "cl-3", FLOAT32, 4);
    c_ptr[3] = new Column(7, "cl-4", FLOAT64, 8);
    c_ptr[4] = new Column(8, "cl-5", DATE, 4);
    c_ptr[5] = new Column(9, "cl-6", CHARN, 20);

    t_ptr[0]->addColumn(4);
    t_ptr[0]->addColumn(5);
    t_ptr[0]->addColumn(6);
    t_ptr[1]->addColumn(7);
    t_ptr[1]->addColumn(8);
    t_ptr[1]->addColumn(9);

    std::vector < int64_t > k1;
    k1.push_back(4);
    k1.push_back(5);
    Key key1;
    key1.set(k1);
    Index *i_ptr = new Index(10, "ix-1", HASHINDEX, key1);
    t_ptr[0]->addIndex(10);

    d_ptr->print();
    t_ptr[0]->print();
    c_ptr[0]->print();
    i_ptr->print();

    delete d_ptr;
    for (int ii = 0; ii < 2; ii++)
        delete t_ptr[ii];
    for (int ii = 0; ii < 6; ii++)
        delete c_ptr[ii];
    delete i_ptr;

    return 0;
}

int main()
{
    printf("------test schema------\n");
    test1();
    printf("test pass!(checked by author)\n");
    return 0;
}
