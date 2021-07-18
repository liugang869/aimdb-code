/**
 * @file    catalog.cc
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *
 * this file provides an element container of database, which can be described as the stem of a tree
 * with this Catalog, the only one in global system, you can access all elements of system
 *  
 * basic usage:
 *
 * (1) you use g_catalog.createXXX[Database,Table,Column,Index] to create objects.
 * (2) you can get the pointer of [Database,Table,Column,Index] by call g_catalog.getObjById/Name
 * (3) in [Database,Table,Column,Index], you can define the relation of different objects by adding operation in those object
 * (4) when all relation defined in database, you must call initDatabase to actually getting the database prepared to be used.
 * (5) shut will get everything shutup can free 
 *     shutDatabase will only shutup the selected database
 * (6) for more tips, you may learn from debug_catalog.cc
 *
 */

#include "catalog.h"

Catalog g_catalog;
bool Catalog::createDatabase(const char *name, int64_t & d_id)
{
    auto it = cl_name_obj.find(name);
    if (it != cl_name_obj.end())
        return false;
    int64_t id = obtainId();
    Database *p = new Database(id, name);
    d_id = registerObj(p);
    return id == d_id ? true : false;
}

bool Catalog::createTable(const char *name, TableType type, int64_t & t_id)
{
    auto it = cl_name_obj.find(name);
    if (it != cl_name_obj.end())
        return false;
    int64_t id = obtainId();
    Table *p = NULL;
    switch (type) {
    case INVID_T:
        printf("[Catalog][ERROR][createTable]: type error! -1\n");
        return false;
    case ROWTABLE:
        p = new RowTable(id, name);
        break;
    case COLTABLE:
        printf("[Catalog][ERROR][createTable]: column type not support! -2\n");
        return false;   // not support
    case MAXTYPE_T:
        printf("[Catalog][ERROR][createTable]: type error! -3\n");
        return false;
    }
    t_id = registerObj(p);
    return id == t_id ? true : false;
}

bool Catalog::createColumn(const char *name, ColumnType type,
                           int64_t option_size, int64_t & c_id)
{
    auto it = cl_name_obj.find(name);
    if (it != cl_name_obj.end())
        return false;
    int64_t id = obtainId();
    Column *p = new Column(id, name, type, option_size);
    c_id = registerObj(p);
    return id == c_id ? true : false;
}

bool Catalog::createIndex(const char *name, IndexType type, Key i_key,
                          int64_t & i_id)
{
    auto it = cl_name_obj.find(name);
    if (it != cl_name_obj.end())
        return false;
    int64_t id = obtainId();
    Index *p = NULL;
    ColumnType ct = INVID_C;
    switch (type) {
    case INVID_I:
        printf("[Catalog][ERROR][createIndex]: type error! -4\n");
        return false;
    case HASHINDEX:
        p = new HashIndex(id, name, i_key);
        break;
    case BPTREEINDEX:
        if (i_key.getKey().size() != 1) {
            printf("[Catalog][ERROR][createIndex]: key number not 1! -5\n");
            return false;
        }
        ct = ((Column*)getObjById(i_key.getKey()[0]))->getCType();
        if ((ct>=FLOAT32 && ct<=CHARN) || ct<=INVID_C || ct>+MAXTYPE_I) {
            printf("[Catalog][ERROR][createIndex]: key type not support! -5\n");
            return false;
        }
        p = new PbtreeIndex(id, name, i_key);
        break;
        // printf("[Catalog][ERROR][createIndex]: bptree index not support! -5\n");
        // return false;
    case ARTTREEINDEX:
        printf("[Catalog][ERROR][createIndex]: arttree index not support! -6\n");
        return false;    // not support
    case MAXTYPE_I:
        printf("[Catalog][ERROR][createIndex]: type error! -7\n");
        return false;
    }
    i_id = registerObj(p);
    return id == i_id ? true : false;
}

Object *Catalog::getObjById(int64_t o_id)
{
    return (uint64_t)o_id < cl_id_obj.size()? cl_id_obj[o_id] : NULL;
}

Object *Catalog::getObjByName(char *o_name)
{
    auto it = cl_name_obj.find(o_name);
    if (it == cl_name_obj.end())
        return NULL;
    return it->second;
}

bool Catalog::initDatabase(int64_t d_id)
{
    Database *database = (Database *) getObjById(d_id);
    if (database->getOtype() != DATABASE) {
        printf
            ("[Catalog][ERROR][initDatabase]: database type error! -8\n");
        return false;
    }
    std::vector < int64_t > &tables = database->getTables();
    for (unsigned int ii = 0; ii < tables.size(); ii++) {
        if (initTable(tables[ii]) == false) {
            printf
                ("[Catalog][ERROR][initDatabase]: table init error! -9\n");
            return false;
        }
    }
    return true;
}

bool Catalog::initTable(int64_t t_id)
{
    Table *table = (Table *) getObjById(t_id);
    if (table->getOtype() != TABLE) {
        printf("[Catalog][ERROR][initTable]: table type error! -10\n");
        return false;
    }
    TableType type = table->getTtype();
    switch (type) {
    case INVID_T:
        break;
    case ROWTABLE:
        {
            RowTable *rt = (RowTable *) table;
            RPattern & pattern = rt->getRPattern();
            MStorage & storage = rt->getMStorage();
            std::vector < int64_t > &columns = table->getColumns();
            pattern.init(columns.size() + 1);
            for (unsigned int ii = 0; ii < columns.size(); ii++) {
                if (initColumn(columns[ii]) == false) {
                    printf
                        ("[Catalog][ERROR][initTable]: column init error! -11\n");
                    return false;
                }
                Column *column = (Column *) getObjById(columns[ii]);
                pattern.addColumn(column->getDataType());
                column->setCoffset (pattern.getColumnOffset((int64_t)ii));
            }
            TypeCharN *tc = new TypeCharN(1);
            pattern.addColumn(tc);
            storage.init(pattern.getRowSize());
            std::vector < int64_t > &indexs = table->getIndexs();
            for (unsigned int ii = 0; ii < indexs.size(); ii++) {
                if (initIndex(indexs[ii],t_id) == false) {
                    printf("[Catalog][ERROR][initTable]: index init error! -13\n");
                    return false;
                }
            }
        }
        break;
    case COLTABLE:
        printf("[Catalog][ERROR][initTable]: column table not support! -12\n");
        return false;
    case MAXTYPE_T:
        break;
    }
    return true;
}

bool Catalog::initColumn(int64_t c_id)
{
    Column *column = (Column *) getObjById(c_id);
    if (column->getOtype() != COLUMN) {
        printf("[Catalog][ERROR][initColumn]: column type error! -14\n");
        return false;
    }
    column->init();
    column->finish();
    return true;
}

bool Catalog::initIndex(int64_t i_id, int64_t t_id)
{
    Index *index = (Index *) getObjById(i_id);
    if (index->getOtype() != INDEX) {
        printf("[Catalog][ERROR][initIndex]: index type error! -15\n");
        return false;
    }
    index->setIndexTid (t_id);
    IndexType type = index->getIType();
    switch (type) {
    case INVID_I:
        printf("[Catalog][ERROR][initIndex] index type error! -16\n");
        return false;
    case HASHINDEX:
        {
            HashIndex *p = (HashIndex *) index;
            p->init();
            p->setCellCap(16);  // this value can be set global, it's hashtable size 1<<16 cells
            Key & key = p->getIKey();
            std::vector < int64_t > &vv = key.getKey();
            for (unsigned int ii = 0; ii < vv.size(); ii++) {
                Column *column = (Column *) getObjById(vv[ii]);
                p->addIndexDTpye(column->getDataType(),column->getCoffset());
            }
            p->finish();
        }
        break;
    case BPTREEINDEX:
        {
            PbtreeIndex *p = (PbtreeIndex *) index;
            p->init();
            Key & key = p->getIKey();
            std::vector <int64_t> &vv = key.getKey();
            Column *column = (Column *) getObjById(vv[0]);
            p->setIndexDTpye(column->getDataType());
            p->finish();
            // printf("[Catalog][ERROR][initIndex]: bptree index not support! -17\n");
            // return false;
        }
        break;
    case ARTTREEINDEX:
        {
            printf("[Catalog][ERROR][initIndex]: arttree index not support! -18\n");
            return false;
        }
    case MAXTYPE_I:
        {
            printf("[Catalog][ERROR][initIndex]: index type error! -19\n");
            return false;
        }
    }
    return true;
}

bool Catalog::shut(void)
{
    for (unsigned int ii = 0;ii < cl_id_obj.size(); ii++) {
        if (cl_id_obj[ii] != NULL) {
            cl_id_obj[ii]->shut ();
        }
    }
    return true;
}

bool Catalog::shutDatabase(int64_t d_id)
{
    return true;
}
