/**
 * @file    catalog.h
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

#ifndef _CATALOG_H
#define _CATALOG_H

#include <vector>
#include <unordered_map>
#include "schema.h"
#include "rowtable.h"
#include "hashindex.h"
#include "pbtreeindex.h"

/** definition of class Catalog. */
class Catalog {
  private:
    std::vector <Object *> cl_id_obj;   /**< container of Objects  */
    std::unordered_map <std::string, Object *> cl_name_obj;    /**< name map of Objects ,name should not be the same in the whole Catalog */

  public:
    /**
     * init operation.
     */
    void init(void) {
        cl_id_obj.push_back(NULL);
    }
    /**
     * shut down, free all memory of Objects.
     */ 
    bool shut(void);
    /**
     * create database.
     * @param  name  database name
     * @param  d_id  reference of database identifier, position in cl_id_obj
     * @retval true  success
     * @retval false failure
     */
    bool createDatabase(const char *name, int64_t & d_id);
    /**
     * create table.
     * @param name   table name
     * @param type   tabletype: [ROWTABLE,COLUMNTABLE]
     * @param t_id   reference of table identifier, position in cl_id_obj
     * @retval true  success
     * @retval false failure
     */
    bool createTable(const char *name, TableType type, int64_t & t_id);
    /**
     * create column.
     * @param name        column name
     * @param type        column type: [INT8,INT16,...]
     * @param option_size only work for column type CHARN(option_size)
     * @param c_id        reference of column identifier, position in cl_id_obj
     * @retval true       success
     * @retval false      failure
     */
    bool createColumn(const char *name, ColumnType type,int64_t option_size, int64_t & c_id);
    /**
     * create index.
     * @param name   index name
     * @param type   index type: [HASHINDEX,BPTREEINDEX,ARTTREEINDEX]
     * @param i_key  stores column identifiers of this index
     * @param i_id   reference of index identifier, position in cl_id_obj
     * @retval true  success
     * @retval false failure
     */
    bool createIndex(const char *name, IndexType type, Key i_key,int64_t & i_id);
    /**
     * init database, very important, after all setting, call initDatabase to get this database in work
     * @param  d_id  which database to prepare
     * @retval true  success
     * @retval false failure
     */
    bool initDatabase(int64_t d_id);    //  this function will truly init the inherited class
    /**
     * shutdowm database
     * @param  d_id  which database to shut
     * @retval true  success
     * @retval false failure
     */
    bool shutDatabase(int64_t d_id);
    /**
     * get object[DATABASE,TABLE,COLUMN,INDEX] by identifier
     * @param  o_id identifier of object
     * @retval != NULL available
     * @retval == NULL unavaliable, deleted or not exist
     */
    Object *getObjById(int64_t o_id);
    /**
     * get object[DATABASE,TABLE,COLUMN,INDEX] by object name
     * @param  name of an object
     * @retval != NULL available
     * @retval == NULL unavaliable, deleted or not exist
     */
    Object *getObjByName(char *o_name);
    /**
     * print the catalog 
     */
    void print(void) {
        for (unsigned int ii = 0; ii < cl_id_obj.size(); ii++) {
            Object *obj = cl_id_obj[ii];
            if (obj == NULL)
                continue;
            obj->print();
        }
    }

  private:
    /**
     * init table
     * @param  t_id  which table to prepare
     * @retval true  success
     * @retval false failure
     */
    bool initTable(int64_t t_id);
    /**
     * init column
     * @param  c_id  which column to prepare
     * @retval true  success
     * @retval false failure
     */
    bool initColumn(int64_t c_id);
    /**
     * init index
     * @param  i_id  which index to prepare
     * @param  t_id  index in which table
     * @retval true  success
     * @retval false failure
     */
    bool initIndex(int64_t i_id,int64_t t_id);
    /**
     * put object in cl_id_obj and cl_name_obj
     * @param obj pointer of object to put
     * @retval >0  success
     * @retval <=0 failure
     */
    int64_t registerObj(Object * obj) {
        cl_id_obj[obj->getOid()] = obj;
        cl_name_obj.insert(std::pair < std::string,
                           Object * >(obj->getOname(), obj));
        return obj->getOid();
    }
    /**
     * get a free object identifier
     * @retval  >0 success
     * @retval <=0 failure
     */
    int64_t obtainId(void) {
        int64_t id = cl_id_obj.size();
        cl_id_obj.push_back(NULL);
        return id;
    }
};  // class Catalog

extern Catalog g_catalog;
#endif
