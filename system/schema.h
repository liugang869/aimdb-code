/**
 *  @file   schema.h
 *  @author liugang(liugang@ict.ac.cn)
 *  @version 0.1
 *
 *  @section DESCRIPTION
 *
 *  this file defines the abstract class of four primary elements of database system,
 *  these abstract classes provide uniform interface for upper application
 *
 *  basic interface:
 *  init,finish,shut,select,insert,update,del,selectCol,lookup,scan
 *
 *  notice:
 *  for insert,del,update, input data requires to be processed by BasicType method 
 *   formatBin, then call above function to actually put the into table
 *  example:
 *    char date[10] = 1970-01-01
 *    TypeDate type;
 *    char buff[10];
 *    type.format (buff, date); // in buff, it's stored as time_t with 4 Byte
 *    then, you can perform insert ()
 *
 */

#ifndef _SCHEMA_H
#define _SCHEMA_H

#include <string.h>
#include <vector>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "datatype.h"

/** an enum for ObjectType label. */
enum ObjectType {
    INVID_O = 0,
    DATABASE,  /**< database */
    TABLE,     /**< table    */
    COLUMN,    /**< column   */
    INDEX,     /**< index    */
    MAXTYPE_O
};

/** an enum for Index.  */
enum IndexType {
    INVID_I = 0,
    HASHINDEX,    /**< hash index     */
    BPTREEINDEX,  /**< bptree index   */
    ARTTREEINDEX, /**< art tree index */
    MAXTYPE_I
};

/** an enum for Table.  */
enum TableType {
    INVID_T = 0,
    ROWTABLE,     /**< row table    */
    COLTABLE,     /**< column table */
    MAXTYPE_T
};

/** an enum for column. */
enum ColumnType {
    INVID_C = 0,
    INT8,        /**< int8  */
    INT16,       /**< int16 */
    INT32,       /**< int32 */
    INT64,       /**< int64 */
    FLOAT32,     /**< float32 */
    FLOAT64,     /**< float64 */
    CHARN,       /**< charn, fixed length string */
    DATE,        /**< days from 1970-01-01 till current DATE */
    TIME,        /**< seconds from 00:00:00 till current TIME */
    DATETIME,    /**< seconds from 1970-01-01 00:00:00 till current DATETIME */
    MAXTYPE_C
};

#define OBJ_NAME_MAX (128)

/** definition of Object, basic element in database. */
class Object {
  private:
    int64_t o_id;                /**< object identifier */
    ObjectType o_type;           /**< object type       */
    char o_name[OBJ_NAME_MAX];   /**< object name, max length OBJ_NAME_MAX */

  public:
     /**
      * constructor.
      * @param o_id   object identifier
      * @param o_type object type
      * @param o_name object name
      */
     Object(int64_t o_id, ObjectType o_type, const char *o_name) {
        this->o_id = o_id;
        this->o_type = o_type;
        strncpy(this->o_name, o_name, OBJ_NAME_MAX - 1);
        this->o_name[OBJ_NAME_MAX - 1] = '\0';
    }
    /**
     * shut down the object.
     */
    virtual bool shut (void) { return false; }
    /**
     * print the object infomation.
     */
    virtual void print(void) {
        printf("Object- o_id: %ld o_type: %d, o_name: %s\n", o_id, o_type,
               o_name);
    }
    /**
     * get identifier of object.
     */
    int64_t getOid(void) {
        return o_id;
    }
    /**
     * get object type.
     */
    ObjectType getOtype(void) {
        return o_type;
    }
    /**
     * get object name.
     */
    char *getOname(void) {
        return o_name;
    }
    /**
     * change object name(not in use).
     */
    bool changeName(char *o_name) {
        if (strlen(o_name) >= OBJ_NAME_MAX - 1) {
            printf
                ("[Object][ERROR][changeName]: o_name exceed length! -1\n");
            return false;
        }
        strncpy(this->o_name, o_name, OBJ_NAME_MAX);
        return true;
    }
};  // class Object

/** definition of class Column.  */
class Column: public Object {
  private:
    ColumnType c_type;     /**< column type */
    int64_t c_size;        /**< column size */
    BasicType *c_datatype; /**< column data type */
    int64_t c_offset;      /**< column offset in a table, if column table the value is 0 */
  public:
    /**
     * constructor.
     * @param c_id   column identiier
     * @param c_name column name
     * @param c_type column type
     * @param c_size data type size
     */
    Column(int64_t c_id, const char *c_name, ColumnType c_type, int64_t c_size = 0)
        :Object(c_id, COLUMN,c_name)
    {
        this->c_type = c_type;
        this->c_size = c_size;
        this->c_datatype = NULL;
        this->c_offset = 0;
    }
    /**
     * destructor.
     */
    virtual ~Column(void) {
        delete c_datatype;
    }
    /**
     * get column type.
     */
    ColumnType getCType(void) {
        return c_type;
    }
    /**
     * get data dize. 
     */
    int64_t getCSize(void) {
        return c_size;
    }
    /**
     * get column offset.
     */
    int64_t getCoffset(void) {
        return c_offset;
    }
    /**
     * get column offset.
     */
    int64_t setCoffset(int64_t offset) {
        return this->c_offset = offset;
    }
    /**
     * init column.
     */
    virtual bool init(void) {
        switch (c_type) {
        case INVID_C:
            printf("[Column][ERROR][init]: invid type! -1\n");
            return false;
        case INT8:
            c_datatype = new TypeInt8();
            break;
        case INT16:
            c_datatype = new TypeInt16();
            break;
        case INT32:
            c_datatype = new TypeInt32();
            break;
        case INT64:
            c_datatype = new TypeInt64();
            break;
        case FLOAT32:
            c_datatype = new TypeFloat32();
            break;
        case FLOAT64:
            c_datatype = new TypeFloat64();
            break;
        case CHARN:
            c_datatype = new TypeCharN(c_size);
            break;
        case DATE:
            c_datatype = new TypeDate();
            break;              // days from 1970-01-01 till current DATE
        case TIME:
            c_datatype = new TypeTime();
            break;              // seconds from 00:00:00 till current TIME
        case DATETIME:
            c_datatype = new TypeDateTime();
            break;              // seconds from 1970-01-01 00:00:00 till current DATETIME
        case MAXTYPE_C:
            printf("[Column][ERROR][init]: invid type! -1\n");
            return false;
        }
        c_size     = c_datatype->getTypeSize();
        return true;
    }
    /**
     * finish column setting.
     */
    virtual bool finish(void) {
        return true;
    }
    /**
     * shut down column.
     */
    virtual bool shut(void) {
        delete c_datatype;
        return true;
    }
    /**
     * print column information
     */
    virtual void print(void) {
        printf("Column- c_id: %ld c_type: %d c_size: %ld c_name: %s c_offset: %ld\n",
               getOid(), c_type, c_size, getOname(), getCoffset());
    }
    /**
     * get data type of the column
     */
    BasicType *getDataType(void) {
        return c_datatype;
    }
    // HACK, if you want to implement COLTABLE, then add code here, now is for ROWTABLE
};  // class Column

//  Note: the table provides no key definition (primary key & foreign key)

/** definition of class Table.  */
class Table:public Object {
  private:
    TableType t_type;     /**< table type */
    std::vector < int64_t > t_columns;  /**< vector of columns' identifier */
    std::vector < int64_t > t_index;    /**< vector of index's identifier */

  public:
    /**
     * get table type.
     */
    TableType getTtype(void) {
        return t_type;
    }
    /**
     * destructor.
     */
    virtual ~Table(void) {}
    /**
     * get column identifier vector.
     * @retval vector of column identifiers
     */
    std::vector < int64_t > &getColumns(void) {
        return t_columns;
    }
    /**
     * get index identifier vector.
     */
    std::vector < int64_t > &getIndexs(void) {
        return t_index;
    }
    /**
     * get column rank in this table
     * @param  c_id column identifier
     * @retval >= 0 valid rank
     * @retval <0 invalid, not exist
     */
    int64_t getColumnRank(int64_t c_id) {
        return getRank(t_columns, c_id);
    }
    /**
     * get index rank in this table
     * @param  i_id column identifier
     * @retval >= 0 valid rank
     * @retval <0 invalid, not exist
     */
    int64_t getIndexRank(int64_t i_id) {
        return getRank(t_index, i_id);
    }
    /**
     * get rank in a vector
     * @param  vec  vector to search in
     * @param  id   object identifier
     * @retval >= 0 valid rank
     * @retval <0 invalid, not exist
     */
    int64_t getRank(std::vector < int64_t > &vec, int64_t id) {
        for (unsigned int ii = 0; ii < vec.size(); ii++)
            if (vec[ii] == id)
                return ii;
        return -1;
    }
  public:
    /**
     * constructor.
     * @param t_id   table identifier
     * @param t_name table name
     * @param t_type table type
     */
    Table(int64_t t_id, const char *t_name, TableType t_type):Object(t_id, TABLE,
           t_name)
    {
        this->t_type = t_type;
    }
    /**
     * print table information.
     */
    virtual void print(void) {
        printf("Table- t_id: %ld t_type: %d t_name: %s columns: {",
               getOid(), getTtype(), getOname());
        unsigned int ii = 0;
        for (; ii < t_columns.size() - 1; ii++) {
            printf("%ld,", t_columns[ii]);
        }
        printf("%ld} index: {", t_columns[ii]);
        if (t_index.size() > 0) {
            for (ii = 0; ii < t_index.size() - 1; ii++) {
                printf("%ld,", t_index[ii]);
            }
            printf("%ld", t_index[ii]);
        }
        printf("}\n");
    }

    // schema operating method, if you call finish, you must not call init and add Column

    /**
     * init, important interface for son class
     */
    virtual bool init(void) {
        return false;
    }
    /**
     *  add column identifier to this table.
     */
    virtual bool addColumn(int64_t column_id) {
        t_columns.push_back(column_id);
        return true;
    }
    /**
     * add index identifier to this table.
     */
    virtual bool addIndex(int64_t index_id) {
        t_index.push_back(index_id);
        return true;
    }
    /**
     * finish, important interface for son class
     */
    virtual bool finish(void) {
        return false;
    }                           // call actual storage system, to actual inilizer
    /**
     * shut, important interface for son class
     */
    virtual bool shut(void) {
        return false;
    }

    // data   operating method

    // select
    // get data by record_rank, mainly for OLAP to sacn 
    /**
     * select one column data.
     * @param  record_rank the n th row in the table storage
     * @param  column_rank the n th column in table pattern
     * @param  dest        buffer to store result
     * @retval true        success
     * @retval false       failure
     */
    virtual bool selectCol(int64_t record_rank, int64_t column_rank,
                           char *dest) {
        return false;
    }
    /**
     * select several column data.
     * @param  record_rank  the n th row in the table storage
     * @param  column_total total number of columns to select
     * @param  column_ranks array of column_rank, column_rank is the n th column in table pattern
     * @param  dest         buffer to store result
     * @retval true         success
     * @retval false        failure
     */ 
   virtual bool selectCols(int64_t record_rank, int64_t column_total,
                            int64_t * column_ranks, char *dest) {
        return false;
    }
    /**
     * select all columns' data.
     * @param  record_rank the n th row in the table storage
     * @param  dest        buffer to store result
     * @retval true        success
     * @retval false       failure
     */
    virtual bool select(int64_t record_rank, char *dest) {
        return false;
    }
    // if you know the pointer of row by index, for OLTP to copy out the data to dest
    /**
     * select one column data by pointer of a row.
     * @param  row_pointer the pointer of a row
     * @param  column_rank the n th column in table pattern
     * @param  dest        buffer to store result
     * @retval true        success
     * @retval false       failure
     */
    virtual bool selectCol(char *row_pointer, int64_t column_rank,
                           char *dest) {
        return false;
    }
    /**
     * select several column data.
     * @param  row_pointer  the pointer of a row
     * @param  column_total total number of columns to select
     * @param  column_ranks array of column_rank, column_rank is the n th column in table pattern
     * @param  dest         buffer to store result
     * @retval true         success
     * @retval false        failure
     */
    virtual bool selectCols(char *row_pointer, int64_t column_total,
                            int64_t * column_ranks, char *dest) {
        return false;
    }
    /**
     * select all columns' data.
     * @param  row_pointer the pointer of a row
     * @param  dest        buffer to store result
     * @retval true        success
     * @retval false       failure
     */
    virtual bool select(char *row_pointer, char *dest) {
        return false;
    }

    // update
    /**
     * update one column data.
     * @param  record_rank the n th row in the table storage
     * @param  column_rank the n th column in table pattern
     * @param  source      buffer to store data to change for
     * @retval true        success
     * @retval false       failure
     */
    virtual bool updateCol(int64_t record_rank, int64_t column_rank,
                           char *source) {
        return false;
    }
    /**
     * update one column data.
     * @param  row_pointer the pointer of a row
     * @param  column_rank the n th column in table pattern
     * @param  source      buffer to store data to change for
     * @retval true        success
     * @retval false       failure
     */
    virtual bool updateCol(char *row_pointer, int64_t column_rank,
                           char *source) {
        return false;
    }
    /* update several column data.
     * @param  record_rank  the n th row in the table storage
     * @param  column_total total number of columns to select
     * @param  column_ranks array of column_rank, column_rank is the n th column in table pattern
     * @param  source       buffer to store data to change for
     * @retval true         success
     * @retval false        failure
     */ 
    virtual bool updateCols(int64_t record_rank, int64_t column_total,
                            int64_t * column_ranks, char *source) {
        return false;
    }
    /**
     * update several column data.
     * @param  row_pointer  the pointer of a row
     * @param  column_total total number of columns to select
     * @param  column_ranks array of column_rank, column_rank is the n th column in table pattern
     * @param  source       buffer to store data to change for
     * @retval true         success
     * @retval false        failure
     */ 
    virtual bool updateCols(char *row_pointer, int64_t column_total,
                            int64_t * column_ranks, char *source) {
        return false;
    }
    /**
     * update several column data.
     * @param  record_rank  the n th row in the table storage
     * @param  column_total total number of columns to select
     * @param  column_ranks array of column_rank, column_rank is the n th column in table pattern
     * @param  source       array of columns' pointers, each points a column data to change for
     * @retval true         success
     * @retval false        failure
     */ 
    virtual bool updateCols(int64_t record_rank, int64_t column_total,
                            int64_t * column_ranks, char *source[]) {
        return false;
    }
    /**
     * update several column data.
     * @param  row_pointer  the pointer of a row
     * @param  column_total total number of columns to select
     * @param  column_ranks array of column_rank, column_rank is the n th column in table pattern
     * @param  source       array of columns' pointers, each points a column data to change for
     * @retval true         success
     * @retval false        failure
     */ 
    virtual bool updateCols(char *row_pointer, int64_t column_total,
                            int64_t * column_ranks, char *source[]) {
        return false;
    }

    // del
    /**
     * del a row.
     * @param  row_rank the n th record of the table
     * @retval true     success
     * @retval false    failure
     */
    virtual bool del(int64_t record_rank) {
        return false;
    }
    /**
     * del a row(not in use).
     * @param  row_pointer the pointer of a row
     * @retval true        success
     * @retval false       failure
     */
    virtual bool del(char *row_pointer) {
        return false;
    }
    /**
     * del a row(not in use).
     * @param  columns     array of the pointers in a row
     * @retval true        success
     * @retval false       failure
     */
    virtual bool del(char *columns[]) {
        return false;
    }                           // this isn't support because SQL condition

    // insert
    /**
     * insert a row.
     * @param  source buffer of a row in pattern
     * @retval true   success
     * @retval false  failure
     */
    virtual bool insert(char *source) {
        return false;
    }                           // the insert data are arranged in a buffer(source), in the order of and fixed-length pattern
    /**
     * insert a row.
     * @param  columns each element of the array pointed to a column data
     * @retval true    success
     * @retval false   failure
     */
    virtual bool insert(char *columns[]) {
        return false;
    }                           // the insert data are arranged in different space, and we know its pointer 
    /**
     * get record number.
     */
    virtual int64_t getRecordNum(void) {
        return -1;
    }
    /**
     * get record pointer.
     */
    virtual void *getRecordPtr(int64_t row_rank) {
        return NULL;
    }
    /**
     * load data(not in use).
     */
    virtual bool loadData(const char *filename) {
        return false;
    }
    /**
     * print data in table.
     */
    virtual bool printData(void) {
        return false;
    }
};  // class Table

/** definition of class Database.  */
class Database:public Object {
  private:
    std::vector < int64_t > d_table;   /**< table identifier container.  */

  public:
    /**
     * constructor.
     * @param d_id   database identifier
     * @param d_name database name
     */
    Database(int64_t d_id, const char *d_name)
        :Object(d_id, DATABASE,d_name) {
    }
    /**
     * destructor.
     */
    virtual ~Database(void) {}
    /**
     * init, important interface for son class
     */
    virtual bool init(void) {
        return false;
    }
    /**
     * add table identifier to this database.
     * @param table_id table identifier
     * @retval true success
     * @retval flase failure
     */
    virtual bool addTable(int64_t table_id) {
        d_table.push_back(table_id);
        return true;
    }
    /**
     * finish, important interface for son class
     */
    virtual bool finish(void) {
        return false;
    }                           // call actual storage system, to actual inilizer
    /**
     * shut down this database, free all memory.
     */
    virtual bool shut(void) {
        return false;
    }
    /**
     *  print database information
     */
    virtual void print(void) {
        printf("Database- d_id: %ld d_name: %s table: {", getOid(),
               getOname());
        unsigned int ii = 0;
        for (; ii < d_table.size() - 1; ii++) {
            printf("%ld,", d_table[ii]);
        }
        printf("%ld}\n", d_table[ii]);
    }
    /**
     * get table identifier container.
     */
    std::vector < int64_t > &getTables(void) {
        return d_table;
    }
    // insert
    /**
     * insert a record to this database's table.
     * @param  table_id table identifier
     * @param  source   buffer of data to insert
     * @retval true     success
     * @retval false    failure
     */
    virtual bool insert(int64_t table_id, char *source) {
        return false;
    }
    /**
     * insert a record to this database's table.
     * @param  table_id table identifier
     * @param  columns  each element of columns is a pointer to data of the column
     * @retval true     success
     * @retval false    failure
     */
    virtual bool insert(int64_t table_id, char *columns[]) {
        return false;
    }
    /**
     * load data into table in this database(not use).
     * @param  table_id table identifier
     * @param  filename data file to load
     * @retval true     success
     * @retval false    failure
     */
    virtual bool loadData(int64_t table_id, const char *filename) {
        return false;
    }
};  // class Database

/** definition of class Key.  */
class Key {
  private:
    std::vector < int64_t > key;  /**< index iidentifier container */
  public:
    /**
     * constructor.
     */
    Key(void) {
    } 
    /**
     * set key.
     * @param in_key keys(column identifiers) in vector
     */
    void set(std::vector < int64_t > &in_key) {
        for (unsigned int ii = 0; ii < in_key.size(); ii++)
            key.push_back(in_key[ii]);
    }
    /**
     * check if the key contain a column identifier.
     * @col_id column identifier.
     * @retval true   contain
     * @retval false  don't contain
     */
    bool contain(int64_t col_id) {
        for (unsigned int ii = 0; ii < key.size(); ii++) {
            if (key[ii] == col_id)
                return true;
        }
        return false;
    }
    /**
     * over write operator(=).
     */
    Key & operator=(const Key & p) {
        key.clear();
        for (unsigned int ii = 0; ii < p.key.size(); ii++)
            key.push_back(p.key[ii]);
        return *this;
    }
    /**
     * print key information.
     */
    void print(void) {
        printf("{");
        unsigned int ii = 0;
        for (; ii < key.size() - 1; ii++)
            printf("%ld,", key[ii]);
        printf("%ld}", key[ii]);
    }
    /**
     * get key data.
     */
    std::vector < int64_t > &getKey(void) {
        return key;
    }
};  // class Key

/** definition of class Index  */
class Index:public Object {
  protected:
    IndexType i_type;  /**< index type */
    Key i_key;         /**< index keys */
    int64_t i_t_id;      /**< in which table */

  public:
    /**
     * constructor.
     * @param i_id   index identifier
     * @param i_name index name
     * @param i_type index type
     * @param i_key  key of this index
     */
    Index(int64_t i_id, const char *i_name, IndexType i_type,
           Key & i_key):Object(i_id, INDEX, i_name) {
        this->i_type = i_type;
        this->i_key = i_key;
        this->i_t_id = 0;
    }
    /**
     * destructor.
     */
    virtual ~Index(void) {}
    /**
     * init index, important interface for son class
     */
    virtual bool init(void) {
        return false;
    }
    /**
     * finish index, important interface for son class
     */
    virtual bool finish(void) {
        return false;
    }
    /**
     * shut dowm the index, free memory.
     */
    virtual bool shut(void) {
        return false;
    }
    /**
     * insert an entry to Index.
     * @param  i_data char buffer to store data in pattern
     * @param  p_in   pointer of a row to make index
     * @retval true   operation success
     * @retval false  operation failure
     */
    virtual bool insert(void *i_data, void *p_in) {
        return false;
    }
    /**
     * insert an entry to Index.
     * @param  i_data each element of the array stores a pointer to column key
     * @param  p_in   pointer of a row to make index
     * @retval true   operation success
     * @retval false  operation failure
     */
    virtual bool insert(void *i_data[], void *p_in) {
        return false;
    }
    /**
     * del an entry in Index.
     * @param  i_data char buffer to store data in pattern
     * @retval true   operation success
     * @retval false  operation failure
     */
    virtual bool del(void *i_data) {
        return false;
    }
    /**
     * del an entry in BptreeIndex.
     * @param  i_data each element of the array stores a pointer to column key
     * @retval true   operation success
     * @retval false  operation failure
    */
    virtual bool del(void *i_data[]) {
        return false;
    }
    /**
     * del an entry in Index.
     * @param  i_data char buffer to store data in pattern
     * @param  p_del  address of specified row
     * @retval true   operation success
     * @retval false  operation failure
     */
    virtual bool del(void *i_data, void *p_del) {
        return false;
    }
    /**
     * del an entry in BptreeIndex.
     * @param  i_data each element of the array stores a pointer to column key
     * @param  p_del  address of specified row
     * @retval true   operation success
     * @retval false  operation failure
    */
    virtual bool del(void *i_data[], void *p_del) {
        return false;
    }
    virtual bool update(void *i_data, void *p_in) {
        return false;
    }
    virtual bool update(void *i_data[], void *p_in) {
        return false;
    }

    // the following function can pull one by one 
    /**
     * prepare for lookup/scan operation.
     * @param  i_data1 char buffer to store data in pattern
     * @param  i_data2 char buffer to store data in pattern, for lookup, i_data2=NULL
     * @param  info    pointer of an index info
     * @retval true    operation success
     * @retval false   operation failure
     */
    virtual bool set_ls(void *i_data1, void *i_data2, void *info) {
        return false;
    }
    /**
     * prepare for lookup/scan operation.
     * @param  i_data1 each element of the array stores a pointer to column key
     * @param  i_data2 each element of the array stores a pointer to column key, for lookup, i_data2=NULL
     * @param  info    pointer of an index info
     * @retval true    operation success
     * @retval false   operation failure
     */
    virtual bool set_ls(void *i_data1[], void *i_data2[], void *info) {
        return false;
    }
    /**
     * lookup nonduplicate key in Index.
     * @param  i_data  char buffer to store data in pattern
     * @param  result  return the pointer to the indexed row
     * @retval true    operation success
     * @retval false   operation failure
     */
    virtual bool lookup(void *i_data, void *&result) {
        return false;
    }
    /**
     * lookup nonduplicate key in Index.
     * @param  i_data  each element of the array stores a pointer to column key
     * @param  result  return the pointer to the indexed row
     * @retval true    operation success
     * @retval false   operation failure
     */
    virtual bool lookup(void *i_data[], void *&result) {
        return false;
    }
    /**
     * lookup duplicate key, iterate through the Index.
     * @param  i_data  char buffer to store data in pattern
     * @param  info    pointer of a BptreeInfo
     * @param  result  return the pointer to the indexed row
     * @retval true    operation success
     * @retval false   operation failure
     */
    virtual bool lookup(void *i_data, void *info, void *&result) {
        return false;
    }
    /**
     * lookup duplicate key,iterate through the Index.
     * @param  i_data  each element of the array stores a pointer to column key
     * @param  info    pointer of an index info
     * @param  result  return the pointer to the indexed row
     * @retval true    has more values
     * @retval false   no more values
     */
    virtual bool lookup(void *i_data[], void *info, void *&result) {
        return false;
    }
    /**
     * iterate on calling to scan for values.
     * @param  info    pointer of an index info
     * @param  result  return the pointer to the indexed row
     * @retval true    has more values
     * @retval false   no more values
     */
    virtual bool scan (void *info, void *&result) {
        return false;
    }
    /**
     * prepare for scan operation.
     * @param  i_left  char buffer to store data in pattern, ">="
     * @param  info    pointer of an index info
     * @retval true    has more values
     * @retval false   no more values
     */
    virtual bool scan_1(void *i_left, void *info) {
        return false;
    }
    /**
     * pepare for scan operation.
     * @param  i_left  each element of the array stores a pointer to column key, ">="
     * @param  info    pointer of an index info
     * @retval true    operation success
     * @retval false   operation failure
     */
    virtual bool scan_1(void *i_left[], void *info) {
        return false;
    }
    /**
     * iterate on calling to scan for values.
     * @param  i_right char buffer to store data in pattern, "<"
     * @param  info    pointer of an index info
     * @param  result  return the pointer to the indexed row
     * @retval true    has more values
     * @retval false   no more values
     */
    virtual bool scan_2(void *i_right, void *info, void *&result) {
        return false;
    }
    /**
     * iterate on calling to scan for values.
     * @param  i_right each element of the array stores a pointer to column key, "<"
     * @param  info    pointer of an index info
     * @param  result  return the pointer to the indexed row
     * @retval true    has more values
     * @retval false   no more values
     */
    virtual bool scan_2(void *i_right[], void *info, void *&result) {
        return false;
    }
    /**
     * encode keys.
     * @param  i_data  char buffer to store data in pattern
     * @retval int64_t index code of i_data
     */
    virtual int64_t tranToInt64(void *i_data) {
        return -1;
    }
    /**
     * encode keys.
     * @param  i_data  each element of the array stores a pointer to column key
     * @retval int64_t index code of i_data
     */
    virtual int64_t tranToInt64(void *i_data[]) {
        return -1;
    }
    /**
     * print index information
     */
    virtual void print(void) {
        printf("Index- i_id: %ld type: %d i_name: %s columns: ", getOid(),
               i_type, getOname());
        i_key.print();
        printf("\n");
    }
    /**
     * get index type
     */
    IndexType getIType(void) {
        return i_type;
    }
    /**
     * get index key
     */
    Key & getIKey(void) {
        return i_key;
    }
    /**
     *  set index in which table
     */
    virtual void setIndexTid (int64_t tid) {
        this->i_t_id = tid;
    }
    /**
     *  get index in which table
     */
    virtual int64_t getIndexTid (void) {
        return this->i_t_id;
    }
};  // class Index

#endif
