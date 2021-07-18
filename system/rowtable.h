/**
 * @file    rowtable.h
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *
 *  rowtable implementation, this file implement all interface required by class table
 *  data space is managed by g_memory, which decreases malloc overhead
 *  when create rowtable, I will make one more column to represent its validation.
 *  if delete a record, put down the label to set it "invalid"
 *  for index in this table, delete the entry inside index
 *  
 * basic usage:
 * using rowtable interface surrounded by "//----------" will be enough for you
 *
 */

#ifndef _ROWTABLE_H
#define _ROWTABLE_H

#include "mymemory.h"
#include "schema.h"

extern Memory g_memory;

/** definition of class RPattern, describe row struture. */
class RPattern {
  private:
    int64_t rp_colnum;    /**< total columns */
    int64_t *rp_offset;   /**< offset of column in a row */
    BasicType **rp_dtype; /**< array of pointers to each column's data type */
    char *rp_memory;      /**< memory to store rp_offfset and rp_datatype */
    int64_t rp_mem_sz;    /**< memory size */
    int64_t rp_current;   /**< currrent columns already set offset and datatype */
    int64_t rp_row_sz;    /**< size of a row record */
    char par[128 - 3 * sizeof(void *) - 4 * sizeof(int64_t)]; /**< make the sizeof RPattern 128, managed by g_memory */

  public:
     /**
      * init, alloc memory and initial setting.
      * @param  col_num number of columns, from class Table
      * @retval true    success
      * @retval false   failure
      */
     bool init(int64_t col_num) {
        rp_row_sz = 0L;
        rp_current = 0L;
        rp_colnum = col_num;
        int64_t alloc_size =
            rp_colnum * (sizeof(BasicType *) + sizeof(int64_t));
        for (rp_mem_sz = 8L; rp_mem_sz < alloc_size;
             rp_mem_sz = rp_mem_sz << 1);
         alloc_size = g_memory.alloc(rp_memory, rp_mem_sz);
        if (alloc_size != rp_mem_sz) {
            printf("[RPattern][ERROR][init]: alloc memory error! -1\n");
            return false;
        }
        rp_offset = (int64_t *) rp_memory;
        rp_dtype = (BasicType **) (rp_memory + rp_mem_sz / 2);
        return true;
    }
    /**
     * add column infomation.
     * @param col_type data type of the column
     */
    bool addColumn(BasicType * col_type) {
        if (rp_current >= rp_colnum)
            return false;
        rp_dtype[rp_current] = col_type;
        rp_offset[rp_current] = rp_row_sz;
        rp_row_sz += col_type->getTypeSize();
        rp_current++;
        return true;
    }
    /**
     * get offset of column in a row record.
     * @param  col_rank the n th column in the table
     * @retval >=0      valid offset
     * @retval ==-1     input error
     */
    int64_t getColumnOffset(int64_t col_rank) {
        return col_rank < rp_colnum ? rp_offset[col_rank] : -1;
    }
    /**
     * get data type of column.
     * @param  col_rank the n th column in the table
     * @retval != NULL  valid pointer
     * @retval == NULL  input error
     */
    BasicType *getColumnType(int64_t col_rank) {
        return col_rank < rp_colnum ? rp_dtype[col_rank] : NULL;
    }
    /**
     * reset if addcolumn error happen.
     */
    void reset(void) {
        rp_row_sz = 0L;
        rp_current = 0L;
    }
    /**
     * shut down, free memory allocated from g_memory.
     */
    void shut(void) {
        g_memory.free(rp_memory, rp_mem_sz);
    }
    /**
     * get size of a row record.
     * @retval the size of a row record
     */
    int64_t getRowSize(void) {
        return rp_row_sz;
    }
    /**
     * print a row following this pattern.
     * @param  r_ptr         pointer of a row
     * @retval ==rp_row_size success
     * @retval !=rp_row_size error
     */
    int64_t print(char *r_ptr) {
	int64_t sz = 0;
        for (int64_t ii = 0; ii < rp_colnum - 1; ii++) {
            char buf[1024];
            sz += rp_dtype[ii]->formatTxt(buf, r_ptr + rp_offset[ii]);
            printf("%s\t", buf);
        }
        printf("%c", *(r_ptr + rp_row_sz - 1));  // label of validation
	return sz;
    }
    
};  // class RPattern

/** definition of MStorage, table storage manager.  */
class MStorage {

  private:
    int64_t  ms_record_size;   /**< size per record */
    int64_t  ms_record_num;    /**< current record used */
    int64_t  ms_record_max;    /**< max records with actual mmaped memory */
    char    *ms_memory;        /**< memory address */
    char    *ms_memory_cur;    /**< curent memory address */
    int64_t  ms_memory_size;   /**< memory size actually mmaped */
    char     pad[128-4*sizeof(int64_t)-sizeof(void *)]; /** mkae sizeof(MStorage)==128, managed by g_memory  */
  public:
    /**
     * init, allocate memory and initial setting.
     * @param  record_size   size of a row record
     * @retval true          success
     * @retval false         failure
     */
    bool init(int64_t record_size) {
        ms_record_num  = 0L;
        ms_memory_size = 0L;
        ms_record_max  = 0L;
        ms_record_size = record_size;
        if(g_memory.allocTableAddr (ms_memory)) {
            printf ("[Mstorage][ERROR][init]: allocTableAddr error!\n");
            return false;
        }
        ms_memory_cur = ms_memory;
        return true;
    }
    /**
     * alloc an empty row.
     * @param  pointer reference of pointer result
     * @retval >=0     row rank in all table
     * @retval <0      failure
     */
    int64_t allocRow(char *&pointer) {
        if (ms_record_num >= ms_record_max) {
            if (!expand()) {
                printf ("[Mstorage][ERROR][allowRow]: expend error! -172\n");
                return -1;
            }
        }
        pointer = ms_memory_cur;
        ms_memory_cur += ms_record_size;
        return ms_record_num++;
    }
    /**
     * get the pointer of a row specified by record_rank.
     * @param  record_rank the n th row in the table
     * @retval !=NULL      valid
     * @retval ==NULL      param error
     */
    char *getRow(int64_t record_rank) {
        if (record_rank >= ms_record_num) {
            printf("[MStorage][ERROR][getRow]: record_rank exceed! -4\n");
            return NULL;
        }
        return ms_memory + record_rank*ms_record_size;
    }
    /**
     * shut down, free memory to system.
     */
    void shut(void) {
        for (char *mm= ms_memory; mm< ms_memory_cur; mm+= TABLE_MEMORY_ALLOC_INC) {
            if (munmap (mm, TABLE_MEMORY_ALLOC_INC)) {
                printf("[MStorage][ERROR][shut]: munmap non-table error!-50\n");
            }
        }
    }
    /**
     * get the last record rank till now.
     */
    int64_t getRecordNum(void) {
        return ms_record_num;
    }
  private:
    /**
     * expand slots for more storage avaliable for this table.
     * @retval true  success
     * @retval false lack memory
     */
    bool expand(void) {
        if (ms_memory_size > TABLE_MEMORY_ALLOC_MAX) {
            printf("[Mstorage][ERROR][expend]: exceed maximux mmap fail! -218\n");
            return false;
        }
        void* addr = mmap (ms_memory+ms_memory_size, TABLE_MEMORY_ALLOC_INC, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_FIXED, -1, 0);
        if (addr == MAP_FAILED) {
            printf("[Mstorage][ERROR][expend]: mmap fail! \n");
            return false;
        }
        ms_memory_size += TABLE_MEMORY_ALLOC_INC;
        ms_record_max = ms_memory_size / ms_record_size;
        return true;
    }
};  // class MStorage

/** definition of class RowTable.  */
class RowTable:public Table {
  private:
    RPattern r_pattern;  /**< pattern of row  */
    MStorage r_storage;  /**< storage of table  */
  public:
    /**
     * constructor.
     * @param r_id   table ideitifer
     * @param r_name table name
     */
    RowTable(int64_t r_id, const char *r_name)
        :Table(r_id, r_name,ROWTABLE) {
    }

    // schema operating method, if you call finish, you must not call init and add Column

    /**
     * init, leave it empty.
     */
    bool init(void);
    /**
     * finish, leave it empty.
     */
    bool finish(void);
    /**
     * shut down r_pattern and r_storage, free their memory.
     */
    bool shut(void);

    // data   operating method
    //----------------------------------------------------------------------------------------------------------------------
    // select
    // get data by record_rank, mainly for OLAP to scan
    /**
     * select one column data.
     * @param  record_rank the n th row in the table storage
     * @param  column_rank the n th column in table pattern
     * @param  dest        buffer to store result
     * @retval true        success
     * @retval false       failure
     */ 
    bool selectCol(int64_t record_rank, int64_t column_rank, char *dest);
    /**
     * select several column data.
     * @param  record_rank  the n th row in the table storage
     * @param  column_total total number of columns to select
     * @param  column_ranks array of column_rank, column_rank is the n th column in table pattern
     * @param  dest         buffer to store result
     * @retval true         success
     * @retval false        failure
     */ 
    bool selectCols(int64_t record_rank, int64_t column_total,
                    int64_t * column_ranks, char *dest);
    /**
     * select all columns' data.
     * @param  record_rank the n th row in the table storage
     * @param  dest        buffer to store result
     * @retval true        success
     * @retval false       failure
     */
    bool select(int64_t record_rank, char *dest);

    // if you know the pointer of row by index, for OLTP to copy out the data to dest
    /**
     * select one column data by pointer of a row.
     * @param  row_pointer the pointer of a row
     * @param  column_rank the n th column in table pattern
     * @param  dest        buffer to store result
     * @retval true        success
     * @retval false       failure
     */
    bool selectCol(char *row_pointer, int64_t column_rank, char *dest);
    /**
     * select several column data.
     * @param  row_pointer  the pointer of a row
     * @param  column_total total number of columns to select
     * @param  column_ranks array of column_rank, column_rank is the n th column in table pattern
     * @param  dest         buffer to store result
     * @retval true         success
     * @retval false        failure
     */
    bool selectCols(char *row_pointer, int64_t column_total,
                    int64_t * column_ranks, char *dest);
    /**
     * select all columns' data.
     * @param  row_pointer the pointer of a row
     * @param  dest        buffer to store result
     * @retval true        success
     * @retval false       failure
     */
    bool select(char *row_pointer, char *dest);

    // update
    /**
     * update a column data.
     * @param  row_pointer the pointer of a row
     * @param  column_rank the n th column in table pattern
     * @param  source      buffer to store data to change for
     * @retval true        success
     * @retval false       failure
     */
    bool updateCol(char *row_pointer, int64_t column_rank, char *source);
    /**
     * update a column data.
     * @param  record_rank the n th row in the table storage
     * @param  column_rank the n th column in table pattern
     * @param  source      buffer to store data to change for
     * @retval true        success
     * @retval false       failure
     */
    bool updateCol(int64_t record_rank, int64_t column_rank, char *source);
     /**
     * update several column data.
     * @param  record_rank  the n th row in the table storage
     * @param  column_total total number of columns to select
     * @param  column_ranks array of column_rank, column_rank is the n th column in table pattern
     * @param  source       buffer to store data to change for
     * @retval true         success
     * @retval false        failure
     */ 
    bool updateCols(int64_t record_rank, int64_t column_total,
                    int64_t * column_ranks, char *source);
    /**
     * update several column data.
     * @param  row_pointer  the pointer of a row
     * @param  column_total total number of columns to select
     * @param  column_ranks array of column_rank, column_rank is the n th column in table pattern
     * @param  source       buffer to store data to change for
     * @retval true         success
     * @retval false        failure
     */ 
    bool updateCols(char *row_pointer, int64_t column_total,
                    int64_t * column_ranks, char *source);
    /**
     * update several column data.
     * @param  record_rank  the n th row in the table storage
     * @param  column_total total number of columns to select
     * @param  column_ranks array of column_rank, column_rank is the n th column in table pattern
     * @param  source       array of columns' pointers, each points a column data to change for
     * @retval true         success
     * @retval false        failure
     */ 
    bool updateCols(int64_t record_rank, int64_t column_total,
                    int64_t * column_ranks, char *source[]);
    /**
     * update several column data.
     * @param  row_pointer  the pointer of a row
     * @param  column_total total number of columns to select
     * @param  column_ranks array of column_rank, column_rank is the n th column in table pattern
     * @param  source       array of columns' pointers, each points a column data to change for
     * @retval true         success
     * @retval false        failure
     */ 
    bool updateCols(char *row_pointer, int64_t column_total,
                    int64_t * column_ranks, char *source[]);

    // del

    /**
     * del a row.
     * @param  row_rank the n th record of the table
     * @retval true     success
     * @retval false    failure
     */
    bool del(int64_t record_rank);
    /**
     * del a row(not in use).
     * @param  row_pointer the pointer of a row
     * @retval true        success
     * @retval false       failure
     */
    bool del(char *row_pointer);

    // insert

    /**
     * insert a row.
     * @param  source buffer of a row in pattern
     * @retval true   success
     * @retval false  failure
     */
    bool insert(char *source);
    /**
     * insert a row.
     * @param  columns each element of the array pointed to a column data
     * @retval true    success
     * @retval false   failure
     */
    bool insert(char *columns[]);
    //------------------------------------------------------------------------------------------------------------------------

    /**
     * print table data, for debug.
     */
    bool printData(void);
    /**
     * load data of the table(not in use).
     */
    bool loadData(const char *filename);
    /**
     * get pattern of table.
     */
    RPattern & getRPattern(void) {
        return r_pattern;
    }
    /**
     * get storage of table.
     */
    MStorage & getMStorage(void) {
        return r_storage;
    }
    /**
     * get the last record rank.
     */
    int64_t getRecordNum(void) {
        return r_storage.getRecordNum();
    }
    /**
     * get row record pointer.
     * @param  row_rank the n th record in the table
     * @retval !=NULL   success
     * @retval ==NULL   failure
     */
    void *getRecordPtr(int64_t row_rank) {
        char *ptr = NULL;
        return access(row_rank, ptr) ? ptr : NULL;
    }

  private:
    /**
     * get a row record pointer.
     * @param  record_rank the n th record in the table
     * @param  pointer     result pointer to return
     * @retval true        success
     * @retval false       failure
     */
    bool access(int64_t record_rank, char *&pointer);
    /**
     * get a column of record pointer.
     * @param  record_rank the n th record in the table
     * @param  column_rank the n th cilumn in the pattern
     * @param  pointer     result pointer to return
     * @retval true        success
     * @retval false       failure
     */
    bool accessCol(int64_t record_rank, int64_t column_rank,
                   char *&pointer);
    /**
     * get result,whether the record is valid or not
     * @param  record_ptr the pointer of a record
     * @retval true       valid 
     * @retval false      invalid
     */
    bool isValid(char *record_ptr) {
        return *(record_ptr + r_pattern.getRowSize() - 1) ==
            'Y' ? true : false;
    }
    /**
     * make the record invalid when del the record
     * @param  record_ptr the pointer of a record
     * @retval true       success
     * @retval false      failure
     */
    bool invalid(char *record_ptr) {
        if (*(record_ptr + r_pattern.getRowSize() - 1) == 'N')
            return false;
        *(record_ptr + r_pattern.getRowSize() - 1) = 'N';
        return true;
    }
};  // class RowTable

#endif
