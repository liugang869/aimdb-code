/**
 * @file    rowtable.cc
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
 */

#include "rowtable.h"

bool RowTable::init(void)
{
    return true;
}

bool RowTable::finish(void)
{
/*
	std::vector<int64_t> columns = getColumns ();
	r_pattern.init (columns.size());
	for (int64_t ii= 0; ii< columns.size(); ii++) {
		Column *column = (Column*) g_catalog.getObjById (columns[ii]);
		switch (column->getCType()) {
			case INVID_C:	{	printf ("[RowTable][ERROR][finish]: column invalid! -1\n");
						r_pattern.shut();
						return false;	}
			case INT8:	{	BasicType *bt = new TypeInt8 ();
						r_pattern.addColumn (bt);
						break;	}
			case INT16:	{	BasicType *bt = new TypeInt16 ();
						r_pattern.addColumn (bt);
						break;	}
			case INT32:	{	BasicType *bt = new TypeInt32 ();
						r_pattern.addColumn (bt);
						break;	}
			case INT64:	{	BasicType *bt = new TypeInt64 ();
						r_pattern.addColumn (bt);
						break;	}
			case FLOAT32:	{	BasicType *bt = new TypeFloat32 ();
						r_pattern.addColumn (bt);
						break;	}
			case FLOAT64:	{	BasicType *bt = new TypeFloat64 ();
						r_pattern.addColumn (bt);
						break;	}
			case CHARN:	{	BasicType *bt = new TypeCharN (column->getCSize());
						r_pattern.addColumn (bt);
						break;	}
			case DATE:	{	BasicType *bt = new TypeDate ();
						r_pattern.addColumn (bt);
						break;	}
			case TIME:	{	BasicType *bt = new TypeTime ();
						r_pattern.addColumn (bt);
						break;	}
			case DATETIME:	{	BasicType *bt = new TypeDateTime ();
						r_pattern.addColumn (bt);
						break;	}
			case MAXTYPE_C:	{	printf ("[RowTable][ERROR][finish]: column maxtype! -2\n");
						r_pattern.shut();
						return false;	}
		}
	}
	BasicType *bt = new TypeCharN (1);
	r_pattern.addColumn (bt);
	r_storage.init (r_pattern.getRowSize());
	return true;
*/
    return true;
}

bool RowTable::shut(void)
{
    r_pattern.shut();
    r_storage.shut();
    return true;
}

        // data   operating method
        // select
        // get data by record_rank, mainly for OLAP to sacn 
bool RowTable::selectCol(int64_t record_rank, int64_t column_rank,
                         char *dest)
{
    char *ptr = NULL;
    bool bl = accessCol(record_rank, column_rank, ptr);
    if (bl == false)
        return false;
    return r_pattern.getColumnType(column_rank)->copy(dest,
                                                      ptr) >
        0 ? true : false;
}

bool RowTable::selectCols(int64_t record_rank, int64_t column_total,
                          int64_t * column_ranks, char *dest)
{
    char *ptr = NULL;
    bool bl = access(record_rank, ptr);
    if (bl == false)
        return false;
    return selectCols(ptr, column_total, column_ranks, dest);
}

bool RowTable::select(int64_t record_rank, char *dest)
{
    char *ptr = NULL;
    bool bl = access(record_rank, ptr);
    if (bl == false)
        return false;
    return select(ptr, dest);
}

        // if you know the pointer of row by index, for OLTP to copy out the data to dest
bool RowTable::selectCol(char *row_pointer, int64_t column_rank,
                         char *dest)
{
    return r_pattern.getColumnType(column_rank)->copy(dest,
                                                      row_pointer +
                                                      r_pattern.
                                                      getColumnOffset
                                                      (column_rank)) >
        0 ? true : false;
}

bool RowTable::selectCols(char *row_pointer, int64_t column_total,
                          int64_t * column_ranks, char *dest)
{
    for (int64_t ii = 0, pos = 0; ii < column_total; ii++) {
        int64_t of = r_pattern.getColumnOffset(column_ranks[ii]);
        pos +=
            r_pattern.getColumnType(column_ranks[ii])->copy(dest + pos,
                                                            row_pointer +
                                                            of);
    }
    return true;
}

bool RowTable::select(char *row_pointer, char *dest)
{
    int64_t num = getColumns().size();
    for (int64_t ii = 0, pos = 0; ii < num; ii++)
        pos +=
            r_pattern.getColumnType(ii)->copy(dest + pos,
                                              row_pointer + pos);
    return true;
}

        // del
bool RowTable::del(int64_t record_rank)
{
    char *ptr = NULL;
    bool bl = access(record_rank, ptr);
    if (bl == false)
        return false;
    return del(ptr);
}

bool RowTable::del(char *row_pointer)
{
    return invalid(row_pointer);
}

bool RowTable::access(int64_t record_rank, char *&pointer)
{
    pointer = r_storage.getRow(record_rank);
    return isValid(pointer);
}

bool RowTable::accessCol(int64_t record_rank, int64_t column_rank,
                         char *&pointer)
{
    bool bl = access(record_rank, pointer);
    if (bl == false)
        return false;
    int64_t of = r_pattern.getColumnOffset(column_rank);
    if (of < 0)
        return false;
    pointer += of;
    return true;
}

        // update
bool RowTable::updateCol(char *row_pointer, int64_t column_rank,
                         char *source)
{
    int64_t of = r_pattern.getColumnOffset(column_rank);
    if (of < 0)
        return false;
    return r_pattern.getColumnType(column_rank)->copy(row_pointer + of,
                                                      source) >
        0 ? true : false;
}

bool RowTable::updateCol(int64_t record_rank, int64_t column_rank,
                         char *source)
{
    char *ptr = NULL;
    bool bl = access(record_rank, ptr);
    if (bl == false)
        return false;
    return updateCol(ptr, column_rank, source);
}

bool RowTable::updateCols(int64_t record_rank, int64_t column_total,
                          int64_t * column_ranks, char *source)
{
    char *ptr = NULL;
    bool bl = access(record_rank, ptr);
    if (bl == false)
        return false;
    return updateCols(ptr, column_total, column_ranks, source);
}

bool RowTable::updateCols(char *row_pointer, int64_t column_total,
                          int64_t * column_ranks, char *source)
{
    for (int64_t ii = 0, pos = 0; ii < column_total; ii++) {
        int64_t of = r_pattern.getColumnOffset(column_ranks[ii]);
        pos +=
            r_pattern.getColumnType(column_ranks[ii])->copy(row_pointer +
                                                            of,
                                                            source + pos);
    }
    return true;
}

bool RowTable::updateCols(int64_t record_rank, int64_t column_total,
                          int64_t * column_ranks, char *source[])
{
    char *ptr = NULL;
    bool bl = access(record_rank, ptr);
    if (bl == false)
        return false;
    return updateCols(ptr, column_total, column_ranks, source);
}

bool RowTable::updateCols(char *row_pointer, int64_t column_total,
                          int64_t * column_ranks, char *source[])
{
    for (int64_t ii = 0; ii < column_total; ii++) {
        int64_t of = r_pattern.getColumnOffset(column_ranks[ii]);
        r_pattern.getColumnType(column_ranks[ii])->copy(row_pointer + of,
                                                        source[ii]);
    }
    return true;
}

        // insert
bool RowTable::insert(char *source)
{
    char *ptr = NULL;
    int64_t rec_id = r_storage.allocRow(ptr);
    if (rec_id< 0) {
        printf ("[RowTable][ERROR][insert]: allocRow error!\n");
        return false;
    }
    for (unsigned int ii = 0; ii < getColumns().size(); ii++) {
        int64_t of = r_pattern.getColumnOffset(ii);
        r_pattern.getColumnType(ii)->copy(ptr + of, source + of);
    }
    ptr[r_pattern.getRowSize() - 1] = 'Y';
    return true;
}

bool RowTable::insert(char *columns[])
{
    char *ptr = NULL;
    int64_t rec_id = r_storage.allocRow(ptr);
    if (rec_id< 0) {
        printf ("[RowTable][ERROR][insert]: allocRow error!\n");
        return false;
    }
    for (unsigned int ii = 0; ii < getColumns().size(); ii++) {
        int64_t of = r_pattern.getColumnOffset(ii);
        r_pattern.getColumnType(ii)->copy(ptr + of, columns[ii]);
    }
    ptr[r_pattern.getRowSize() - 1] = 'Y';
    return true;
}

bool RowTable::printData(void)
{
    int64_t num = r_storage.getRecordNum();
    printf("rowtable data:\n");
    for (int64_t ii = 0; ii < num; ii++) {
        char *p = r_storage.getRow(ii);
        r_pattern.print(p);
        printf("\n");
    }
    return true;
}

bool RowTable::loadData(const char *filename)
{
    // for file, each row represents a record, each column is split by One tab; date/time/datetime has the standard format ISO-8601
    return false;
}
