#include "global.h"
#include "executor.h"

#include <stdio.h>
#include <stdlib.h>
#include <vector>

const char *file_schema = "example_schema.txt";
const char *file_data[] = {
 				"example_table_1.data",
				"example_table_2.data"
 			  };
const char *table_name[]= {
				"example_table_1",
				"example_table_2"
			  };

int load_schema (const char *filename);
int load_data   (const char *filename[],const char *tablename[], int number);
int test        (void);

int main (void) {

	if (global_init ()) return -1;
	if (load_schema (file_schema)) return -2;
	if (load_data   (file_data,table_name, 2)) return -3;
	
	///< here start to test your code by call executor
	test ();

	global_shut ();
	return 0;

}

int test        (void) {
	// here we run some test with random data
	Executor executor;
	// .........
	
	return 0;
}

///<----------------------------------------------------------------------------
/**
 *	schema format
 *
 *	(1) split by one '\t', a row ends with '\n'
 *	(2) claim Database,Table,column,index in order
 *	(3) no empty row
**/
int load_schema (const char *filename) {
	int64_t		 cur_db_id = -1;
	int64_t		 cur_tb_id;
	int64_t		 cur_col_id;
	int64_t		 cur_ix_id;
	Database	*cur_db_ptr = NULL;
	Table		*cur_tb_ptr = NULL;
	Column		*cur_col_ptr= NULL;
	FILE		*fp = fopen (filename, "r");
	if (fp== NULL) {
		printf ("[load_schema][ERROR]: filename error!\n");
		return -1;
	}
	char buffer[1024];
	while (fgets(buffer, 1024, fp)) {
		int pos = 0, num = 0;
		char        *row[16];
		char	    *ptr = NULL;
		while (buffer[pos]=='\t') pos ++;
		ptr = buffer+ pos; 
		while (num< 16) {
			while (buffer[pos]!='\t' && buffer[pos]!='\n') pos ++;
			row   [num++] =  ptr;
			ptr           = buffer+pos+1;
			if (buffer[pos]=='\n') {
				buffer [pos] = '\0';
				break;
			}
			buffer [pos]   = '\0';
			pos   ++;
		}
		/*
		///  debug
		for (int ii=0; ii< num; ii++)
			printf ("%s\t", row[ii]);
		printf ("\n");
		*/
		if (num>= 16) {
			printf ("[load_schema][ERROR]: row with too many field!\n");
			return -2;
		}
		if (!strcmp (row[0], "DATABASE")) {
			if (cur_db_id!= -1) 
				g_catalog.initDatabase(cur_db_id);
			g_catalog.createDatabase ((const char *)row[1], cur_db_id);
			cur_db_ptr = (Database* )g_catalog.getObjById  (cur_db_id);
		}
		else if (!strcmp (row[0], "TABLE")) {
			TableType type;
			if (!strcmp (row[2], "ROWTABLE")) type = ROWTABLE;
			else if (!strcmp (row[2], "COLTABLE")) type = COLTABLE;
			else {
				printf ("[load_schema][ERROR]: table type error!\n");
				return -4;
			}
			g_catalog.createTable ((const char *)row[1], type, cur_tb_id);
			cur_tb_ptr = (Table*) g_catalog.getObjById (cur_tb_id);
			cur_db_ptr->addTable  (cur_tb_id);
		}
		else if (!strcmp (row[0], "COLUMN")) {
			ColumnType   type;
			int64_t     len=0;
			if (!strcmp (row[2], "INT8"))          type =  INT8;
			else if (!strcmp (row[2], "INT16"))    type =  INT16;
			else if (!strcmp (row[2], "INT32"))    type =  INT32;
			else if (!strcmp (row[2], "INT64"))    type =  INT64;
			else if (!strcmp (row[2], "FLOAT32"))  type =  FLOAT32;
			else if (!strcmp (row[2], "FLOAT64"))  type =  FLOAT64;
			else if (!strcmp (row[2], "DATE"))     type =  DATE;
			else if (!strcmp (row[2], "TIME"))     type =  TIME;
			else if (!strcmp (row[2], "DATETIME")) type =  DATETIME;
			else if (!strcmp (row[2], "CHARN"))  { 
				type =  CHARN;
				len  =  atoi (row[3]);
			}
			else {
				printf ("[load_schema][ERROR]: column type error!\n");
				return -5;
			}
			g_catalog.createColumn ((const char *)row[1], type, len,  cur_col_id);
			cur_tb_ptr->addColumn  (cur_col_id);
		}
		else if (!strcmp (row[0], "INDEX")) {
			IndexType   type;
			std::vector<int64_t> cols;
			if (!strcmp (row[2], "HASHINDEX"))        type =  HASHINDEX;
			else if (!strcmp (row[2], "BPTREEINDEX")) type =  BPTREEINDEX;
			for (int ii= 3; ii< num; ii++) {
				int64_t oid = g_catalog.getObjByName (row[ii])->getOid();
				cols.push_back (oid);
			}
			Key key;key.set (cols);
			g_catalog.createIndex (row[1], type, key, cur_ix_id);
			cur_tb_ptr->addIndex (cur_ix_id);
		}
		else {
			printf ("[load_schema][ERROR]: o_type error!\n");
			return -3;
		}
	}
	g_catalog.initDatabase(cur_db_id);
	g_catalog.print ();
	return 0;
}
/**
 *	data format
 *
 *	(1) split by one '\t', a row ends with '\n'
 *	(2) no empty row
**/
int load_data   (const char *filename[], const char *tablename[], int number) {
	for (int ii= 0; ii< number; ii++) {
		FILE *fp = fopen (filename[ii], "r");
		if (fp == NULL) {
			printf ("[load_data][ERROR]: filename error!\n");
			return -1;
		}
		Table *tp = (Table*)g_catalog.getObjByName((char*)tablename[ii]);
		if (tp == NULL) {
			printf ("[load_data][ERROR]: tablename error!\n");
			return -2;
		}
		int colnum = tp->getColumns().size();
		BasicType *dtype[colnum];
		for (int ii= 0; ii< colnum; ii++) 
			dtype[ii] = ((Column*)g_catalog.getObjById(tp->getColumns()[ii]))->getDataType();
		int indexnum = tp->getIndexs().size();
		Index      *index[indexnum];
		for (int ii= 0; ii< indexnum; ii++) 
			index[ii] = (Index*) g_catalog.getObjById (tp->getIndexs()[ii]);	
		
		char    buffer [2048];
		char   *columns[colnum];
		char    data   [colnum][1024];
		while (fgets(buffer, 2048, fp)) {
			// insert table
			columns[0] = buffer;
			int64_t pos =  0;
			for (int64_t ii= 1; ii< colnum; ii++) {
				for (int64_t jj= 0; jj< 2048; jj++) {
					if (buffer[pos]== '\t') {
						buffer[pos] = '\0';
						columns[ii] = buffer+ pos+ 1;
						pos ++;
						break;
					}
					else pos ++;
				}
			}
			/*
			// debug
			for (int ii= 0; ii< colnum; ii++) {
				printf ("%s\t", columns[ii]);
			}
			printf ("\n");
			*/
			while (buffer[pos]!='\n') pos++;
			buffer[pos] = '\0';
			for (int64_t ii= 0; ii< colnum; ii++) {
				BasicType *p = dtype[ii];
				p->formatbin (data[ii], columns[ii]);
				columns[ii] = data[ii];
			}
			tp->insert (columns);
			void *ptr = tp->getRecordPtr(tp->getRecordNum()-1);
			// insert index
			for (int ii= 0; ii< indexnum; ii++) {
				int indexsz = index[ii]->getIKey().getKey().size();
				for (int jj= 0; jj< indexsz; jj++) 
					columns[jj] = data[tp->getColumnRank(index[ii]->getIKey().getKey()[jj])];
				index[ii]->insert (columns, ptr);
			}
		}
		tp->printData();
	}
	return 0;
}
