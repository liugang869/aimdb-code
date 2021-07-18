/**
 *
 * @file debug_runtpcc.cc
 * @author liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION  
 *
 * optimal performance of tpcc benchmark
 *
 */

#include <stdio.h>
#include <sys/time.h>
#include "global.h"

//=================================================================================
//                      1. tpcc structures and functions
//=================================================================================

#define I_NAME_LEN   (24)
#define O_OL_CNT_MAX (15)

#define DELIVERY (0)
#define NEW_ORDER (1)
#define ORDER_STATUS (2)
#define PAYMENT (3)
#define STOCK_LEVEL (4)
#define INTEGRITY (10)
#define C_LAST_LEN (16)

struct input_delivery_t {
	/* Input data.  */
	int w_id;
	int o_carrier_id;
};

struct input_os_order_line_t
{
        int ol_i_id;
        int ol_supply_w_id;
        int ol_quantity;
};

struct no_order_line_t
{
	int ol_i_id;
	int ol_supply_w_id;
	int ol_quantity;
	double ol_amount;
	int s_quantity;
	char brand_generic;
	char i_name[I_NAME_LEN + 1];
	double i_price;
	void print () {
		printf ("ol_i_id: %d ol_supply_w_id: %d ol_quantity: %d\n",
		ol_i_id,ol_supply_w_id,ol_quantity);
	}  
};

struct input_new_order_t {
        /* Input data. */
    int w_id;
    int d_id;
    int c_id;
    int o_ol_cnt;
    int o_all_local;
    struct no_order_line_t order_line[O_OL_CNT_MAX];
};

struct input_payment_t
{
        /* Input data. */
    int c_d_id;
    int c_w_id;
    int d_id;
    double h_amount;
    int w_id;

    /* Input and output data. */
    int c_id;
    char c_last[C_LAST_LEN + 1];
};

struct input_order_status_t
{
        /* Input data. */
        int c_d_id;
        int c_w_id;

        /* Input and output data. */
        int c_id;
        char c_last[C_LAST_LEN + 1];
};

struct input_stock_level_t
{
        /* Input data. */
        int w_id;
        int d_id;
        int threshold;
};

struct input_integrity_t
{
        /* Input data. */
        int w_id;
};

#define DISTRICT_PER_WAREHOUSE  (10)
#define CUSTOMER_PER_DISTRICT (3000)
#define MAXIMUM_ITEM_NUMBER (100000)
#define ORDER_PER_DISTRICT  (100000)

int64_t getCustomerKey(char *row_customer) {
    int64_t key = *(int64_t*)row_customer -1;
    key += (*(int64_t*)(row_customer+sizeof(int64_t))-1)*CUSTOMER_PER_DISTRICT;
    key += (*(int64_t*)(row_customer+2*sizeof(int64_t))-1)*CUSTOMER_PER_DISTRICT*DISTRICT_PER_WAREHOUSE;
    return key;
}
int64_t hash(char *str, int64_t maxlen)
{
    int64_t hash = 5381L;
    int64_t c, ii = 0L;
    while (ii < maxlen && (c = *(unsigned char *) (str + ii))) {
        hash = ((hash << 5) + hash) + c;   /* hash * 33 + c */
        ii++;
    }
    return hash;
}
int64_t getCustomerKey2(char *row_customer) {
    int64_t key = hash (row_customer+42,16); // last name at offset 42 ? check
    key = ((0x1L<<32)-1) & key;
    key += (*(int64_t*)(row_customer+sizeof(int64_t))-1)*CUSTOMER_PER_DISTRICT;
    key += (*(int64_t*)(row_customer+2*sizeof(int64_t))-1)*CUSTOMER_PER_DISTRICT*DISTRICT_PER_WAREHOUSE;
    return key;
}
int64_t getDistrictKey(char *row_district) {
    int64_t key = *(int64_t*)row_district -1;
    key += (*(int64_t*)(row_district+sizeof(int64_t))-1)*DISTRICT_PER_WAREHOUSE;
    return key;
}
int64_t getItemKey(char *row_item) {
    return (*(int64_t*)row_item)-1;
}
int64_t getStockKey(char *row_stock) {
    return (*(int64_t*)row_stock)-1+ (*(int64_t*)(row_stock+sizeof(int64_t))-1)*MAXIMUM_ITEM_NUMBER;
}
int64_t getWarehouseKey(char *row_warehouse) {
    return (*(int64_t*)row_warehouse)-1;
}

int64_t getNewOrderKey (char *row_neworder) {
    return getDistrictKey (row_neworder);
}
int64_t getOrderLineKey (char *row_orderline) {
    int64_t key = *(int64_t*)row_orderline-1;
    key += (*(int64_t*)(row_orderline+sizeof(int64_t))-1)*ORDER_PER_DISTRICT;
    key += (*(int64_t*)(row_orderline+2*sizeof(int64_t))-1)*ORDER_PER_DISTRICT*DISTRICT_PER_WAREHOUSE;
    return key;
}
int64_t getOrderKey (char *row_order) {   // o_id,o_d_id,o_w_id
    return getOrderLineKey (row_order);
}
int64_t getOrderKey2 (char *row_order) {  // o_d_id,o_w_id,o_c_id
    int64_t key = *(int64_t*)(row_order+2*sizeof(int64_t))-1;
    key += getDistrictKey(row_order)*CUSTOMER_PER_DISTRICT;
    return key;
}

//=================================================================================
//                      2. load tpcc transactions into arrays
//=================================================================================

int loadTransactions(const char *trace_name, int trans_num, int whichs, struct input_new_order_t *&neworders, 
                     struct input_payment_t* &payments, struct input_delivery_t *&deliverys, 
                     struct input_order_status_t*& orderstatuss,struct input_stock_level_t *&stocklevels,int *&choice) 
{
    // whichs 11111b 5 types all, a bitmap
    FILE *fp = fopen (trace_name, "r");
    if (fp == NULL) {
        printf ("[main][loadTransactionsNP][ERROR]: file name error! -1\n");
        return -1;
    }
    char tran_buffer[1024];
    int load_number = 0;
    int neworders_number=0, payments_number=0,
        deliverys_number=0, orderstatuss_number=0,stocklevels_number=0;
    neworders = new struct input_new_order_t[trans_num];
    payments = new struct input_payment_t[trans_num];
    deliverys = new struct input_delivery_t[trans_num];
    orderstatuss = new struct input_order_status_t[trans_num];
    stocklevels = new struct input_stock_level_t[trans_num];
    choice = new int[trans_num];
    while ((fread (tran_buffer,1024,1,fp)) > 0) {
        if ((whichs&16) && (*(int*)tran_buffer == NEW_ORDER+100)) {
            //if (neworders == NULL) continue;
            memcpy(&neworders[neworders_number], tran_buffer+sizeof(int), sizeof(struct input_new_order_t));
            neworders_number++;
            choice[load_number++] = NEW_ORDER;
            if (load_number >= trans_num) {
                fclose (fp);
                break;
            }
        }
        else if ((whichs&8) && (*(int*)tran_buffer == PAYMENT+100)) {
            //if (payments == NULL) continue;
            memcpy(&payments[payments_number], tran_buffer+sizeof(int), sizeof(struct input_payment_t));
            payments_number ++;
            choice[load_number++] = PAYMENT;
            if (load_number >= trans_num) {
                fclose (fp);
                break;
            }
        }
        else if ((whichs&4) && (*(int*)tran_buffer == DELIVERY+100)) {
            //if (deliverys == NULL) continue;
            memcpy(&deliverys[deliverys_number], tran_buffer+sizeof(int), sizeof(struct input_delivery_t));
            deliverys_number ++;
            choice[load_number++] = DELIVERY;
            if (load_number >= trans_num) {
                fclose (fp);
                break;
            }
        }
        else if ((whichs&2) && (*(int*)tran_buffer == ORDER_STATUS+100)) {
            //if (orderstatuss == NULL) continue;
            memcpy(&orderstatuss[orderstatuss_number], tran_buffer+sizeof(int), sizeof(struct input_order_status_t));
            orderstatuss_number ++;
            choice[load_number++] = ORDER_STATUS;
            if (load_number >= trans_num) {
                fclose (fp);
                break;
            }
        }
        else if ((whichs&1) && (*(int*)tran_buffer == STOCK_LEVEL+100)) {
            //if (stocklevels == NULL) continue;
            memcpy(&stocklevels[stocklevels_number], tran_buffer+sizeof(int), sizeof(struct input_stock_level_t));
            stocklevels_number ++;
            choice[load_number++] = STOCK_LEVEL;
            if (load_number >= trans_num) {
                fclose (fp);
                break;
            }
        }
    }
    // printf ("loadTransactions nn: %d pp: %d dd: %d oo: %d ss: %d\n", neworders_number, payments_number, deliverys_number, orderstatuss_number, stocklevels_number);
    return load_number;
}

//=================================================================================
//                      3. create tables and indexes
//=================================================================================

/**
 * load a database schema from a txt file.
 * @param filename name of schema file, the schema must meet the folowing condition
 * @retval 0  success
 * #retval <0 faliure 
 *
 * schema format 
 * (1) split by one '\t', a row ends with '\n'
 * (2) claim Database,Table,column,index in order
 * (3) no empty row
**/
int load_schema(const char *filename)
{
    int64_t cur_db_id = -1;
    int64_t cur_tb_id;
    int64_t cur_col_id;
    int64_t cur_ix_id;
    Database *cur_db_ptr = NULL;
    Table *cur_tb_ptr = NULL;
    FILE *fp = fopen(filename, "r");
    if (fp == NULL) {
        printf("[load_schema][ERROR]: filename error!\n");
        return -1;
    }
    char buffer[1024];
    while (fgets(buffer, 1024, fp)) {
        int pos = 0, num = 0;
        char *row[16];
        char *ptr = NULL;
        while (buffer[pos] == '\t')
            pos++;
        ptr = buffer + pos;
        while (num < 16) {
            while (buffer[pos] != '\t' && buffer[pos] != '\n')
                pos++;
            row[num++] = ptr;
            ptr = buffer + pos + 1;
            if (buffer[pos] == '\n') {
                buffer[pos] = '\0';
                break;
            }
            buffer[pos] = '\0';
            pos++;
        }
        /*
           //  debug
           for (int ii=0; ii< num; ii++)
           printf ("%s\t", row[ii]);
           printf ("\n");
         */
        if (num >= 16) {
            printf("[load_schema][ERROR]: row with too many field!\n");
            return -2;
        }
        if (!strcmp(row[0], "DATABASE")) {
            if (cur_db_id != -1)
                g_catalog.initDatabase(cur_db_id);
            g_catalog.createDatabase((const char *) row[1], cur_db_id);
            cur_db_ptr = (Database *) g_catalog.getObjById(cur_db_id);
        } else if (!strcmp(row[0], "TABLE")) {
            TableType type;
            if (!strcmp(row[2], "ROWTABLE"))
                type = ROWTABLE;
            else if (!strcmp(row[2], "COLTABLE"))
                type = COLTABLE;
            else {
                printf("[load_schema][ERROR]: table type error!\n");
                return -4;
            }
            g_catalog.createTable((const char *) row[1], type, cur_tb_id);
            cur_tb_ptr = (Table *) g_catalog.getObjById(cur_tb_id);
            cur_db_ptr->addTable(cur_tb_id);
        } else if (!strcmp(row[0], "COLUMN")) {
            ColumnType type;
            int64_t len = 0;
            if (!strcmp(row[2], "INT8"))
                type = INT8;
            else if (!strcmp(row[2], "INT16"))
                type = INT16;
            else if (!strcmp(row[2], "INT32"))
                type = INT32;
            else if (!strcmp(row[2], "INT64"))
                type = INT64;
            else if (!strcmp(row[2], "FLOAT32"))
                type = FLOAT32;
            else if (!strcmp(row[2], "FLOAT64"))
                type = FLOAT64;
            else if (!strcmp(row[2], "DATE"))
                type = DATE;
            else if (!strcmp(row[2], "TIME"))
                type = TIME;
            else if (!strcmp(row[2], "DATETIME"))
                type = DATETIME;
            else if (!strcmp(row[2], "CHARN")) {
                type = CHARN;
                len = atoi(row[3]);
            } else {
                printf("[load_schema][ERROR]: column type error!\n");
                printf("error: %s\n", row[1]);
                return -5;
            }
            g_catalog.createColumn((const char *) row[1], type, len,
                                   cur_col_id);
            cur_tb_ptr->addColumn(cur_col_id);
        } else if (!strcmp(row[0], "INDEX")) {
            IndexType type = INVID_I;
            std::vector < int64_t > cols;
            if (!strcmp(row[2], "HASHINDEX"))
                type = HASHINDEX;
            else if (!strcmp(row[2], "BPTREEINDEX"))
                type = BPTREEINDEX;
            else if (!strcmp(row[2], "ARTTREEINDEX"))
                type = ARTTREEINDEX;
            for (int ii = 3; ii < num; ii++) {
                int64_t oid = g_catalog.getObjByName(row[ii])->getOid();
                cols.push_back(oid);
            }
            Key key;
            key.set(cols);
            g_catalog.createIndex(row[1], type, key, cur_ix_id);
            cur_tb_ptr->addIndex(cur_ix_id);
        } else {
            printf("[load_schema][ERROR]: o_type error!\n");
            return -3;
        }
    }
    g_catalog.initDatabase(cur_db_id);
    return 0;
}

//=================================================================================
//                      4. insert data into tables and indexes
//=================================================================================

/**
 * load table data from txt files
 * @param tablename names of tables
 * @param data_dir the dir of data files corresponding to the table, end with '/'
 * @param number number of tables to load data
 * @retval 0  success
 * @retval <0 faliure
 *
 * naming rule of data files
 * 
 * each data file should be named "table_name.tab" and meet the following requirement
 *
 * data format
 *
 * (1) split by one '\t', a row ends with '\n'
 * (2) no empty row
 *
**/

const char *table_name[] = {
    "customer",
    "district",
    "history",
    "item",
    "new_order",
    "order",
    "order_line",
    "stock",
    "warehouse"
};

int load_data(const char *tablename[], int table_number, const char *data_dir)
{
    for (int ii = 0; ii < table_number; ii++) {
        char filename[1024];
        strcpy (filename, data_dir);
        strcat (filename, tablename[ii]);
        strcat (filename, ".data");
        FILE *fp = fopen(filename, "r");
        if (fp == NULL) {
            printf("[load_data][ERROR]: filename error!\n");
            return -1;
        }
        Table *tp =
            (Table *) g_catalog.getObjByName((char *) tablename[ii]);
        if (tp == NULL) {
            printf("[load_data][ERROR]: tablename error!\n");
            return -2;
        }
        int colnum = tp->getColumns().size();
        BasicType *dtype[colnum];
        for (int ii = 0; ii < colnum; ii++)
            dtype[ii] =
                ((Column *) g_catalog.getObjById(tp->getColumns()[ii]))->getDataType();
        int indexnum = tp->getIndexs().size();
        Index *index[indexnum];
        for (int ii = 0; ii < indexnum; ii++)
            index[ii] = (Index *) g_catalog.getObjById(tp->getIndexs()[ii]);

        char buffer[2048];
        char *columns[colnum];
        void *columnsvoid[colnum];
        char data[colnum][1024];
        while (fgets(buffer, 2048, fp)) {
            // insert table
            columns[0] = buffer;
            int64_t pos = 0;
            for (int64_t ii = 1; ii < colnum; ii++) {
                for (int64_t jj = 0; jj < 2048; jj++) {
                    if (buffer[pos] == '\t') {
                        buffer[pos] = '\0';
                        columns[ii] = buffer + pos + 1;
                        pos++;
                        break;
                    } else
                        pos++;
                }
            }
            /*
               // debug
               for (int ii= 0; ii< colnum; ii++) {
               printf ("%s\t", columns[ii]);
               }
               printf ("\n");
             */
            while (buffer[pos] != '\n')
                pos++;
            buffer[pos] = '\0';
            for (int64_t ii = 0; ii < colnum; ii++) {
                BasicType *p = dtype[ii];
                p->formatBin(data[ii], columns[ii]);
                columns[ii] = data[ii];
            }
            tp->insert(columns);
            void *ptr = tp->getRecordPtr(tp->getRecordNum() - 1);
            // insert index
            for (int ii = 0; ii < indexnum; ii++) {
                int indexsz = index[ii]->getIKey().getKey().size();
                for (int jj = 0; jj < indexsz; jj++)
                    columnsvoid[jj] =
                        data[tp->getColumnRank(index[ii]->getIKey().getKey()[jj])];
                index[ii]->insert(columnsvoid, ptr);
            }
        }
        // ((RowTable*)tp)->printData();
    }
    return 0;
}

//=================================================================================
//                      5. implement transactions
//=================================================================================

struct tpcc_context {
    Table *customer,*district,*history;
    Table *new_order,*order,*order_line;
    Table *warehouse,*item,*stock;
    Index *ht_customer,*ht_customer_2,*ht_district;
    Index *ht_item,*ht_neworder,*ht_order,*ht_warehouse;
    Index *ht_order_2,*ht_order_line,*ht_stock;
};

int runNewOrder (struct input_new_order_t *neworder, struct tpcc_context context) {
    int64_t q_wid = neworder->w_id;
    int64_t q_did = neworder->d_id;
    int64_t q_cid = neworder->c_id;
    int64_t q_ol_cnt = neworder->o_ol_cnt;
    int64_t q_o_all_local = neworder->o_all_local;
    char info[2048];
    /*========================
        SELECT w_tax
        INTO out_w_tax
        FROM warehouse
        WHERE w_id = tmp_w_id;
    =========================*/
    int64_t key = q_wid;
    bool    isok =  false;
    void   *record = NULL;
    // char *match[4];
    // int last = ht_warehouse.probe (key, match, 4);
    context.ht_warehouse->set_ls (&key, NULL, (void*)info);
#ifndef DEBUG
    context.ht_warehouse->lookup (&key, (void*)info, record);
#else
    isok = context.ht_warehouse->lookup (&key, (void*)info, record);
    if (isok == false) {
        printf ("error runNewOrder -533\n");
        return -533;
    }
#endif
    double out_w_tax = 0.0;
#ifndef DEBUG
    context.warehouse->selectCol ((char*)record, 7, (char*)&out_w_tax);
#else
    isok = context.warehouse->selectCol ((char*)record, 7, (char*)&out_w_tax);
    if (isok == false) {
        printf ("error runNewOrder -536\n");
        return -540;
    }
#endif
    /*=========================    
        SELECT d_tax, d_next_o_id
        INTO out_d_tax, out_d_next_o_id
        FROM district   
        WHERE d_w_id = tmp_w_id
        AND d_id = tmp_d_id FOR UPDATE;
    ===========================*/
    void *keys[4];
    keys[0] = &q_did;
    keys[1] = &q_wid;
    context.ht_district->set_ls (keys, NULL, (void*)info);
#ifndef DEBUG
    context.ht_district->lookup (keys, (void*)info, record);
#else
    isok = context.ht_district->lookup (keys, (void*)info, record);
    if (isok == false) {
        printf ("error runNewOrder -552\n");
        return -556;
    }
#endif
    double out_d_tax = 0.0;
#ifndef DEBUG
    context.district->selectCol ((char*)record, 8, (char*)&out_d_tax);
#else
    isok = context.district->selectCol ((char*)record, 8, (char*)&out_d_tax);
    if (isok == false) {
        printf ("error runNewOrder -558\n");
        return -561;
    }
#endif
    int64_t out_d_next_o_id = 0;
#ifndef DEBUG
    context.district->selectCol ((char*)record, 10, (char*)&out_d_next_o_id);
#else
    isok = context.district->selectCol ((char*)record, 10, (char*)&out_d_next_o_id);
    if (isok == false) {
        printf ("error runNewOrder -564\n");
        return -568;
    }
#endif
    /*==========================
        UPDATE district
        SET d_next_o_id = d_next_o_id + 1
        WHERE d_w_id = tmp_w_id
        AND d_id = tmp_d_id;
    ============================*/
    out_d_next_o_id += 1;
#ifndef DEBUG
    context.district->updateCol ((char*)record, 10, (char*)&out_d_next_o_id);
#else
    isok = context.district->updateCol ((char*)record, 10, (char*)&out_d_next_o_id);
    out_d_next_o_id -= 1;
    if (isok == false) {
        printf ("error runNewOrder -577\n");
        return -568;
    }
#endif
    /*==========================
        SELECT c_discount , c_last, c_credit
        INTO out_c_discount, out_c_last, out_c_credit
        FROM customer
        WHERE c_w_id = tmp_w_id
        AND c_d_id = tmp_d_id
        AND c_id = tmp_c_id;
    ============================*/
    keys[0] = &q_cid;
    keys[1] = &q_did;
    keys[2] = &q_wid;
    context.ht_customer->set_ls (keys, NULL, (void*)info);
#ifndef DEBUG
    context.ht_customer->lookup (keys, (void*)info, record);
#else
    isok = context.ht_customer->lookup (keys, (void*)info, record);
    if (isok == false) {
        printf ("error runNewOrder -595\n");
        return -594;
    }
#endif
    double out_c_discount = 0.0;
#ifndef DEBUG
    context.customer->selectCol ((char*)record, 15, (char*)&out_c_discount);
#else
    isok = context.customer->selectCol ((char*)record, 15, (char*)&out_c_discount);
    if (isok == false) {
        printf ("error runNewOrder -601\n");
        return -600;
    }
#endif
    char out_c_last[256];
#ifndef DEBUG
    context.customer->selectCol ((char*)record, 5, out_c_last);
#else
    isok = context.customer->selectCol ((char*)record, 5, out_c_last);
    if (isok == false) {
        printf ("error runNewOrder -608\n");
        return -606;
    }
#endif
    char out_c_credit[256];
#ifndef DEBUG
    context.customer->selectCol ((char*)record, 13, out_c_credit);
#else
    isok = context.customer->selectCol ((char*)record, 13, out_c_credit);
    if (isok == false) {
        printf ("error runNewOrder -614\n");
        return -614;
    }
#endif
    /*==========================
        INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
        VALUES (out_d_next_o_id, tmp_d_id, tmp_w_id);
    ============================*/
    char *data[20];
    data[0] = (char*)&out_d_next_o_id;
    data[1] = (char*)&q_did;
    data[2] = (char*)&q_wid;
    context.new_order->insert (data);
    int64_t record_num = context.new_order->getRecordNum();
    void *pin = context.new_order->getRecordPtr(record_num-1);
    keys[0] = &q_did;
    keys[1] = &q_wid;
    context.ht_neworder->insert (keys, pin);
    /*==========================
        INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d,
	        o_carrier_id, o_ol_cnt, o_all_local)
        VALUES (out_d_next_o_id, tmp_d_id, tmp_w_id, tmp_c_id,
	        current_timestamp, NULL, tmp_o_ol_cnt, tmp_o_all_local);
    ============================*/
    uint64_t tmp_o_entry_d = 10000000;
    int64_t  tmp_o_carrier_id = 0;
    data[0] = (char*)&out_d_next_o_id;
    data[1] = (char*)&q_did;
    data[2] = (char*)&q_wid;
    data[3] = (char*)&q_cid;
    data[4] = (char*)&tmp_o_entry_d;
    data[5] = (char*)&tmp_o_carrier_id;
    data[6] = (char*)&q_ol_cnt;
    data[7] = (char*)&q_o_all_local;
    context.order->insert (data);
    record_num = context.order->getRecordNum();
    pin = context.order->getRecordPtr(record_num-1);
    keys[0] = &out_d_next_o_id;
    keys[1] = &q_did;
    keys[2] = &q_wid;
    context.ht_order->insert (keys, pin);
    keys[0] = &q_cid;
    context.ht_order_2->insert(keys, pin);

    double total_amount = 0.0;
    for (int64_t ii= 0;ii< q_ol_cnt; ii++) {
        struct no_order_line_t q_ol = neworder->order_line[ii];
        int64_t tmp_i_id = q_ol.ol_i_id;
        int64_t tmp_ol_supply_w_id = q_ol.ol_supply_w_id;
        int64_t tmp_ol_quantity = q_ol.ol_quantity;
        /*========================
            SELECT i_price, i_name, i_data
            INTO tmp_i_price, tmp_i_name, tmp_i_data
            FROM item
            WHERE i_id = tmp_i_id;
        ==========================*/
        key = tmp_i_id;
        keys[0] = (void*)&key;
        // the item has 1% error to rollback       
        context.ht_item->set_ls (keys, NULL, (void*)info);
        isok = context.ht_item->lookup (keys, (void*)info, record);
        if (isok == false) {
            printf ("error runNewOrder -672 ii:%d tmp_id:%d\n", ii, tmp_i_id);
            // return -672;
            return 0;
        }

        double tmp_i_price = 0.0;
        context.item->selectCol ((char*)record, 3, (char*)&tmp_i_price);
        char   tmp_i_name[50],tmp_i_data[50];
        context.item->selectCol ((char*)record, 2, tmp_i_name);
        context.item->selectCol ((char*)record, 4, tmp_i_data);
 	/*========================
            SELECT s_quantity, s_dist_0N, s_data
		    INTO out_s_quantity, tmp_s_dist, tmp_s_data
		    FROM stock
		    WHERE s_i_id = in_ol_i_id
		    AND s_w_id = in_w_id;
        ==========================*/
        keys[0] = &tmp_i_id;
        keys[1] = &q_wid;
        context.ht_stock->set_ls (keys, NULL, (void*)info);
        context.ht_stock->lookup (keys, (void*)info, record);
        int64_t out_s_quantity = 0;
        context.stock->selectCol ((char*)record, 2, (char*)&out_s_quantity);
        char tmp_s_district[50];
        context.stock->selectCol ((char*)record, q_did+2, tmp_s_district);
    	/*========================
            IF out_s_quantity > in_ol_quantity + 10 THEN
		        UPDATE stock
		        SET s_quantity = s_quantity - in_ol_quantity
		        WHERE s_i_id = in_ol_i_id
		        AND s_w_id = in_w_id;
	        ELSE
		        UPDATE stock
		        SET s_quantity = s_quantity - in_ol_quantity + 91
		        WHERE s_i_id = in_ol_i_id
		        AND s_w_id = in_w_id;
	        END IF;
        ==========================*/
        if (out_s_quantity > tmp_ol_quantity+10)
            out_s_quantity -= tmp_ol_quantity;
        else
            out_s_quantity = out_s_quantity-tmp_ol_quantity+91;
        context.stock->updateCol ((char*)record, 2, (char*)&out_s_quantity);
        /*========================
	        INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id,
	                        ol_supply_w_id, ol_delivery_d, ol_quantity,
                                ol_amount, ol_dist_info)
	        VALUES (in_ol_o_id, in_d_id, in_w_id, in_ol_number, in_ol_i_id,
	            in_ol_supply_w_id, NULL, in_ol_quantity, in_ol_amount,
	            tmp_s_dist);
        ==========================*/
        float tmp_amount = tmp_ol_quantity*tmp_i_price;
        int64_t tmp_ol_delivery_d = 0;
        data[0] = (char*)&out_d_next_o_id;
        data[1] = (char*)&q_did;
        data[2] = (char*)&q_wid;
        data[3] = (char*)&ii;
        data[4] = (char*)&tmp_i_id;
        data[5] = (char*)&tmp_ol_supply_w_id;
        data[6] = (char*)&tmp_ol_delivery_d;
        data[7] = (char*)&tmp_ol_quantity;
        data[8] = (char*)&tmp_amount;
        data[9] = tmp_s_district;
        context.order_line->insert (data);
        record_num = context.order->getRecordNum();
        pin = context.order->getRecordPtr(record_num-1);
        keys[0] = &out_d_next_o_id;
        keys[1] = &q_did;
        keys[2] = &q_wid;
        context.ht_order_line->insert (keys, pin);
        total_amount += tmp_amount;
    }
    return 0;
}
int runPayment (struct input_payment_t *payment, struct tpcc_context context) {
    /*===========================
        SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip
        INTO out_w_name, out_w_street_1, out_w_street_2, out_w_city,
             out_w_state, out_w_zip
        FROM warehouse
        WHERE w_id = in_w_id;
        UPDATE warehouse
        SET w_ytd = w_ytd + in_h_amount
        WHERE w_id = in_w_id;
    =============================*/
    int64_t q_wid = payment->w_id;
    int64_t q_did = payment->d_id;
    int64_t q_cwid = payment->c_w_id;
    int64_t q_cdid = payment->c_d_id;
    int64_t q_cid = payment->c_id;
    char *q_clast = payment->c_last;
    double q_hamount = payment->h_amount;
#ifdef DEBUG
    bool isok;
#endif
    char info[2048];
    int64_t key = q_wid;
    void *record = NULL;
    double tmp_w_ytd = 0.0;
    void *keys[10];
    keys[0] = &key;
    context.ht_warehouse->set_ls (keys, NULL, (void*)info);
#ifndef DEBUG
    context.ht_warehouse->lookup (keys, (void*)info, record);
#else
    isok = context.ht_warehouse->lookup (keys, (void*)info, record);
    if (isok == false) {
        printf ("error runPayment! -774\n");
        return -774;
    }
#endif
#ifndef DEBUG
    context.warehouse->selectCol ((char*)record, 8, (char*)&tmp_w_ytd);
#else
    isok = context.warehouse->selectCol ((char*)record, 8, (char*)&tmp_w_ytd);
    if (isok == false) {
        printf ("error runPayment! -779\n");
        return -779;
    }
#endif
    tmp_w_ytd += q_hamount;
    context.warehouse->updateCol ((char*)record,8,(char*)&tmp_w_ytd);
    /*============================
        SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip
        INTO out_d_name, out_d_street_1, out_d_street_2, out_d_city,
             out_d_state, out_d_zip
        FROM district
        WHERE d_id = in_d_id
          AND d_w_id = in_w_id;
        UPDATE district
        SET d_ytd = d_ytd + in_h_amount
        WHERE d_id = in_d_id
          AND d_w_id = in_w_id;
    ==============================*/
    keys[0] = &q_did;
    keys[1] = &q_wid;
    context.ht_district->set_ls (keys, NULL, (void*)info);
    double tmp_d_ytd = 0.0;
    context.ht_district->lookup (keys, (void*)info, record);
    context.district->selectCol ((char*)record, 9, (char*)&tmp_d_ytd);
    tmp_d_ytd += q_hamount;
    context.district->updateCol ((char*)record, 9, (char*)&tmp_d_ytd);

    if (q_cid == 0) {
        /*=============================
        SELECT c_id
        INTO out_c_id
        FROM customer
        WHERE c_w_id = in_c_w_id
          AND c_d_id = in_c_d_id
          AND c_last = in_c_last
          ORDER BY c_first ASC limit 1;
        =============================*/
        keys[0] = &q_cdid;
        keys[1] = &q_cwid;
        keys[2] = q_clast;
        context.ht_customer_2->set_ls (keys, NULL, (void*)info);
#ifndef DEBUG
        context.ht_customer_2->lookup (keys, (void*)info, record);
        context.customer->selectCol ((char*)record, 0, (char*)&q_cid);
#else
        isok = context.ht_customer_2->lookup (keys, (void*)info, record);
        if (isok == false) {
            printf ("error runPayment! -821\n");
            return -821;
        }
        isok = context.customer->selectCol ((char*)record, 0, (char*)&q_cid);
        if (isok == false) {
            printf ("error runPayment! -826\n");
            return -826;
        }
#endif
    }
    else {
        keys[0] = &q_cid;
        keys[1] = &q_cdid;
        keys[2] = &q_cwid;
        context.ht_customer->set_ls (keys, NULL, (void*)info);
#ifndef DEBUG
        context.ht_customer->lookup (keys, (void*)info, record);
#else
        isok = context.ht_customer->lookup (keys, (void*)info, record);
        if (isok == false) {
            printf ("error runPayment! -837\n");
            return -837;
        }
#endif
    }
    /*=============================
        UPDATE customer
        SET c_balance = out_c_balance - in_h_amount,
            c_ytd_payment = out_c_ytd_payment + 1,
            c_data = out_c_data
        WHERE c_id = out_c_id
          AND c_w_id = in_c_w_id
          AND c_d_id = in_c_d_id;
      =============================*/
    double tmp_balance = 0.0;
    context.customer->selectCol ((char*)record, 16, (char*)&tmp_balance);
    tmp_balance -= q_hamount;
    context.customer->updateCol ((char*)record, 16, (char*)&tmp_balance);
    int64_t tmp_c_ytd_payment = 0;
    context.customer->selectCol ((char*)record, 17, (char*)&tmp_c_ytd_payment);
    tmp_c_ytd_payment += 1;
    context.customer->updateCol ((char*)record, 17, (char*)&tmp_c_ytd_payment);
    /*=============================
        SET tmp_h_data = concat(out_w_name,' ', out_d_name);
        INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id,
                             h_date, h_amount, h_data)
        VALUES (out_c_id, in_c_d_id, in_c_w_id, in_d_id, in_w_id,
                current_timestamp, in_h_amount, tmp_h_data);
      =============================*/
    char *data[20];
    int64_t tmp_current_timestamp = 10000000;
    char tmp_h_data[20] = "tmp_h_data";
    data[0] = (char*)&q_cid;
    data[1] = (char*)&q_cdid;
    data[2] = (char*)&q_cwid;
    data[3] = (char*)&q_did;
    data[4] = (char*)&q_wid;
    data[5] = (char*)&tmp_current_timestamp;
    data[6] = (char*)&q_hamount;
    data[7] = tmp_h_data;
    context.history->insert (data);
    return 0;
}
int runDelivery(struct input_delivery_t *delivery, struct tpcc_context context) 
{
/*
    int q_w_id = delivery->w_id;
    int q_o_carrier_id = delivery->o_carrier_id;
    
    int64_t out_c_id, tmp_d_id, tmp_o_id=0;
    double  out_ol_amount = 0;

    for (int64_t tmp_d_id=1;tmp_d_id<=DISTRICT_PER_WAREHOUSE;tmp_d_id++) {
        tmp_o_id = 0;
        //=============================
        SELECT no_o_id
        INTO tmp_o_id
        FROM new_order
        WHERE no_w_id = in_w_id AND no_d_id = tmp_d_id limit 1;
        //===============================
        int64_t key = (q_w_id-1)*DISTRICT_PER_WAREHOUSE+(tmp_d_id-1);
        char *match[16];
        int last = ht_neworder.probe (key,match,16);
        if (last == 0) 
            continue;
        char *record = match[0];
        tmp_o_id = *(int64_t*)record;
        if (*(int64_t*)record <= 0)
            continue;
        //=============================
        DELETE FROM new_order 
        WHERE no_o_id = tmp_o_id 
         AND no_w_id = in_w_id 
         AND no_d_id = tmp_d_id;
        //===============================
        ht_neworder.del (getNewOrderKey(record), record);
        //============================
        SELECT o_c_id
        INTO out_c_id
        FROM orders
        WHERE o_id = tmp_o_id
         AND o_w_id = in_w_id
         AND o_d_id = tmp_d_id;
        //==============================
        key =  tmp_o_id-1;
        key += (tmp_d_id-1)*ORDER_PER_DISTRICT;
        key += (q_w_id-1)*DISTRICT_PER_WAREHOUSE*ORDER_PER_DISTRICT;
        last = ht_order.probe (key, match, 16);
#ifdef DEBUG
        if (last != 1) {
            printf ("[main][ERROR][runDelivery]:table order primary key error! 1075\n");
            printf ("key: %ld last: %d\n", key, last);
            return -1;
        }
#endif
        int offset = order.getColumnOffset(2);
        out_c_id = *(int64_t*)(record+offset);
        ============================
        UPDATE orders
        SET o_carrier_id = in_o_carrier_id
        WHERE o_id = tmp_o_id
         AND o_w_id = in_w_id
         AND o_d_id = tmp_d_id;
        ==============================
        offset = order.getColumnOffset(5);
        *(int64_t*)(record+offset) = 10000000000;
        ============================
        UPDATE order_line
        SET ol_delivery_d = current_timestamp
        WHERE ol_o_id = tmp_o_id
         AND ol_w_id = in_w_id
         AND ol_d_id = tmp_d_id;
        ==============================
        last = ht_orderline.probe (key, match, 16);
#ifdef DEBUG
        if (last < 0) {
            printf ("[main][ERROR][runDelivery]: table orderline error! 1098\n");
            return -2;
        }
#endif
        record = match[0];
        offset = order_line.getColumnOffset(6);
        *(int64_t*)(record+offset) = 10000000000;
        ============================
        SELECT SUM(ol_amount * ol_quantity)
        INTO out_ol_amount
        FROM order_line
        WHERE ol_o_id = tmp_o_id
         AND ol_w_id = in_w_id
         AND ol_d_id = tmp_d_id;
        ==============================
        int offset1 = order_line.getColumnOffset(7);
        int offset2 = order_line.getColumnOffset(8);
        for (int ii=0; ii<last; ii++)
            out_ol_amount += (*(int64_t*)(match[ii]+offset1))*(*(double*)(match[ii]+offset2));
        //============================
        UPDATE customer
        SET c_delivery_cnt = c_delivery_cnt + 1,
           c_balance = c_balance + out_ol_amount
        WHERE c_id = out_c_id
         AND c_w_id = in_w_id
         AND c_d_id = tmp_d_id;
        //==============================
        key = out_c_id-1;
        key += (tmp_d_id-1)*CUSTOMER_PER_DISTRICT;
        key += (q_w_id-1)*CUSTOMER_PER_DISTRICT*DISTRICT_PER_WAREHOUSE;
        last = ht_customer.probe (key, match, 16);
#ifdef DEBUG
        if (last != 1) {
            printf ("[main][ERROR][runDelivery]: table customer primary key error! 1129\n");
            return -3;
        }
#endif
        record = match[0];
        offset = customer.getColumnOffset (19);
        *(int64_t*)(record+offset) += 1;
        offset = customer.getColumnOffset (16);
        *(double*)(record+offset) += out_ol_amount;
    }
*/
    return 0;
}

int runOrderStatus(struct input_order_status_t *orderstatus, struct tpcc_context context)
{
/*
    int q_cid = orderstatus->c_id;
    int q_cwid = orderstatus->c_w_id;
    int q_cdid = orderstatus->c_d_id;
    char *q_clast = orderstatus->c_last;

    char *record = NULL;
    char *match[16];
    int offset,last;
    int64_t out_c_id,key;
    char out_c_first[255],out_c_middle[255],out_c_last[255];
    double out_c_balance;
    if (q_cid == 0) {
        =============================
        SELECT c_id
        INTO out_c_id
        FROM customer
        WHERE c_w_id = in_c_w_id
          AND c_d_id = in_c_d_id
          AND c_last = in_c_last
          ORDER BY c_first ASC limit 1;
        =============================
        key = hash (q_clast,C_LAST_LEN);
	    key = ((0x1L<<32)-1) & key;
        key += ((q_cwid-1)*DISTRICT_PER_WAREHOUSE+(q_cdid-1))*CUSTOMER_PER_DISTRICT;
        last = ht_customer2.probe (key,match,16);
        if (last < 0) record = match[7];
        else record = match[last/2];
        offset = customer.getColumnOffset(0);
        q_cid = *(int64_t*)(record+offset);
    }
    else {
        key = q_cid-1;
        key += ((q_cwid-1)*DISTRICT_PER_WAREHOUSE+(q_cdid-1))*CUSTOMER_PER_DISTRICT;
        last = ht_customer.probe (key,match,16);
#ifdef DEBUG
        if (last != 1) {
            printf ("[main][ERROR][runPayment]: customer not found! -3\n");
            return -3;
        }
#endif
        record = match[0];
    }
    out_c_id = q_cid;
    ===============================
    SELECT c_first, c_middle, c_last, c_balance
	INTO out_c_first, out_c_middle, out_c_last, out_c_balance
	FROM customer
	WHERE c_w_id = in_c_w_id   
	  AND c_d_id = in_c_d_id
	  AND c_id = out_c_id;
    =================================
    strncpy (out_c_first,record+customer.getColumnOffset(3),255);
    strncpy (out_c_middle,record+customer.getColumnOffset(4),255);
    strncpy (out_c_last,record+customer.getColumnOffset(5),255);
    out_c_balance = *(double*)(record+customer.getColumnOffset(16));
    =============================== 
	SELECT o_id, o_carrier_id, o_entry_d, o_ol_cnt
	INTO out_o_id, out_o_carrier_id, out_o_entry_d, out_o_ol_cnt
	FROM orders
	WHERE o_w_id = in_c_w_id
  	AND o_d_id = in_c_d_id
  	AND o_c_id = out_c_id
	ORDER BY o_id DESC limit 1;
    =================================
    key = out_c_id-1;
    key += (q_cdid-1)*CUSTOMER_PER_DISTRICT;
    key += (q_cwid-1)*CUSTOMER_PER_DISTRICT*DISTRICT_PER_WAREHOUSE;
    last = ht_order2.probe (key, match, 4);
#ifdef DEBUG
    if (last == 0) {
        printf ("[main][ERROR][runOrderStatus]: table order error! 1219\n");
        return -4;
    }
#endif
    record = match[0];
    int64_t out_o_id = *(int64_t*)(record+order.getColumnOffset(0));
    int64_t out_o_carrier_id = *(int64_t*)(record+order.getColumnOffset(5));
    int64_t out_o_entry_d = *(int64_t*)(record+order.getColumnOffset(4));
    int64_t out_o_ol_cnt = *(int64_t*)(record+order.getColumnOffset(6));
    ================================
        declare c cursor for SELECT ol_i_id, ol_supply_w_id, ol_quantity, 
                                    ol_amount, ol_delivery_d
                             FROM order_line
                             WHERE ol_w_id = in_c_w_id
                               AND ol_d_id = in_c_d_id
                               AND ol_o_id = out_o_id;
    ==================================
    int64_t out_ol_i_id[15],out_ol_supply_w_id[15],out_ol_quantity[15],out_ol_delivery[15];
    double out_ol_amount[15];
    key = out_o_id - 1;
    key += (q_cdid-1)*ORDER_PER_DISTRICT;
    key += (q_cwid-1)*ORDER_PER_DISTRICT*DISTRICT_PER_WAREHOUSE;
    last = ht_orderline.probe (key, match, 16);
#ifdef DEBUG
    if (last <= 0) {
        printf ("[main][ERROR][runOrderStatus]: table orderline error! 1234\n");
        return -4;
    }
#endif
    int offset1 = order_line.getColumnOffset (4);
    int offset2 = order_line.getColumnOffset (5);
    int offset3 = order_line.getColumnOffset (7);
    int offset4 = order_line.getColumnOffset (8);
    int offset5 = order_line.getColumnOffset (6);
    for (int ii=0;ii<last;ii++) {
        out_ol_i_id[ii] = *(int64_t*)(match[ii]+offset1);
        out_ol_supply_w_id[ii] = *(int64_t*)(match[ii]+offset2);
        out_ol_quantity[ii] = *(int64_t*)(match[ii]+offset3);
        out_ol_amount[ii] = *(double*)(match[ii]+offset4);
        out_ol_delivery[ii] = *(int64_t*)(match[ii]+offset5);
    }
*/
    return 0;
}

int runStockLevel(struct input_stock_level_t *stocklevel, struct tpcc_context context)
{
/*
    int q_w_id = stocklevel->w_id;
    int q_d_id = stocklevel->d_id;
    int64_t q_threshold = stocklevel->threshold;
    =============================
    SELECT d_next_o_id
    INTO tmp_d_next_o_id
    FROM district
    WHERE d_w_id = in_w_id
      AND d_id = in_d_id;
    ===============================
    int64_t key = q_d_id-1+(q_w_id-1)*DISTRICT_PER_WAREHOUSE,key2;
    char *match[16], *match2[16];
    int last = ht_district.probe (key, match, 16),last2;
#ifdef DEBUG
    if (last != 1) {
        printf ("[main][ERROR][runStockLevel]: table district error! 1271\n");
        return -1;
    }
#endif
    char *record = match[0];
    int offset = district.getColumnOffset (10);
    int64_t tmp_next_o_id = *(int64_t*)(record+offset);
    =============================
    SELECT count(*)
    INTO low_stock
    FROM order_line, stock, district
    WHERE d_id = in_d_id
        AND d_w_id = in_w_id
        AND d_id = ol_d_id
        AND d_w_id = ol_w_id
        AND ol_i_id = s_i_id
        AND ol_w_id = s_w_id
        AND s_quantity < in_threshold
        AND ol_o_id BETWEEN (tmp_d_next_o_id - 20)
                        AND (tmp_d_next_o_id - 1);
    ===============================
    int offset1 = order_line.getColumnOffset(4);
    int offset2 = stock.getColumnOffset(2);
    int out_low_stock = 0;
    for (int64_t tmp_o_id=tmp_next_o_id-20; tmp_o_id<= tmp_next_o_id-1; tmp_o_id++) {
        key = tmp_o_id-1;
        key += (q_d_id-1)*ORDER_PER_DISTRICT;
        key += (q_w_id-1)*ORDER_PER_DISTRICT*DISTRICT_PER_WAREHOUSE;
        last2 = ht_orderline.probe (key, match, 16);
#ifdef DEBUG
        if (last2 < 0) {
            printf ("[main][ERROR][runStockLevel]: table order_line error! 1288\n");
            return -2;
        }
#endif
        for (int ii=0;ii<last;ii++) {
            int64_t i_id = *(int64_t*)(match[ii]+offset1);
            key2 = i_id-1;
            key2 += (q_w_id-1)*MAXIMUM_ITEM_NUMBER;
            last2 = ht_item.probe (key2, match2, 16);
#ifdef DEBUG
            if (last2 != 1) {
                printf ("[main][ERROR][runStockLevel]: table item error! 1300\n");
                return -3;
            }
#endif
            int64_t quantity = *(int64_t*)(match2[0]+offset2);
            if (quantity < q_threshold)
                out_low_stock ++;
        }
    }
*/
    return 0;
}

int runTransactions (struct input_new_order_t *&neworders, struct input_payment_t* &payments, 
                     struct input_delivery_t *&deleverys, struct input_order_status_t *&orderstatuss,
                     struct input_stock_level_t *&stocklevels, int trans_num, int *&choice)
{
    // pre operation to get tpcc_context infomation
    struct tpcc_context context;
    context.customer = (Table*)g_catalog.getObjByName("customer");
    context.district = (Table*)g_catalog.getObjByName("district");
    context.history = (Table*)g_catalog.getObjByName("history");
    context.order = (Table*)g_catalog.getObjByName("order");
    context.new_order = (Table*)g_catalog.getObjByName("new_order");
    context.order_line = (Table*)g_catalog.getObjByName("order_line");
    context.stock = (Table*)g_catalog.getObjByName("stock");
    context.item = (Table*)g_catalog.getObjByName("item");
    context.warehouse = (Table*)g_catalog.getObjByName("warehouse");
    context.ht_customer = (Index*)g_catalog.getObjByName("ht_customer");
    context.ht_customer_2 = (Index*)g_catalog.getObjByName("ht_customer_2");
    context.ht_district = (Index*)g_catalog.getObjByName("ht_district");
    context.ht_item = (Index*)g_catalog.getObjByName("ht_item");
    context.ht_neworder = (Index*)g_catalog.getObjByName("ht_neworder");
    context.ht_order = (Index*)g_catalog.getObjByName("ht_order");
    context.ht_order_2 = (Index*)g_catalog.getObjByName("ht_order_2");
    context.ht_order_line = (Index*)g_catalog.getObjByName("ht_orderline");
    context.ht_stock= (Index*)g_catalog.getObjByName("ht_stock");
    context.ht_warehouse= (Index*)g_catalog.getObjByName("ht_warehouse");

    struct timeval start,end;
    gettimeofday (&start, NULL);
    int nn= 0, pp= 0, dd= 0,oo= 0,ss= 0, stat=-1;
    for (int ii = 0;ii < trans_num;ii ++) {
#ifdef DEBUG
        switch (choice[ii]) 
        {
            case NEW_ORDER: 
                    if ((stat = runNewOrder (&neworders[nn], context) != 0)) {
                        printf ("[main][ERROR][runNewOrder]: neworder error! %d\n", ii);
                        return -1;
                    }
                    nn ++;
                    break;
            case PAYMENT: 
                    if ((stat = runPayment (&payments[pp], context) != 0)) {
                        printf ("[main][ERROR][runPayment]: payment error! %d\n", ii);
                        return -1;
                    }
                    pp ++;
                    break;
            case DELIVERY:
                    if ((stat = runDelivery (&deleverys[dd], context) != 0)) {
                        printf ("[main][ERROR][runDelivery]: delivery error! %d\n", ii);
                        return -1;
                    }
                    dd ++;
                    break;
            case ORDER_STATUS:
                    if ((stat = runOrderStatus (&orderstatuss[oo], context) != 0)) {
                        printf ("[main][ERROR][runOrderStatus]: order status error! %d\n", ii);
                        return -1;
                    }
                    oo ++;
                    break;
            case STOCK_LEVEL:
                    if ((stat = runStockLevel (&stocklevels[ss], context) != 0)) {
                        printf ("[main][ERROR][runPayment]: payment error! %d\n", ii);
                        return -1;
                    }
                    ss ++;
                    break;
        }

#else
        switch (choice[ii]) 
        {
            case NEW_ORDER: runNewOrder (&neworders[nn++],context);break;
            case PAYMENT: runPayment (&payments[pp++],context);break;
            case DELIVERY: runDelivery (&deleverys[dd++],context);break;
            case ORDER_STATUS: runOrderStatus (&orderstatuss[oo++],context);break;
            case STOCK_LEVEL: runStockLevel (&stocklevels[ss++],context);break;
        }

#endif
    }
    gettimeofday (&end, NULL);
    unsigned long diff = 1000000*(end.tv_sec-start.tv_sec)+(end.tv_usec-start.tv_usec);
    double average = ((double)diff) / trans_num; 
    printf ("transactions: %d time use: %lu us average: %lf us\n",trans_num, diff,average);
    printf ("transactions: nn:%d pp:%d dd:%d oo:%d ss:%d\n",nn,pp,dd,oo,ss);
    delete [] neworders;
    delete [] payments;
    delete [] deleverys;
    delete [] orderstatuss;
    delete [] stocklevels;
    delete [] choice;
    return 0;
}

//=================================================================================
//                      6. run transactions and analyze
//=================================================================================

int main (int argc,char *argv[]) 
{
    if (argc < 6) {
        printf("example: ./runtpcc schema_file trace_file transaction_number mode data_dir/\n");
           return 0;
    }
        
    int trans_number = -1, stat = -1;
    struct input_new_order_t *neworders = NULL;
    struct input_payment_t *payments = NULL;
    struct input_delivery_t *deliverys = NULL;
    struct input_order_status_t *orderstatuss = NULL;
    struct input_stock_level_t *stocklevels = NULL;
    int *choice = NULL;

    global_init ();
    if ((trans_number = loadTransactions (argv[2], atoi(argv[3]), atoi(argv[4]), neworders, payments, deliverys, orderstatuss, stocklevels, choice)) < 0) {
        printf ("loadTransactions error! %d\n", trans_number);
        return -5;
    }
    printf ("finish load transactions: %d\n", trans_number);
    printf ("----------------------------------------------------\n");
    // return 0;
    if ((stat = load_schema (argv[1])) < 0) {
        printf ("load_schema error! %d\n", stat);
        return -6;
    }
    printf ("finish load tpcc schema...ok\n");
    // g_catalog.print ();
    printf ("----------------------------------------------------\n");
    if ((stat = load_data (table_name, 9, argv[5])) < 0) {
        printf ("runTransactions error! %d\n", stat);
        return -6;
    }
    printf ("finish load tpcc data...ok\n");
    printf ("----------------------------------------------------\n");
    printf ("start tpcc test...\n");
    if ((stat = runTransactions (neworders,payments,deliverys,orderstatuss,stocklevels,trans_number,choice) < 0)) {
        printf ("runTransactions error! %d\n", stat);
        return -6;
    }
    global_shut ();

    return 0;    
}

