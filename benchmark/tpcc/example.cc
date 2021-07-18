/**
 * @file debug_tpcc.cc
 * @author liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION  
 *
 * optimal performance of tpcc-neworder
 *
 * (1) put table as an array
 * (2) use hash table to get things directly
 * (3) use trace from dbt-2
 *     under the same conditions
 *
 * step 1: set tables
 * step 2: load data
 * step 3: set index
 * step 4: load neworders
 * step 5: run new orders
 * 
 */

#include "datatype.h"
#include "hashtable.h"
#include <sys/time.h>
#include <assert.h>
#include <stdint.h>
#include <stdio.h>

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
#define TPCC_COLUMN_MAX_NUMBER (21)
class Table {
  public:
    char *memory_start;
    int memory_size; 
    int record_number;
    int record_capicity;
    int record_size;
    int column_number;
    int column_offset[TPCC_COLUMN_MAX_NUMBER];
    BasicType *data_type[TPCC_COLUMN_MAX_NUMBER];
  public:
    void initialize(int record_capicity = 1<<16) {
        for (int ii = 0;ii < column_number;ii ++) {
            column_offset[ii] = record_size;
            record_size += data_type[ii]->getTypeSize();
        }
        this->record_capicity = record_capicity;
        memory_size = record_capicity * record_size;
        memory_start = new char[memory_size];
        memset (memory_start,0,memory_size);
    }
    void finalize(void) {
        for (int ii = 0;ii < column_number;ii ++) {
            if (data_type[ii]) {
                delete data_type[ii];
            }
        }
        if (memory_start) {
            delete memory_start;
        }
    }
    char *getRC (int row, int column) {
#ifdef DEBUG
        return (row<record_capicity && column< column_number) ? memory_start+row*record_size+column_offset[column]: NULL;
#else
        return memory_start+row*record_size+column_offset[column];
#endif
    }
    char *getR (int row) {
        return memory_start+row*record_size;
    }
    int getColumnOffset(int column) {
        return column_offset[column];
    }
    int loadData (char *file_name) {
        FILE *fp = fopen (file_name,"r");
        if (fp == NULL) {
            printf ("[Table][ERROR][loadData]: data file can't be opened! -1\n");
            return -1;
        }
        char row_buffer[2048];
        char *wp = memory_start;
#ifdef DEBUG
        int jj = 0;
#endif
        while (fgets(row_buffer,2048,fp) != NULL) {
            int rp = 0;
            for (int ii = 0; ii < column_number; ii ++) {
                if (row_buffer[rp] != '\t') {
                    if (data_type[ii]->getTypeCode() == CHARN_TC) {
                        char column_buffer[1024];
                        sscanf (row_buffer+rp,"%s",column_buffer);
                        // column_buffer[data_type[ii]->getTypeSize()] = '\0';
                        data_type[ii]->formatBin(wp, column_buffer);
                    }
                    else {
                        data_type[ii]->formatBin(wp, row_buffer+rp);
                    }
                }
                while (row_buffer[rp++] != '\t') ;
                wp += data_type[ii]->getTypeSize();
            }
            record_number ++; 
#ifdef DEBUG
//            print (jj++);
#endif
        }
#ifdef DEBUG
        printf ("\nTHEENDOFSTDOUT\n\n");
#endif
        return 0;
    }
    void print (int row) {
        for (int ii = 0;ii < column_number; ii ++) {
            char column_buffer[1024];
            data_type[ii]->formatTxt (column_buffer, getRC(row,ii));
            printf ("%s\t", column_buffer);
            memset (column_buffer,0,1024);
        }
        printf ("\n");
    }
};

Table customer = { NULL,0,0,0,0,21,
                   { 0 },
                   { new TypeInt64(),new TypeInt64(),new TypeInt64(),
                     new TypeCharN(16),new TypeCharN(2),new TypeCharN(16),
                     new TypeCharN(20),new TypeCharN(20),new TypeCharN(20),
                     new TypeCharN(2),new TypeCharN(9),new TypeCharN(16),
                     new TypeDateTime(),new TypeCharN(2),new TypeInt64(),
                     new TypeInt64(),new TypeFloat64(),new TypeFloat64(),
                     new TypeInt64(),new TypeInt64(),new TypeCharN(500)
                   }
                 };

Table district = { NULL,0,0,0,0,11,
                   { 0 },
                   { new TypeInt64(),new TypeInt64(),new TypeCharN(10),
                     new TypeCharN(20),new TypeCharN(20),new TypeCharN(20),
                     new TypeCharN(2),new TypeCharN(9),new TypeFloat64(),
                     new TypeFloat64(),new TypeInt64()
                   }
                 };

Table history  = { NULL,0,0,0,0,8,
                   { 0 },
                   { new TypeInt64(),new TypeInt64(),new TypeInt64(),
                     new TypeInt64(),new TypeInt64(),new TypeDateTime(),
                     new TypeFloat64(),new TypeCharN(24)
                   }
                 };

Table item     = { NULL,0,0,0,0,5,
                   { 0 },
                   { new TypeInt64(),new TypeInt64(),new TypeCharN(24),
                     new TypeFloat64(),new TypeCharN(50)
                   }
                 };

Table new_order= { NULL,0,0,0,0,3,
                   { 0 },
                   { new TypeInt64(),new TypeInt64(),new TypeInt64()
                   }
                 };

Table order    = { NULL,0,0,0,0,8,
                   { 0 },
                   { new TypeInt64(),new TypeInt64(),new TypeInt64(),
                     new TypeInt64(),new TypeDateTime(),new TypeInt64(),
                     new TypeInt64(),new TypeInt64()
                   }
                 };

Table order_line={ NULL,0,0,0,0,10,
                   { 0 },
                   { new TypeInt64(),new TypeInt64(),new TypeInt64(),
                     new TypeInt64(),new TypeInt64(),new TypeInt64(),
                     new TypeDateTime(),new TypeInt64(),new TypeFloat64(),
                     new TypeCharN(24)
                   }
                 };

Table stock =    { NULL,0,0,0,0,17,
                   { 0 },
                   { new TypeInt64(),new TypeInt64(),new TypeInt64(),
                     new TypeCharN(24),new TypeCharN(24),new TypeCharN(24),
                     new TypeCharN(24),new TypeCharN(24),new TypeCharN(24),
                     new TypeCharN(24),new TypeCharN(24),new TypeCharN(24),
                     new TypeCharN(24),new TypeInt64(),new TypeInt64(),
                     new TypeInt64(),new TypeCharN(50)
                  }
                 };

Table warehouse= { NULL,0,0,0,0,9,
                   { 0 },
                   { new TypeInt64(),new TypeCharN(10),new TypeCharN(20),
                     new TypeCharN(20),new TypeCharN(20),new TypeCharN(2),
                     new TypeCharN(9),new TypeFloat64(),new TypeFloat64()
                   }
                 };

HashTable ht_customer;
HashTable ht_customer2;
HashTable ht_district;
HashTable ht_warehouse;
HashTable ht_item;
HashTable ht_stock;

HashTable ht_neworder;  // no_w_id,no_d_id non-pk
HashTable ht_orderline; // ol_o_id,ol_w_id,ol_d_id nonpk
HashTable ht_order;    // o_w_id,o_d_id,o_id pk
HashTable ht_order2;   // o_w_id,o_d_id,o_c_id nonpk

int initTables(void);
int initIndexs(void);
int loadData(const char *data_dir);
int loadIndexsFromTables(void);
int loadNewOrders(const char *trace_name, int trans_num, struct input_new_order_t* &neworders);
int runNewOrders(struct input_new_order_t *neworders, int neworder_number);
int loadTransactionsNP(const char *trace_name, int trans_num, struct input_new_order_t *&neworders, 
                       struct input_payment_t* &payments, int *&choice);
int runTransactionsNP (struct input_new_order_t *&neworders, 
                       struct input_payment_t* &payments, int trans_num,int *&choice);

int runNewOrder(struct input_new_order_t *neworder);
int runPayment(struct input_payment_t *payment);
int runDelivery(struct input_delivery_t *delivery);
int runOrderStatus(struct input_order_status_t *orderstatus);
int runStockLevel(struct input_stock_level_t *stocklevel);

int loadTransactions(const char *trace_name, int trans_num, struct input_new_order_t *&neworders, 
                     struct input_payment_t* &payments, struct input_delivery_t *&deleverys, 
                     struct input_order_status_t *&orderstatuss,struct input_stock_level_t *&stocklevels,int *&choice);
int runTransactions (struct input_new_order_t *&neworders, struct input_payment_t* &payments, struct input_delivery_t *&deleverys,
                     struct input_order_status_t *&orderstatuss,struct input_stock_level_t *&stocklevels,int trans_num,int *&choice);

int finalizeTables(void);
int finalizeIndexs(void);

int main (int argc,char *argv[]) 
{
    if (argc < 4) {
        printf("example: ./fasttpcn data_dir/ trace_name neworder_number\n");
           return 0;
    }
    int stat = 0;
    if ((stat = initTables ()) != 0) {
        printf("initTable error! stat: %d\n",stat);
        return -1;
    }
    if ((stat = initIndexs ()) != 0) {
        printf("initIndex error! stat: %d\n",stat);
        return -2;
    }
    if ((stat = loadData (argv[1])) != 0) {
        printf("loadData error! stat: %d\n",stat);
        return -3;
    }
    if ((stat = loadIndexsFromTables()) != 0) {
        printf("loadIndexFromTable error! stat: %d\n",stat);
        return -4;
    }
    int trans_number = -1;
    struct input_new_order_t *neworders = NULL;
    struct input_payment_t *payments = NULL;
    struct input_delivery_t *deliverys = NULL;
    struct input_order_status_t *orderstatuss = NULL;
    struct input_stock_level_t *stocklevels = NULL;
    int *choice = NULL;

    if ((trans_number = loadTransactionsNP(argv[2],atoi(argv[3]), neworders, payments,choice)) < 0) {
        printf ("loadTransactionsNP error! %d\n", trans_number);
        return -5;
    }
    if ((stat = runTransactionsNP (neworders,payments,trans_number,choice))) {
        printf ("runTransactionsNP error! %d\n", stat);
        return -6;
    }

    finalizeTables ();
    finalizeIndexs ();
    return 0;    
}

#define TPCC_WAREHOUSE_MAX_NUMBER (10)
#define TPCC_ITEM_MAX_NUMBER      (100000)
#define TPCC_NEWORDER_MAX_NUMBER  (1000000)
#define TPCC_HISTORY_MAX_NUMBER   (1000000)

#define TPCC_DISTRICT_MAX_NUMBER  (10*TPCC_WAREHOUSE_MAX_NUMBER)
#define TPCC_CUSTOMER_MAX_NUMBER  (3000*TPCC_DISTRICT_MAX_NUMBER)
#define TPCC_STOCK_MAX_NUMBER     (TPCC_WAREHOUSE_MAX_NUMBER*TPCC_ITEM_MAX_NUMBER)
#define TPCC_ORDER_MAX_NUMBER     (TPCC_NEWORDER_MAX_NUMBER)
#define TPCC_ORDERLINE_MAX_NUMBER (15*TPCC_NEWORDER_MAX_NUMBER)

int initTables(void) {
    customer.initialize(TPCC_CUSTOMER_MAX_NUMBER);
    district.initialize(TPCC_DISTRICT_MAX_NUMBER);
    history.initialize(TPCC_HISTORY_MAX_NUMBER);
    item.initialize(TPCC_ITEM_MAX_NUMBER);
    new_order.initialize(TPCC_NEWORDER_MAX_NUMBER);
    order.initialize(TPCC_ORDER_MAX_NUMBER);
    order_line.initialize(TPCC_ORDERLINE_MAX_NUMBER);
    stock.initialize(TPCC_STOCK_MAX_NUMBER);
    warehouse.initialize(TPCC_WAREHOUSE_MAX_NUMBER);
    return 0;
}

int finalizeTables(void) {
    customer.finalize();
    district.finalize();
    history.finalize();
    item.finalize();
    new_order.finalize();
    order.finalize();
    order_line.finalize();
    stock.finalize();
    warehouse.finalize();
    return 0;
}

int finalizeIndexs(void) {
    ht_customer.finalize();
    ht_customer2.finalize();
    ht_district.finalize();
    ht_warehouse.finalize();
    ht_item.finalize();
    ht_stock.finalize();
    ht_neworder.finalize();
    ht_orderline.finalize();
    ht_order.finalize();
    ht_order2.finalize();
    return 0;
}

int loadData(const char *data_dir) 
{
    char filename[1024];
    strcpy(filename,data_dir);strcat(filename,"customer.data");
    if(customer.loadData (filename)) return -2;
    strcpy(filename,data_dir);strcat(filename,"district.data");
    if(district.loadData (filename)) return -3;
    strcpy(filename,data_dir);strcat(filename,"history.data");
    if(history.loadData (filename)) return -4;
    strcpy(filename,data_dir);strcat(filename,"item.data");
    if(item.loadData (filename)) return -5;
    strcpy(filename,data_dir);strcat(filename,"new_order.data");
    if(new_order.loadData (filename)) return -6;
    strcpy(filename,data_dir);strcat(filename,"order.data");
    if(order.loadData (filename)) return -7;
    strcpy(filename,data_dir);strcat(filename,"order_line.data");
    if(order_line.loadData (filename)) return -8;
    strcpy(filename,data_dir);strcat(filename,"stock.data");
    if(stock.loadData (filename)) return -9;
    strcpy(filename,data_dir);strcat(filename,"warehouse.data");
    if(warehouse.loadData (filename)) return -10;
    return 0;
}

int loadNewOrders(const char *trace_name, int trans_num, struct input_new_order_t* &neworders)
{
    FILE *fp = fopen (trace_name, "r");
    if (fp == NULL) {
        printf ("[main][loadNewOrder][ERROR]: file name error! -1\n");
        return -1;
    }
    char tran_buffer[1024];
    int neworders_number = 0;
    neworders = new struct input_new_order_t[trans_num];
    while ((fread (tran_buffer,1024,1,fp)) > 0) {
        if (*(int*)tran_buffer == NEW_ORDER+100) {
            memcpy(&neworders[neworders_number], tran_buffer+sizeof(int), sizeof(struct input_new_order_t));
            neworders_number ++;   
            if (neworders_number >= trans_num) {
                fclose (fp);
                break;
            }
        }
    }
    return neworders_number;
}

int loadTransactionsNP (const char *trace_name, int trans_num, struct input_new_order_t *&neworders, 
                        struct input_payment_t* &payments, int *&choice) {
    FILE *fp = fopen (trace_name, "r");
    if (fp == NULL) {
        printf ("[main][loadTransactionsNP][ERROR]: file name error! -1\n");
        return -1;
    }
    char tran_buffer[1024];
    int load_number = 0;
    int neworders_number=0, payments_number=0;
    neworders = new struct input_new_order_t[trans_num];
    payments = new struct input_payment_t[trans_num];
    choice = new int[trans_num];
    while ((fread (tran_buffer,1024,1,fp)) > 0) {
        if (*(int*)tran_buffer == NEW_ORDER+100) {
            memcpy(&neworders[neworders_number], tran_buffer+sizeof(int), sizeof(struct input_new_order_t));
            neworders_number++;
            choice[load_number++] = 1;
            if (load_number >= trans_num) {
                fclose (fp);
                break;
            }
        }
        else if (*(int*)tran_buffer == PAYMENT+100) {
            memcpy(&payments[payments_number], tran_buffer+sizeof(int), sizeof(struct input_payment_t));
            payments_number ++;
            choice[load_number++] = 0;
            if (load_number >= trans_num) {
                fclose (fp);
                break;
            }
        }
    }
    return load_number;
}

int loadTransactions(const char *trace_name, int trans_num, struct input_new_order_t *&neworders, 
                     struct input_payment_t* &payments, struct input_delivery_t *&deliverys, 
                     struct input_order_status_t*& orderstatuss,struct input_stock_level_t *&stocklevels,int *&choice) 
{
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
        if (*(int*)tran_buffer == NEW_ORDER+100) {
            memcpy(&neworders[neworders_number], tran_buffer+sizeof(int), sizeof(struct input_new_order_t));
            neworders_number++;
            choice[load_number++] = NEW_ORDER;
            if (load_number >= trans_num) {
                fclose (fp);
                break;
            }
        }
        else if (*(int*)tran_buffer == PAYMENT+100) {
            memcpy(&payments[payments_number], tran_buffer+sizeof(int), sizeof(struct input_payment_t));
            payments_number ++;
            choice[load_number++] = PAYMENT;
            if (load_number >= trans_num) {
                fclose (fp);
                break;
            }
        }
        else if (*(int*)tran_buffer == DELIVERY+100) {
            memcpy(&deliverys[deliverys_number], tran_buffer+sizeof(int), sizeof(struct input_delivery_t));
            deliverys_number ++;
            choice[load_number++] = DELIVERY;
            if (load_number >= trans_num) {
                fclose (fp);
                break;
            }
        }
        else if (*(int*)tran_buffer == ORDER_STATUS+100) {
            memcpy(&orderstatuss[orderstatuss_number], tran_buffer+sizeof(int), sizeof(struct input_order_status_t));
            orderstatuss_number ++;
            choice[load_number++] = ORDER_STATUS;
            if (load_number >= trans_num) {
                fclose (fp);
                break;
            }
        }
        else if (*(int*)tran_buffer == STOCK_LEVEL+100) {
            memcpy(&stocklevels[stocklevels_number], tran_buffer+sizeof(int), sizeof(struct input_stock_level_t));
            stocklevels_number ++;
            choice[load_number++] = STOCK_LEVEL;
            if (load_number >= trans_num) {
                fclose (fp);
                break;
            }
        }
    }
    return load_number;
}

int runTransactions (struct input_new_order_t *&neworders, struct input_payment_t* &payments, struct input_delivery_t *&deleverys,
                     struct input_order_status_t *&orderstatuss,struct input_stock_level_t *&stocklevels,int trans_num,int *&choice)
{
    struct timeval start,end;
    gettimeofday (&start, NULL);
    int nn= 0, pp= 0, dd= 0,oo= 0,ss= 0, stat=-1;
    for (int ii = 0;ii < trans_num;ii ++) {
#ifdef DEBUG
        switch (choice[ii]) 
        {
            case 1: 
                    if ((stat = runNewOrder (&neworders[nn]) != 0)) {
                        printf ("[main][ERROR][runNewOrder]: neworder error! %d\n", ii);
                        return -1;
                    }
                    nn ++;
                    break;
            case 2: 
                    if ((stat = runPayment (&payments[pp]) != 0)) {
                        printf ("[main][ERROR][runPayment]: payment error! %d\n", ii);
                        return -1;
                    }
                    pp ++;
                    break;
            case 3:
                    if ((stat = runDelivery (&deleverys[dd]) != 0)) {
                        printf ("[main][ERROR][runDelivery]: delivery error! %d\n", ii);
                        return -1;
                    }
                    dd ++;
                    break;
            case 4:
                    if ((stat = runOrderStatus (&orderstatuss[oo]) != 0)) {
                        printf ("[main][ERROR][runOrderStatus]: order status error! %d\n", ii);
                        return -1;
                    }
                    oo ++;
                    break;
            case 5:
                    if ((stat = runStockLevel (&stocklevels[ss]) != 0)) {
                        printf ("[main][ERROR][runPayment]: payment error! %d\n", ii);
                        return -1;
                    }
                    ss ++;
                    break;
        }

#else
        switch (choice[ii]) 
        {
            case 1: runNewOrder (&neworders[nn++]);break;
            case 2: runPayment (&payments[pp++]);break;
            case 3: runDelivery (&deleverys[dd++]);break;
            case 4: runOrderStatus (&orderstatuss[oo++]);break;
            case 5: runStockLevel (&stocklevels[ss++]);break;
        }

#endif
    }
    gettimeofday (&end, NULL);
    unsigned long diff = 1000000*(end.tv_sec-start.tv_sec)+(end.tv_usec-start.tv_usec);
    double average = ((double)diff) / trans_num; 
    printf ("transactions: %d time use: %lu us average: %lf us\n",trans_num, diff,average);
    delete [] neworders;
    delete [] payments;
    delete [] deleverys;
    delete [] orderstatuss;
    delete [] stocklevels;
    delete [] choice;
    return 0;
}

int runTransactionsNP (struct input_new_order_t *&neworders, 
                       struct input_payment_t* &payments, int trans_num,int *&choice) {
    struct timeval start,end;
    gettimeofday (&start, NULL);
    int nn= 0, pp= 0;
    for (int ii = 0;ii < trans_num;ii ++) {
#ifdef DEBUG
        if (choice[ii]) {
            int stat = -1;
            if ((stat = runNewOrder (&neworders[nn]) != 0)) {
                printf ("[main][ERROR][runNewOrder]: neworder error! %d\n", ii);
                return -1;
            }
            nn ++;
        }
        else {
            int stat = -1;
            if ((stat = runPayment (&payments[pp]) != 0)) {
                printf ("[main][ERROR][runPayment]: payment error! %d\n", ii);
                return -1;
            }
            pp ++;
        }
#else
        if (choice[ii]) {
            runNewOrder (&neworders[nn++]);
        }
        else { 
            runPayment (&payments[pp++]);
        }
#endif
    }
    gettimeofday (&end, NULL);
    unsigned long diff = 1000000*(end.tv_sec-start.tv_sec)+(end.tv_usec-start.tv_usec);
    double average = ((double)diff) / trans_num; 
    printf ("transactions: %d time use: %lu us average: %lf us\n",trans_num, diff,average);
    delete neworders;
    delete payments;
    delete choice;
    return 0;
}

int runNewOrders(struct input_new_order_t *neworders, int neworders_number)
{
    struct timeval start,end;
    gettimeofday (&start, NULL);
    for (int ii = 0;ii < neworders_number;ii ++) {
#ifdef DEBUG
        int stat = -1;
        if ((stat = runNewOrder (&neworders[ii]) != 0)) {
            printf ("[main][ERROR][runNewOrders]: neworder error! %d\n", ii);
            return -1;
        }
#else
        runNewOrder (&neworders[ii]);
#endif
    }
    gettimeofday (&end, NULL);
    unsigned long diff = 1000000*(end.tv_sec-start.tv_sec)+(end.tv_usec-start.tv_usec);
    double average = ((double)diff) / neworders_number; 
    printf ("neworders: %d time use: %lu us average: %lf us\n", neworders_number,diff,average);
    delete neworders;
    return 0;
}

#define HASHTABLE_CUSTOMER (TPCC_CUSTOMER_MAX_NUMBER)
#define HASHTABLE_DISTRICT (TPCC_DISTRICT_MAX_NUMBER)
#define HASHTABLE_ITEM (TPCC_ITEM_MAX_NUMBER)
#define HASHTABLE_STOCK (TPCC_STOCK_MAX_NUMBER)
#define HASHTABLE_WAREHOUSE (TPCC_WAREHOUSE_MAX_NUMBER)

#define HASHTABLE_NEWORDER (TPCC_WAREHOUSE_MAX_NUMBER*DISTRICT_PER_WAREHOUSE)
#define HASHTABLE_ORDERLINE (TPCC_ORDERLINE_MAX_NUMBER)
#define HASHTABLE_ORDER (TPCC_ORDER_MAX_NUMBER)

int initIndexs(void) {
    ht_customer.initialize(HASHTABLE_CUSTOMER,1.0);
    ht_customer2.initialize(HASHTABLE_CUSTOMER,1.0);
    ht_district.initialize(HASHTABLE_DISTRICT,1.0);
    ht_item.initialize(HASHTABLE_ITEM,1.0);
    ht_stock.initialize(HASHTABLE_STOCK,1.0);
    ht_warehouse.initialize(HASHTABLE_WAREHOUSE,1.0);

    ht_neworder.initialize(HASHTABLE_NEWORDER,1.0);
    ht_orderline.initialize(HASHTABLE_ORDERLINE,1.0);
    ht_order.initialize (HASHTABLE_ORDER, 1.0);
    ht_order2.initialize (HASHTABLE_CUSTOMER,1.0);
    return 0;
}

int loadIndexsFromTables(void) {
    for (int ii= 0;ii< customer.record_number;ii++) {
        char *p = customer.getR (ii);
        int64_t k = getCustomerKey (p);
        ht_customer.add (k,p);
    }
#ifdef DEBUG
    printf ("ht_customer-\n");
    ht_customer.utilization();
    ht_customer.show();
#endif
    for (int ii= 0;ii< customer.record_number;ii++) {
        char *p = customer.getR (ii);
        int64_t k = getCustomerKey2 (p);
        ht_customer2.add (k,p);
    }
#ifdef DEBUG
    printf ("ht_customer2-\n");
    ht_customer2.utilization();
    ht_customer2.show();
#endif
    for (int ii= 0;ii< district.record_number;ii++) {
        char *p = district.getR (ii);
        int64_t k = getDistrictKey (p);
        ht_district.add (k,p);
    }
#ifdef DEBUG
    printf ("ht_district-\n");
    ht_district.utilization();
    ht_district.show();
#endif
    for (int ii= 0;ii< item.record_number;ii++) {
        char *p = item.getR (ii);
        int64_t k = getItemKey (p);
        ht_item.add (k,p);
    }
#ifdef DEBUG
    printf ("ht_item-\n");
    ht_item.utilization();
    ht_item.show();
#endif
    for (int ii= 0;ii< stock.record_number;ii++) {
        char *p = stock.getR (ii);
        int64_t k = getStockKey (p);
        ht_stock.add (k,p);
    }
#ifdef DEBUG
    printf ("ht_stock-\n");
    ht_stock.utilization();
    ht_stock.show();
#endif
    for (int ii= 0;ii< warehouse.record_number;ii++) {
        char *p = warehouse.getR (ii);
        int64_t k = getWarehouseKey (p);
        ht_warehouse.add (k,p);
    }
#ifdef DEBUG
    printf ("ht_warehouse-\n");
    ht_warehouse.utilization();
    ht_warehouse.show();
#endif
    for (int ii= 0;ii< new_order.record_number;ii++) {
        char *p = new_order.getR (ii);
        int64_t k = getNewOrderKey (p+sizeof(int64_t));
        ht_neworder.add (k,p);
    }
#ifdef DEBUG
    printf ("ht_neworder-\n");
    ht_neworder.utilization();
    ht_neworder.show();
#endif
    for (int ii= 0;ii< order_line.record_number;ii++) {
        char *p = order_line.getR (ii);
        int64_t k = getOrderLineKey (p);
        ht_orderline.add (k,p);
    }
#ifdef DEBUG
    printf ("ht_orderline-\n");
    ht_orderline.utilization();
    ht_orderline.show();
#endif
    for (int ii= 0;ii< order.record_number;ii++) {
        char *p = order.getR (ii);
        int64_t k = getOrderKey (p);
        ht_order.add (k,p);
        k = getOrderKey2 (p+sizeof(int64_t));
        ht_order2.add (k,p);
    }
#ifdef DEBUG
    printf ("ht_order-\n");
    ht_order.utilization();
    ht_order.show();
    printf ("ht_order2-\n");
    ht_order2.utilization();
    ht_order2.show();
#endif
    return 0;
}
int runNewOrder (struct input_new_order_t *neworder) {
    int q_wid = neworder->w_id;
    int q_did = neworder->d_id;
    int q_cid = neworder->c_id;
    int q_ol_cnt = neworder->o_ol_cnt;
    int q_o_all_local = neworder->o_all_local;
    /*========================
        SELECT w_tax
        INTO out_w_tax
        FROM warehouse
        WHERE w_id = tmp_w_id;
    =========================*/
    int64_t key = q_wid-1;
    char *match[4];
    int last = ht_warehouse.probe (key, match, 4);
#ifdef DEBUG
    printf ("key: %ld last: %d warehouse: %d\n",key, last,warehouse.record_number);
    ht_warehouse.show();
    assert (last==1);
#endif
    char *record = match[0];
    int offset = warehouse.getColumnOffset (7);
    char *column = record + offset;
    /*=========================    
        SELECT d_tax, d_next_o_id
        INTO out_d_tax, out_d_next_o_id
        FROM district   
        WHERE d_w_id = tmp_w_id
        AND d_id = tmp_d_id FOR UPDATE;
    ===========================*/
    key = (q_wid-1)*DISTRICT_PER_WAREHOUSE+q_did-1;
    last = ht_district.probe (key,match,4);
#ifdef DEBUG
    assert (last==1);
#endif
    record = match[0];
    offset = district.getColumnOffset (8);
    column = record + offset;
    offset = district.getColumnOffset (10);
    column = record + offset;
    int out_d_next_o_id = *(int64_t*)column;
    /*==========================
        UPDATE district
        SET d_next_o_id = d_next_o_id + 1
        WHERE d_w_id = tmp_w_id
        AND d_id = tmp_d_id;
    ============================*/
    *(int64_t*)column += 1;
    /*==========================
        SELECT c_discount , c_last, c_credit
        INTO out_c_discount, out_c_last, out_c_credit
        FROM customer
        WHERE c_w_id = tmp_w_id
        AND c_d_id = tmp_d_id
        AND c_id = tmp_c_id;
    ============================*/
    key = q_cid-1+((q_wid-1)*DISTRICT_PER_WAREHOUSE+q_did-1)*CUSTOMER_PER_DISTRICT;
    last = ht_customer.probe (key,match,4);
    if (last != 1) {
        printf ("[main][ERROR][runNewOrder]: input data customer key error! -1\n");
        return -1; // ROLLBACK
    }
    record = match[0];
    offset = customer.getColumnOffset(15);
    column = record + offset;
    /*==========================
        INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
        VALUES (out_d_next_o_id, tmp_d_id, tmp_w_id);
    ============================*/
    record = new_order.getR(new_order.record_number);
    *(int64_t*)record = (int64_t) out_d_next_o_id;
    *(int64_t*)(record+sizeof(int64_t)) = (int64_t) q_did;
    *(int64_t*)(record+2*sizeof(int64_t)) = (int64_t) q_wid;
    ht_neworder.add (getNewOrderKey(record+sizeof(int64_t)),record);
    new_order.record_number ++;
    /*==========================
        INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d,
	        o_carrier_id, o_ol_cnt, o_all_local)
        VALUES (out_d_next_o_id, tmp_d_id, tmp_w_id, tmp_c_id,
	        current_timestamp, NULL, tmp_o_ol_cnt, tmp_o_all_local);
    ============================*/
    int p = 0;
    record = order.getR(order.record_number);
    *(int64_t*)record = out_d_next_o_id;p += sizeof(int64_t);
    *(int64_t*)(record+p) = q_did;p += sizeof(int64_t);
    *(int64_t*)(record+p) = q_wid;p += sizeof(int64_t);
    *(int64_t*)(record+p) = q_cid;p += sizeof(int64_t);
    *(time_t*)(record+p) = 100000000;p += sizeof(int64_t);
    *(int64_t*)(record+p) = 0;p += sizeof(int64_t);
    *(int64_t*)(record+p) = q_ol_cnt; p += sizeof(int64_t);
    *(int64_t*)(record+p) = q_o_all_local;
    ht_order.add (getOrderKey(record),record);
    ht_order2.add (getOrderKey2(record+sizeof(int64_t)),record);
    order.record_number ++;

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
        key = tmp_i_id -1;
        last = ht_item.probe (key,match,4);
        if (last != 1) {
            q_ol.print ();
            printf ("key: %ld last: %d q_ol.ol_i_id: %d q_ol_cnt: %d ii: %ld\n",key, last, q_ol.ol_i_id,q_ol_cnt,ii);
            printf ("[main][ERROR][runNewOrder]: item not found! -2\n");
            // return -2; // ROLLBACK
            continue;
        }
        record = match[0];
        offset = item.getColumnOffset (3);
        double tmp_i_price = *(double*)(record+offset);
        double tmp_amount = tmp_i_price*tmp_ol_quantity;
 		/*========================
            SELECT s_quantity, s_dist_0N, s_data
		    INTO out_s_quantity, tmp_s_dist, tmp_s_data
		    FROM stock
		    WHERE s_i_id = in_ol_i_id
		    AND s_w_id = in_w_id;
        ==========================*/
        key = (q_wid-1)*MAXIMUM_ITEM_NUMBER+(tmp_i_id-1);
        last = ht_stock.probe (key,match,4);
        if (last != 1) {
            printf ("[main][ERROR][runNewOrder]: stock not found! -3\n");
            return -3;
        }
        record = match[0];
        offset = stock.getColumnOffset(2);
        int64_t *out_s_quantity = (int64_t*)(record+offset);
        char *tmp_s_dist = record+ stock.getColumnOffset(q_did+2); // 24
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
        if (*out_s_quantity> tmp_ol_quantity+10)
            *out_s_quantity -= tmp_ol_quantity;
        else
            *out_s_quantity = *out_s_quantity-tmp_ol_quantity+91;
        /*========================
	        INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id,
	                        ol_supply_w_id, ol_delivery_d, ol_quantity,
                                ol_amount, ol_dist_info)
	        VALUES (in_ol_o_id, in_d_id, in_w_id, in_ol_number, in_ol_i_id,
	            in_ol_supply_w_id, NULL, in_ol_quantity, in_ol_amount,
	            tmp_s_dist);
        ==========================*/        
        record = order_line.getR(order_line.record_number);p= 0;
        *(int64_t*)record = out_d_next_o_id;p += sizeof(int64_t);
        *(int64_t*)(record+p) = q_did;p += sizeof(int64_t);
        *(int64_t*)(record+p) = q_wid;p += sizeof(int64_t);
        *(int64_t*)(record+p) = ii;p += sizeof(int64_t);
        *(int64_t*)(record+p) = tmp_i_id;p += sizeof(int64_t);
        *(int64_t*)(record+p) = tmp_ol_supply_w_id;p += sizeof(int64_t);
        *(int64_t*)(record+p) = 0; p += sizeof(int64_t);
        *(int64_t*)(record+p) = tmp_ol_quantity;p+= sizeof(int64_t);
        *(double*)(record+p) = tmp_amount;p += sizeof(double);
        strncpy (record+p, tmp_s_dist, 24);
        ht_orderline.add (getOrderLineKey(record),record);
        order_line.record_number++;
        total_amount += tmp_amount;
    }
    return 0;
}
int runPayment (struct input_payment_t *payment) {
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
    int q_wid = payment->w_id;
    int q_did = payment->d_id;
    int q_cwid = payment->c_w_id;
    int q_cdid = payment->c_d_id;
    int q_cid = payment->c_id;
    char *q_clast = payment->c_last;
    double q_hamount = payment->h_amount;

    int64_t key = q_wid-1;
    char *match[4];
    int last = ht_warehouse.probe (key,match,4);
#ifdef DEBUG
    if (last != 1) {
        printf ("[main][ERROR][runPayment]: warehouse not found! -1\n");
        return -1;
    }
#endif
    char *record = match[0];
    int offset = warehouse.getColumnOffset(8);
    *(double*)(record+offset) += q_hamount;
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
    key = (q_wid-1)*DISTRICT_PER_WAREHOUSE+(q_did-1);
    last = ht_district.probe (key, match, 4);
#ifdef DEBUG
    if (last != 1) {
        printf ("[main][ERROR][runPayment]: district not found! -2\n");
        return -2;
    }
#endif
    record = match[0];
    offset = district.getColumnOffset(9);
    *(double*)(record+offset) += q_hamount;
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
        key = hash (q_clast,C_LAST_LEN);
	key = ((0x1L<<32)-1) & key;
        key += ((q_cwid-1)*DISTRICT_PER_WAREHOUSE+(q_cdid-1))*CUSTOMER_PER_DISTRICT;
        last = ht_customer2.probe (key,match,4);
        if (last < 0) record = match[2];
        else record = match[last/2];
        offset = customer.getColumnOffset(0);
        q_cid = *(int64_t*)(record+offset);
    }
    else {
        key = q_cid-1;
        key += ((q_cwid-1)*DISTRICT_PER_WAREHOUSE+(q_cdid-1))*CUSTOMER_PER_DISTRICT;
        last = ht_customer.probe (key,match,4);
#ifdef DEBUG
        if (last != 1) {
            printf ("[main][ERROR][runPayment]: customer not found! -3\n");
            return -3;
        }
#endif
        record = match[0];    
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
    offset = customer.getColumnOffset(16);
    *(double*)(record+offset) -= q_hamount;
    offset = customer.getColumnOffset(17);
    *(double*)(record+offset) += 1;
    /*=============================
        SET tmp_h_data = concat(out_w_name,' ', out_d_name);
        INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id,
                             h_date, h_amount, h_data)
        VALUES (out_c_id, in_c_d_id, in_c_w_id, in_d_id, in_w_id,
                current_timestamp, in_h_amount, tmp_h_data);
      =============================*/
    record = history.getR(history.record_number);
    offset = 0;
    *(int64_t*)record = q_cid;offset += sizeof(int64_t);
    *(int64_t*)(record+offset) = q_cdid;offset += sizeof(int64_t);
    *(int64_t*)(record+offset) = q_cwid;offset += sizeof(int64_t);
    *(int64_t*)(record+offset) = q_did;offset += sizeof(int64_t);
    *(int64_t*)(record+offset) = q_wid;offset += sizeof(int64_t);
    *(time_t*)(record+offset) = 100000000;offset += sizeof(time_t);
    *(double*)(record+offset) = q_hamount;offset += sizeof(double);
    strncpy (record+offset, "historyforpayment",24);
    history.record_number ++;
    return 0;
}
int runDelivery(struct input_delivery_t *delivery) 
{
    int q_w_id = delivery->w_id;
    int q_o_carrier_id = delivery->o_carrier_id;
    
    int64_t out_c_id, tmp_d_id, tmp_o_id=0;
    double  out_ol_amount = 0;

    for (int64_t tmp_d_id=1;tmp_d_id<=DISTRICT_PER_WAREHOUSE;tmp_d_id++) {
        tmp_o_id = 0;
        /*=============================
        SELECT no_o_id
        INTO tmp_o_id
        FROM new_order
        WHERE no_w_id = in_w_id AND no_d_id = tmp_d_id limit 1;
        ===============================*/
        int64_t key = (q_w_id-1)*DISTRICT_PER_WAREHOUSE+(tmp_d_id-1);
        char *match[16];
        int last = ht_neworder.probe (key,match,16);
        if (last == 0) 
            continue;
        char *record = match[0];
        tmp_o_id = *(int64_t*)record;
        if (*(int64_t*)record <= 0)
            continue;
        /*=============================
        DELETE FROM new_order 
        WHERE no_o_id = tmp_o_id 
         AND no_w_id = in_w_id 
         AND no_d_id = tmp_d_id;
        ===============================*/
        ht_neworder.del (getNewOrderKey(record), record);
        /*============================
        SELECT o_c_id
        INTO out_c_id
        FROM orders
        WHERE o_id = tmp_o_id
         AND o_w_id = in_w_id
         AND o_d_id = tmp_d_id;
        ==============================*/
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
        /*============================
        UPDATE orders
        SET o_carrier_id = in_o_carrier_id
        WHERE o_id = tmp_o_id
         AND o_w_id = in_w_id
         AND o_d_id = tmp_d_id;
        ==============================*/
        offset = order.getColumnOffset(5);
        *(int64_t*)(record+offset) = 10000000000;
        /*============================
        UPDATE order_line
        SET ol_delivery_d = current_timestamp
        WHERE ol_o_id = tmp_o_id
         AND ol_w_id = in_w_id
         AND ol_d_id = tmp_d_id;
        ==============================*/
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
        /*============================
        SELECT SUM(ol_amount * ol_quantity)
        INTO out_ol_amount
        FROM order_line
        WHERE ol_o_id = tmp_o_id
         AND ol_w_id = in_w_id
         AND ol_d_id = tmp_d_id;
        ==============================*/
        int offset1 = order_line.getColumnOffset(7);
        int offset2 = order_line.getColumnOffset(8);
        for (int ii=0; ii<last; ii++)
            out_ol_amount += (*(int64_t*)(match[ii]+offset1))*(*(double*)(match[ii]+offset2));
        /*============================
        UPDATE customer
        SET c_delivery_cnt = c_delivery_cnt + 1,
           c_balance = c_balance + out_ol_amount
        WHERE c_id = out_c_id
         AND c_w_id = in_w_id
         AND c_d_id = tmp_d_id;
        ==============================*/
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
    return 0;
}

int runOrderStatus(struct input_order_status_t *orderstatus)
{
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
        /*=============================
        SELECT c_id
        INTO out_c_id
        FROM customer
        WHERE c_w_id = in_c_w_id
          AND c_d_id = in_c_d_id
          AND c_last = in_c_last
          ORDER BY c_first ASC limit 1;
        =============================*/
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
    /*===============================
    SELECT c_first, c_middle, c_last, c_balance
	INTO out_c_first, out_c_middle, out_c_last, out_c_balance
	FROM customer
	WHERE c_w_id = in_c_w_id   
	  AND c_d_id = in_c_d_id
	  AND c_id = out_c_id;
    =================================*/
    strncpy (out_c_first,record+customer.getColumnOffset(3),255);
    strncpy (out_c_middle,record+customer.getColumnOffset(4),255);
    strncpy (out_c_last,record+customer.getColumnOffset(5),255);
    out_c_balance = *(double*)(record+customer.getColumnOffset(16));
    /*=============================== 
	SELECT o_id, o_carrier_id, o_entry_d, o_ol_cnt
	INTO out_o_id, out_o_carrier_id, out_o_entry_d, out_o_ol_cnt
	FROM orders
	WHERE o_w_id = in_c_w_id
  	AND o_d_id = in_c_d_id
  	AND o_c_id = out_c_id
	ORDER BY o_id DESC limit 1;
    =================================*/
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
    /*================================
        declare c cursor for SELECT ol_i_id, ol_supply_w_id, ol_quantity, 
                                    ol_amount, ol_delivery_d
                             FROM order_line
                             WHERE ol_w_id = in_c_w_id
                               AND ol_d_id = in_c_d_id
                               AND ol_o_id = out_o_id;
    ==================================*/
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
    return 0;
}

int runStockLevel(struct input_stock_level_t *stocklevel)
{
    int q_w_id = stocklevel->w_id;
    int q_d_id = stocklevel->d_id;
    int64_t q_threshold = stocklevel->threshold;
    /*=============================
    SELECT d_next_o_id
    INTO tmp_d_next_o_id
    FROM district
    WHERE d_w_id = in_w_id
      AND d_id = in_d_id;
    ===============================*/
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
    /*=============================
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
    ===============================*/
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
    return 0;
}
