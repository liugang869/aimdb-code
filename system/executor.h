/**
 * @file    executor.h
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *  
 * definition of executor
 *
 */

#ifndef _EXECUTOR_H
#define _EXECUTOR_H

#include "catalog.h"
#include "mymemory.h"

/** aggrerate method. */
enum AggrerateMethod {
    NONE_AM = 0, /**< none */
    COUNT,       /**< count of rows */
    SUM,         /**< sum of data */
    AVG,         /**< average of data */
    MAX,         /**< maximum of data */
    MIN,         /**< minimum of data */
    MAX_AM
};

/** compare method. */
enum CompareMethod {
    NONE_CM = 0,
    LT,        /**< less than */
    LE,        /**< less than or equal to */
    EQ,        /**< equal to */
    NE,        /**< not equal than */
    GT,        /**< greater than */
    GE,        /**< greater than or equal to */
    LINK,      /**< join */
    MAX_CM
};

/** definition of request column. */
struct RequestColumn {
    char name[128];    /**< name of column */
    AggrerateMethod aggrerate_method;  /** aggrerate method, could be NONE_AM  */
};

/** definition of request table. */
struct RequestTable {
    char name[128];    /** name of table */
};

/** definition of compare condition. */
struct Condition {
    RequestColumn column;   /**< which column */
    CompareMethod compare;  /**< which method */
    char value[128];        /**< the value to compare with, if compare==LINK,value is another column's name; else it's the column's value*/
};

/** definition of conditions. */
struct Conditions {
    int condition_num;      /**< number of condition in use */
    Condition condition[4]; /**< support maximum 4 & conditions */
};

/** definition of selectquery.  */
class SelectQuery {
  public:
    int64_t database_id;           /**< database to execute */
    int select_number;             /**< number of column to select */
    RequestColumn select_column[4];/**< columns to select, maximum 4 */
    int from_number;               /**< number of tables to select from */
    RequestTable from_table[4];    /**< tables to select from, maximum 4 */
    Conditions where;              /**< where meets conditions, maximum 4 & conditions */
    int groupby_number;            /**< number of columns to groupby */
    RequestColumn groupby[4];      /**< columns to groupby */
    Conditions having;             /**< groupby conditions */
    int orderby_number;            /**< number of columns to orderby */
    RequestColumn orderby[4];      /**< columns to orderby */
};  // class SelectQuery

/** definition of result table.  */
class ResultTable {
  public:
    int column_number;       /**< columns number that a result row consist of */
    BasicType **column_type; /**< each column data type */
    char *buffer;         /**< pointer of buffer alloced from g_memory */
    int64_t buffer_size;  /**< size of buffer, power of 2 */
    int row_length;       /**< length per result row */
    int row_number;       /**< current usage of rows */
    int row_capicity;     /**< maximum capicity of rows according to buffer size and length of row */
    int *offset;
    int offset_size;

    /**
     * init alloc memory and set initial value
     * @col_types array of column type pointers
     * @col_num   number of columns in this ResultTable
     * @param  capicity buffer_size, power of 2
     * @retval >0  success
     * @retval <=0  failure
     */
    int init(BasicType *col_types[],int col_num,int64_t capicity = 1024);
    /**
     * calculate the char pointer of data spcified by row and column id
     * you should set up column_type,then call init function
     * @param row    row id in result table
     * @param column column id in result table
     * @retval !=NULL pointer of a column
     * @retval ==NULL error
     */
    char* getRC(int row, int column);
    /**
     * write data to position row,column
     * @param row    row id in result table
     * @param column column id in result table
     * @data data pointer of a column
     * @retval !=NULL pointer of a column
     * @retval ==NULL error
     */
    int writeRC(int row, int column, void *data);
    /**
     * print result table, split by '\t', output a line per row 
     * @retval the number of rows printed
     */
    int print(void);
    /**
     * write to file with FILE *fp
     */
    int dump(FILE *fp);
    /**
     * free memory of this result table to g_memory
     */
    int shut(void);
};  // class ResultTable

/** pure virtual base class for different operators  */ 
class Operator {
protected:
    RowTable *tab_in[4] = { NULL, NULL, NULL, NULL }; /**< row tables in */
    RowTable *tab_out   = NULL;                       /**< row table out */
public:
    virtual ~Operator() = default;
    virtual bool init() = 0;
    virtual	bool getNext(char *ptr) = 0;
    virtual	bool isEnd() = 0;
    virtual bool close() = 0;
    RowTable *getRowTableOut() { return tab_out; }
};

/** definition of class HashJoin. */
class HashJoin : public Operator {
private:
    bool first_time = true;                       /**< row tables in */
    bool is_end[4] = { false };                   /**< the first time to get next row */
    char *current_ptr;                            /**< current ptr of result table */
    int con_num = 0;                              /**< number of compare columns */
    int op_num = 0;                               /**< number of child operators */
    Operator *op[4] = { NULL, NULL, NULL, NULL }; /**< child operators */
    CompareMethod method[4];                      /**< compare methods */
    int64_t col_rank[4] = { -1, -1, -1, -1 };     /**< rank of compare columns */
    BasicType *compare_val_type[4];               /**< type of compare columns */
    char compare_val[4][128];                     /**< value of compare columns */
    int out_num_left = 0;                         /**< number of left columns */
    HashInfo this_hash_info;                      /**< HashInfo for hashindex operations */
    HashIndex *this_hash_index;                   /**< HashIndex for join based on hash table */
    HashTable *ht;                                /**< hash table */
    std::vector<char *> pos;                      /**< vectors of address of result rows */
    bool is_use_self_ht = false;                  /**< flag of whether use hashindex of itself  */
    char *this_match[3000];                       /**< match result of hashtable::probe */
    int hash_count = 0;                           /**< count of hash */
public:
    /**
     * destructor
     */
    ~HashJoin() = default;
    /**
     * constructor. 
     * @param Op_num number of operators in
     * @param Op result from last step
     * @param Con_num number of conditions
     * @param Cond conditions
     */
    HashJoin(int Op_num, Operator **Op, int Con_num, Condition **Cond);
    bool init();
    /**
     * get next record
     */
    bool getNext(char *ptr);
    /**
     * check whether it's the end
     */
    bool isEnd() { return op[0]->isEnd() && (out_num_left == 0); }
    /**
     * hash function
     * @param key key to hash
     * @param size size of key
     */
    int64_t multhash(char *key, int size) {
        unsigned char *new_key = (unsigned char *)key;
        int64_t hash = 5381L;
        for (int i = 0; i < size; i++)
            hash = (31 * hash + *(new_key + i)) % 1000000 + 3;
        return hash;
    }
    /**
     * free the memory and close operator
     */
    bool close();
};

/** definition of class Join. */
class Join : public Operator {
private:
    bool first_time = true;                            /**< flag of whether the first time to get next row  */
    bool is_end[4] = { false };                        /**< flag of end of every child operator  */
    char *current_ptr;                                 /**< current ptr of result table  */
    int con_num = 0;                                   /**< number of conditions  */
    int op_num = 0;                                    /**< number of child operators  */
    Operator *op[4] = { NULL, NULL, NULL, NULL };      /**< child operators  */
    CompareMethod method[4];                           /**< compare method of conditions  */
    int64_t col_rank[4] = { -1, -1, -1, -1 };          /**< rank of compare columns  */
    BasicType *compare_val_type[4];                    /**< types of compare columns  */
    char compare_val[4][128];                          /**< value of compare columns  */
public:
    /**
     * destructor
     */
    ~Join() = default;
    /**
     * constructor. 
     * @param Op_num number of ops in
     * @param Op result from last step
     * @param Con_num number of conditions
     * @param Cond join conditions
     */
    Join(int Op_num, Operator **Op, int Con_num, Condition **Cond);
    bool init() { 
        current_ptr = NULL;
        first_time = true;
        for (int i = 0; i < 4; i++)
            is_end[i] = false;
        return true; 
    }
    /**
     * get next record
     */
    bool getNext(char *ptr);
    /**
     * check whether it's the end
     */
    bool isEnd() { 
        bool is_end =  true;
        for (int i = 0; i < op_num; i++)
             is_end = is_end && op[i]->isEnd();
        return is_end;
    }
    /**
     * free memory and close operator
     */
    bool close() {
        bool result = true;
        for (int i = 0; i < op_num; i++) {
            result = result && op[i]->close();
            delete op[i];
        }
        g_memory.free(current_ptr, tab_out->getRPattern().getRowSize());
        return result;
    }
};

/** definition of class Project. */
class Project : public Operator {
private:
    Operator *op = NULL;      /**< operator from last step */
    int col_num = 0;          /**< number of selected columns */
    int64_t col_rank[5];      /**< column ranks of selected columns in tab_in */
    /**
     * get Project table name. We have to ensure there're no name conflicts
     * in case of multiple Project operator instantiations.
     */
    static std::string getTableName() {
        static int64_t num = 0;
        std::string name = "project_table_" + std::to_string(num);
        return name;
    }
public:
    /**
     * destructor
     */
    ~Project() = default;
    /**
     * constructor. 
     * @param m_op result from last step
     * @param m_col_num number of cols to project to
     * @param m_req Request columns
     */
    Project(Operator *m_op, int m_col_num, RequestColumn *m_req);
    /**
     * init operator, i.e. get data from the beginning
     */
    bool init() { return op->init(); }
    /**
     * get next record
     */
    bool getNext(char *ptr);
    /**
     * check whether it's the end
     */
    bool isEnd() { return op->isEnd(); }
    /**
     * free the memory and close operator
     */
    bool close() {
        bool result = op->close();
        delete op;
        delete tab_out;
        return result;
    }
};

/** definition of class OrderBy. */
class OrderBy : public Operator {
private:
    static const int step = 64;      /**< basic step for buffer */
    Operator *op = NULL;             /**< op from last step */
    char *buf = NULL;                /**< buffer to store result */
    std::vector<char *> pos;         /**< ordered positions in buf */
    size_t buf_size = 0;             /**< buffer size */
    size_t row_size = 0;             /**< row size */
    size_t index = 0;                /**< current row index in buf */
    int cmp_num = 0;                 /**< number of columns to order by */
    size_t offset[4];                /**< offset of the selected ordered columns */
    BasicType *col_type[4];          /**< type of the selected ordered columns */
    /**
     * compare rows and return a number
     * @retval >0 p is less than q
     * @retval <0 p is greater than q
     */
    int compare(char *p, char *q);
public:
    /**
     * destructor
     */
    ~OrderBy() = default;
    /**
     * constructor. 
     * @param m_op result from last step
     * @param num number of columns to order
     * @param orderby columns to order by
     */
    OrderBy(Operator *m_op, int num, RequestColumn orderby[4]);
    /**
     * init operator, i.e. get data from the beginning
     */
    bool init() { index = 0; return true; }
    /**
     * get next record
     */
    bool getNext(char *ptr) {
        if (index >= pos.size())
            return false;
        memcpy(ptr, this->pos[this->index], this->row_size);
        this->index++;
        return true;
    }
    /**
     * check whether it's the end
     */
    bool isEnd() { return (index == pos.size()); }
    /**
     * close operator, free memory
     */
    bool close() {
        bool result = op->close();
        g_memory.free(buf, buf_size);
        return result;
    }
};

/** definition of class GroupBy. */
class GroupBy : public Operator{
private:
    static const int step = 64;      /**< Init row number of buffer  */
    int groupby_num = 0;             /**< number of selected colums  */
    int aggr_num = 0;                /**< number of colums with aggregate methods  */
    int noaggr_num = 0;              /**< number of colums to group by  */
    RequestColumn groupby[4];        /**< selected colums  */
    Operator *op = NULL;             /**< Operator of child node  */
    char *buf = NULL;                /**< buffer of result table  */
    size_t buf_size = 0;             /**< size of buffer  */
    size_t row_size = 0;             /**< size of row  */
    size_t index = 0;                /**< index of getnext()  */
    size_t noaggr_offset[4];         /**< offset of columns without aggregate methods  */
    size_t aggr_offset[4];           /**< offset of columns  with aggregate methods  */
    BasicType *noaggr_type[4];       /**< type of columns without aggregate methods  */
    BasicType *aggr_type[4];         /**< type of columns with aggregate methods  */
    AggrerateMethod am[4];           /**< aggregate method of colums  */
    uint64_t row_num = 0;            /**< number of row in result table  */
    int64_t multhash(char *str, int64_t maxlen);                       /**< multhash function */
    bool plus(void *a1, void *a2, void *dest, BasicType *type);        /**< add funtion for SUM */
    bool avg(void *a1, int64_t count, void *dest, BasicType *type);    /**< average function for AVG */
	bool check(char *a1, char *a2);                                    /**< check funtion for hash table */
public:
    /**
     * destructor
     */
    ~GroupBy() = default;
	/**
     * constructor. 
     * @param m_op result from last step
     * @param num number of selected columns
     * @param orderby selected columns 
     */
    GroupBy(Operator *m_op, int num, RequestColumn groupby[4]);
    /**
     * init operator, i.e. get data from the beginning
     */
    bool init() { index = 0; return true; }
    /**
     * check whether it's the end
     */
    bool isEnd() { return (index == this->row_num); }
    
    /**
     * get next record
     */
    bool getNext(char * ptr)
    {
        if (index >= this->row_num)
            return false;
        memcpy(ptr, this->buf + this->index * this->row_size, this->row_size);
        this->index++;
        return true;

    }
    /**
     * close operator, free memory
     */
    bool close() {
        bool result = op->close();
        g_memory.free(buf, buf_size);
        return result;
    }
};

/** definition of class Filter. */
class Filter : public Operator {
private:
    Operator *op = NULL;             /**< op from last step */
    int64_t col_rank = -1;           /**< column rank of the column in the condtion */
    CompareMethod method;            /**< compare method of the filter condition */
    BasicType *compare_val_type;     /**< value type of the filter condition */
    char compare_val[128];           /**< compare value of the filter condition */
public:
    /**
     * destructor
     */
    ~Filter() = default;
    /**
     * constructor. 
     * @param m_op result from last step
     * @param m_cond filte condition
     */
    Filter(Operator *m_op, Condition *m_cond);
    /**
     * constructor. 
     * @param m_op result from last step
     * @param m_cond filte condition
     * @param req_num number of columns in last step
     * @param reg columns in last step
     */
    Filter(Operator *m_op, Condition *m_cond, int req_num, RequestColumn *req);
    /**
     * init operator, i.e. get data from the beginning
     */
    bool init() { return op->init(); }
    /**
     * get next record
     */
    bool getNext(char *ptr);
    /**
     * check whether it's the end
     */
    bool isEnd() { return op->isEnd(); }
    /**
     * close operator, free memory
     */
    bool close() {
        bool result = op->close();
        return result;
    }
};
/** definition of class Scan. */
class Scan : public Operator {
private:
    int64_t index = 0;     /**< progress of current execution */
public:
    /**
     * destructor
     */
    ~Scan() = default;
    /**
     * constructor. 
     * @param RowTable to scan
     */
    Scan(RowTable *aTab) { tab_in[0] = aTab; tab_out = aTab; }
    /**
     * init operator, i.e. get data from the beginning
     */
    bool init() { index = 0; return true; }
    /**
     * get next record
     */
    bool getNext(char *ptr) {
        if (!isEnd() && tab_in[0]->select(index, ptr)) {
            index++;
            return true;
        }
        return false;
    }
    /**
     * check whether it's the end
     */
    bool isEnd() { return (index == tab_in[0]->getRecordNum()); }
    /**
     * close operator, free memory
     */
    bool close() { return true; }
};

/** definition of class executor.  */
class Executor {
private:
    SelectQuery *current_query = NULL;  /**< selectquery to iterately execute */
    Operator *result_op = NULL;         /**< current operator of execution */
    int64_t result_num = 128;           /**< number of results shown every execution */
    BasicType **result_type = NULL;     /**< types of result columns */
    /**
     * get the rowtables the columns belong to, returns vector of int64_t
     * @param  query query to parse
     * @param  tid array to save table id, -1 means do not exist
     * @result number of conditions for join
     */
    int parse_where(int64_t *tid, Condition **cond);
public:
    Executor() = default;
    Executor(int64_t num): result_num(num) { }
    /**
     * exec function.
     * @param  query to execute, if NULL, execute query at last time 
     * @result result table generated by an execution, store result in pattern defined by the result table
     * @retval >0  number of result rows stored in result
     * @retval <=0 no more result
     */
    virtual int exec(SelectQuery *query, ResultTable *result);
    /**
     * set number of results shown in every commit
     * @param number of results shown every time
     */
    void set_result_num(int64_t num) { result_num = num; }
    /**
     * close Executor to free allocated memory
     */
    void close() {
        result_op->close();
        delete result_op;
    }
};

/**
 * round up to power of 2
 * @param size integer to round
 */
int64_t round2(int64_t size);

#endif
