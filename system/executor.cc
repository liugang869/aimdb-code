/**
 * @file    executor.cc
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *  
 * definition of executor
 *
 */
#include <algorithm>
#include "executor.h"
#include "assert.h"

/** 
 * README 注意：
 * executor 的正常使用以其他代码的正确性为前提，需要修改如下文件：
 * 
 * - rowtable.h:35
 *     改为 int64_t rp_current = 0; 
 *     理由如下：
 *         rp_current 指示当前位置，但它从未被初始化，需要保证被初始化为 0
 * 
 * - runaimdb_n.cc:777/runaimdb_s.cc:627
 *     testOne 函数从 while 开始改为
 * 
 *     while (stat > 0) {
 *         result.print();
 *         result.dump(fp);
 *         result.shut();                          // 增加
 *         stat = executor.exec(NULL, &result);
 *     }
 *     fprintf(fp, "\n");                          // 增加
 *     fclose (fp);
 *     result.shut();                              // 增加
 *     executor.close();                           // 增加
 *  
 *     其中增加的行在右侧表明了增加。
 *     理由如下：
 *         关闭 ResultTable：每次使用完进行下一次 exec 都需要关闭，避免内存泄漏
 *         向文件中写入"\n"：满足 runcheck.py 脚本要求
 *         关闭 executor：executor 并不知道何时自己的工作结束，
 *                       因此需要添加 close() 函数以释放空间，
 *                       避免内存泄漏
 * 
 * - 其他问题
 *     我们通过下面的函数在 executor.cc 中实现修改
 */

// note that we're not allowed to modify other files
// we have to implement some helper functions here

/** 
 * round `size` up to power of 2
 * @param number to round up
 */
int64_t round2(int64_t size) {
    if (size < 0)
        return -1;
    int64_t result = 1;
    while (result < size)
        result *= 2;
    return result;
}

// Since AIMDB library doesn't provide getColumnNumber(), we have to 
// implementation it by other means
/**
 * get number of columns of RowTable
 * @param tab RowTable to get column number
 */
inline int64_t getColumnNumber(Table *tab) {
    return tab->getColumns().size();
}

// Since the stupid getObjByName(char *) doesn't accept const char *,
// we have to change const char * to char *
/**
 * change const char * to char *
 * @param str str to change
 */
inline char *remove_const(const char *str) {
    size_t len = strlen(str);
    char *result = new char[len + 1];
    memcpy(result, str, len + 1);
    return result;
}


/** executor implementations start here  */

/**
 * parse where condtions in current_query.
 * @param tid array of table id in conditions. ids of columns in join conditions will not be updated.
 * @param cond array of join conditions
 * @retval number of join condtions
 */
int Executor::parse_where(int64_t *tid, Condition **cond) {
    int count = 0;
    for (int i = 0; i < current_query->where.condition_num; i++) {
        tid[i] = -1;
        if (current_query->where.condition[i].compare == LINK) {
            // found a join condition
            cond[count++] = &current_query->where.condition[i];
            continue;
        }
        auto obj = g_catalog.getObjByName(current_query->where.condition[i].column.name);
        // assert(obj != NULL)
        auto cid = obj->getOid();
        // note that it's not allowed (undefined) for cid to occur in more than one table
        for (int j = 0; j < current_query->from_number; j++) {
            auto tab = (Table *)g_catalog.getObjByName(current_query->from_table[j].name);
            auto cols = tab->getColumns();
            // find the column in table
            for (auto col: cols) {
                if (col == cid) {
                    tid[i] = tab->getOid();
                    break;
                }
            }
        }
    }
    return count;
}

int Executor::exec(SelectQuery *query, ResultTable *result)
{
    if (query != NULL) {
        // remove last op and assign new query
        if (result_op != NULL) {
            result_op->close();
            result->shut();
            delete result_op;
        }
        current_query = query;
        /** build operator tree
         *  Our tree looks like this:
         * 
         *                 Filter (for aggregated columns)
         *                                |
         *                             GroupBy
         *                                |
         *                             OrderBy
         *                                |
         *                             Project
         *                                |
         *                              Filter
         *  (in case of IndexNestedLoopJoin that uses the total table for join)
         *                                |
         *      Join(IndexNestedLoopJoin) / HashJoin (simple hash)
         *                                |
         *                 ---------------|------------------
         *                 |          |          |          | 
         *               Filter    Filter     Filter     Filter   (local ones)
         *                 |          |          |          |
         *               Scan       Scan       Scan       Scan
         * 
         */
        // parse where condtions
        int64_t tid[4] = { -1, -1, -1, -1 };
        Condition *cond[4];
        auto count = this->parse_where(tid, cond);
        // build scan and local filter
        Operator *top[4];
        for (int i = 0; i < current_query->from_number; i++) {
            RowTable *rt = (RowTable *)g_catalog.getObjByName(current_query->from_table[i].name);
            top[i] = new Scan(rt);
            for (int j = 0; j < 4; j++) {
                if (tid[j] == rt->getOid()) {
                    top[i] = new Filter(top[i], current_query->where.condition + j);
                }
            }
        }
        // decide whether Join or HashJoin is to be used
        if (count == 1) {
            top[0] = new HashJoin(current_query->from_number, top, count, cond);
        }
        else if (count == 2) {
            RowTable *tab[4];
            bool used[4]=  { false };
            int64_t id1 = g_catalog.getObjByName(cond[0]->column.name)->getOid();
            int64_t id2 = g_catalog.getObjByName(cond[0]->value)->getOid();
            Operator *que_op[2];
            for (int i = 0; i < current_query->from_number; i++ ) {
                tab[i] = top[i]->getRowTableOut();
                auto &col = tab[i]->getColumns();
                if (std::find(col.begin(), col.end(), id1) != col.end()) {
                    que_op[0] = top[i];
                    used[i] = true;
                }
                if (std::find(col.begin(), col.end(), id2) != col.end()) {
                    que_op[1] = top[i];
                    used[i] = true;
                }
            }
            que_op[0] = new HashJoin(2, que_op, 1, cond);
            for (int i = 0; i < current_query->from_number; i++)
                if (!used[i])
                    que_op[1] = top[i]; 
            top[0] = new HashJoin(2, que_op, 1, &cond[1]);

         }
        else if (count == 3) {
            RowTable *tab[4];
            bool used[4]=  { false };
            int64_t id1 = g_catalog.getObjByName(cond[2]->column.name)->getOid();
            int64_t id2 = g_catalog.getObjByName(cond[2]->value)->getOid();
            Operator *que_op[2];
            for (int i = 0; i<current_query->from_number; i++){
                tab[i] = top[i]->getRowTableOut();
                auto &col = tab[i]->getColumns();
                if (std::find(col.begin(), col.end(), id1) != col.end()) {
                    que_op[0] = top[i];
                    used[i] = true;
                }
                if (std::find(col.begin(), col.end(), id2) != col.end()) {
                    que_op[1] = top[i];
                    used[i] = true;
                }
            }
            Operator *temp1 = new HashJoin(2, que_op, 1, &(cond[2]));
            Operator *que_op2[2];            
            Operator *new_top[3];
            int k = 0;
            for (int i = 0; i < current_query->from_number; i++) {
                if (used[i] == false){
                    new_top[k] = top[i];
                    k++;
                }
            }
            new_top[2] = temp1;
            bool new_used[4]=  { false };
            id1 = g_catalog.getObjByName(cond[1]->column.name)->getOid();
            id2 = g_catalog.getObjByName(cond[1]->value)->getOid();
            for (int i = 0; i < current_query->from_number - 1; i++) {
                tab[i] = new_top[i]->getRowTableOut();
                auto &col = tab[i]->getColumns();
                if (std::find(col.begin(), col.end(), id1) != col.end()) {
                    que_op2[0] = new_top[i];
                    new_used[i] = true;
                }
                if (std::find(col.begin(), col.end(), id2) != col.end()) {
                    que_op2[1] = new_top[i];
                    new_used[i] = true;
                }
            }
            if (que_op2[1] == temp1) {
                Operator *t = que_op2[1]; 
                que_op2[1] = que_op2[0];
                que_op2[0] = t;
            }
            Operator *temp = new HashJoin(2, que_op2, 1, &(cond[1]));
            Operator *que_op3[2] = { temp, NULL };  
            for (int i = 0; i < current_query->from_number - 1; i++)
                if (!new_used[i]) {
                    que_op3[1] = new_top[i]; 
                }
            top[0] = new HashJoin(2, que_op3, 1, cond);
         }
        for (int j = 0; j < 4; j++) {
            if (tid[j] != -1) {
                top[0] = new Filter(top[0], current_query->where.condition + j);
            }
        }
        if (current_query->select_number > 0)
            top[0] = new Project(top[0], current_query->select_number, current_query->select_column);
        if (current_query->orderby_number > 0)
            top[0] = new OrderBy(top[0], current_query->orderby_number, current_query->orderby);
        if (current_query->groupby_number > 0)
            top[0] = new GroupBy(top[0], current_query->select_number, current_query->select_column);
        if (current_query->having.condition_num > 0)
            top[0] = new Filter(top[0], current_query->having.condition, current_query->select_number, current_query->select_column);
        // assign new result operator
        result_op = top[0];
    }
    // init ResultTable format
    auto rp = result_op->getRowTableOut()->getRPattern();
    int64_t row_size = round2(rp.getRowSize());
    auto col_num = getColumnNumber(result_op->getRowTableOut());
    if (result_type != NULL)
        delete result_type;
    result_type = new BasicType *[col_num];
    for (int i = 0; i < col_num; i++) {
        result_type[i] = rp.getColumnType(i);
    }
    result->init(result_type, col_num, row_size * result_num);
    // put results into ResultTable
    char *buf;
    g_memory.alloc(buf, row_size);
    int64_t i = 0;
    for (; i < result_num && result_op->getNext(buf); i++) {
        for (int64_t j = 0; j < col_num; j++)
            result->writeRC(i, j, buf + rp.getColumnOffset(j));
        result->row_number++;
    }
    g_memory.free(buf, row_size);
    return i;
}

// note: you should guarantee that col_types is useable as long as this ResultTable in use, maybe you can new from operate memory, the best method is to use g_memory.
int ResultTable::init(BasicType *col_types[], int col_num, int64_t capicity) {
    column_type = col_types;
    column_number = col_num;
    row_length = 0;
    buffer_size = g_memory.alloc (buffer, capicity);
    if(buffer_size != capicity) {
        printf ("[ResultTable][ERROR][init]: buffer allocate error!\n");
        return -1;
    }
    int allocate_size = 1;
    int require_size = sizeof(int)*column_number*2;
    while (allocate_size < require_size)
        allocate_size = allocate_size << 1;
    char *p = NULL;
    offset_size = g_memory.alloc(p, allocate_size);
    if (offset_size != allocate_size) {
        printf ("[ResultTable][ERROR][init]: offset allocate error!\n");
        return -2;
    }
    offset = (int*) p;
    for(int ii = 0;ii < column_number;ii ++) {
        offset[ii] = row_length;
        row_length += column_type[ii]->getTypeSize(); 
    }
    row_capicity = (int)(capicity / row_length);
    row_number   = 0;
    return 0;
}

int ResultTable::print (void) {
    int row = 0;
    int ii = 0;
    char buffer[1024];
    char *p = NULL; 
    while(row < row_number) {
        for( ; ii < column_number-1; ii++) {
            p = getRC(row, ii);
            column_type[ii]->formatTxt(buffer, p);
            printf("%s\t", buffer);
        }
        p = getRC(row, ii);
        column_type[ii]->formatTxt(buffer, p);
        printf("%s\n", buffer);
        row ++; ii=0;
    }
    return row;
}

int ResultTable::dump(FILE *fp) {
    // write to file
    int row = 0;
    int ii = 0;
    char buffer[1024];
    char *p = NULL; 
    while(row < row_number) {
        for( ; ii < column_number-1; ii++) {
            p = getRC(row, ii);
            column_type[ii]->formatTxt(buffer, p);
            fprintf(fp,"%s\t", buffer);
        }
        p = getRC(row, ii);
        column_type[ii]->formatTxt(buffer, p);
        fprintf(fp,"%s\n", buffer);
        row ++; ii=0;
    }
    return row;
}

// this include checks, may decrease its speed
char* ResultTable::getRC(int row, int column) {
    return buffer+ row*row_length+ offset[column];
}

int ResultTable::writeRC(int row, int column, void *data) {
    char *p = getRC (row,column);
    if (p==NULL) return 0;
    return column_type[column]->copy(p,data);
}

int ResultTable::shut (void) {
    // free memory
    if (buffer) {
        g_memory.free (buffer, buffer_size);
    }
    if (offset) {
        g_memory.free ((char*)offset, offset_size);
    }
    return 0;
}

//---------------------operators implementation---------------------------
Filter::Filter(Operator *m_op, Condition *m_cond) {
    op = m_op;
    method = m_cond->compare; // ASSERT(method != LINK)
    tab_out = tab_in[0] = m_op->getRowTableOut();
    // calculate column rank
    Object *col = g_catalog.getObjByName(m_cond->column.name);
    col_rank = tab_out->getColumnRank(col->getOid());
    // decode compare object
    compare_val_type = tab_out->getRPattern().getColumnType(col_rank);
    char temp[128];
    compare_val_type->formatBin(temp, m_cond->value);
    memcpy(compare_val, temp, 128);
}

Filter::Filter(Operator *m_op, Condition *m_cond, int req_num, RequestColumn *m_req) {
    op = m_op;
    method = m_cond->compare; // ASSERT(method != LINK)
    tab_out = tab_in[0] = m_op->getRowTableOut();
    // calculate column rank
    for (int i = 0; i <req_num; i++) {
        auto aggr_eq = (m_req[i].aggrerate_method == m_cond->column.aggrerate_method);
        auto name_eq = !strcmp(m_req[i].name, m_cond->column.name);
        if (aggr_eq && name_eq) {
            col_rank = i;
            break;
        }
    }
    // decode compare object
    compare_val_type = tab_out->getRPattern().getColumnType(col_rank);
    char temp[128];
    compare_val_type->formatBin(temp, m_cond->value);
    memcpy(compare_val, temp, 128);
}

bool Filter::getNext(char *ptr) {
    while (op->getNext(ptr)) {
        uint64_t offset = tab_out->getRPattern().getColumnOffset(col_rank);
        char *temp = ptr + offset;
        bool check;
        switch (method) {
            case LT:
                check = compare_val_type->cmpLT(temp, compare_val);
                break;
            case LE:
                check = compare_val_type->cmpLE(temp, compare_val);
                break;
            case EQ:
                check = compare_val_type->cmpEQ(temp, compare_val);
                break;
            case NE:
                check = !compare_val_type->cmpEQ(temp, compare_val);
                break;
            case GT:
                check = compare_val_type->cmpGT(temp, compare_val);
                break;
            case GE:
                check = compare_val_type->cmpGE(temp, compare_val);
                break;
            default:
                check = false;
                break;
        }
        if (check)
            return true;
    }
    return false;
}

HashJoin::HashJoin(int Op_num, Operator **Op, int Con_num,  Condition **Cond){
    int col_num = 0;
    for (int i = 0; i < Op_num; i++)
        op[i] = Op[i];
    op_num = Op_num;
    con_num = Con_num;
    // pre check
    Object *col = g_catalog.getObjByName(Cond[0]->column.name);
    int64_t col0_id = col->getOid();
    col = g_catalog.getObjByName(Cond[0]->value);
    int64_t col1_id =col->getOid();
    auto &indexs = op[0]->getRowTableOut()->getIndexs();
    bool check = false;
    if (!indexs.empty()) {
        Index *this_index = (Index *)g_catalog.getObjById(indexs[0]);
        Key &this_Key = this_index->getIKey();
        auto &key = this_Key.getKey(); 
        if(!key.empty()){
            // table 0 has index
            if ( key.size() == 1 && (key[0] ==  col0_id || key[0] ==  col1_id)) {
                Operator* temp = op[1];
                op[1] = op[0];
                op[0] = temp;
                check = true;
            }
        }
    }
    if (!check) {
        auto &indexs= op[1]->getRowTableOut()->getIndexs();
        if (!indexs.empty()) {
            Index *this_index = (Index*) g_catalog.getObjById(indexs[0]);
            auto &key = this_index->getIKey().getKey();
            if (!key.empty())
                if (key.size() == 1 && (key[0] ==  col0_id|| key[0] ==  col1_id))
                    check = true;
        }
        if (!check) {
            // setup hash table for op 1
            is_use_self_ht = true;
            RowTable *t = op[1]->getRowTableOut();
            Object *col_1 = g_catalog.getObjByName(Cond[0]->column.name);
            Object *col_2 = g_catalog.getObjByName(Cond[0]->value);
            int64_t rank;
            if (t->getColumnRank(col_1->getOid()) != -1)
                rank = t->getColumnRank(col_1->getOid());
            else
                rank = t->getColumnRank(col_2->getOid());
            char *tab_out;
            int64_t offest = t->getRPattern().getColumnOffset(rank);
            ht = new HashTable(1000000, 2, 0);
            int i = 0;
            while (!op[1]->isEnd()){
                tab_out = (char *) malloc(round2(t->getRPattern().getRowSize()));
                op[1]->getNext(tab_out);
                i++;
                pos.push_back(tab_out);
                int64_t key = multhash(tab_out + offest, t->getRPattern().getColumnType(rank)->getTypeSize());
                ht->add(key, tab_out);
             }
            op[1]->init();
        }
    } 
   // check end
   for (int i = 0; i < Op_num; i++) {
        tab_in[i] = op[i]->getRowTableOut();
        // BECAUSE the tabel have one more column when set up  
        col_num += getColumnNumber(tab_in[i]);
    }
    char *name = remove_const("join_temp_tabel");
    RowTable *join_tabel = (RowTable *)g_catalog.getObjByName(name);
    if (join_tabel != NULL)
        join_tabel->shut();
    tab_out = new RowTable(1000, name);
    tab_out->init();
    RPattern &pattern = tab_out->getRPattern();
    pattern.init(col_num + 1);
    for (int i = 0; i < Op_num; i++) {
        auto &columns_in = tab_in[i]->getColumns();
        for (int j = 0; j < getColumnNumber(tab_in[i]); j++) {
            BasicType *column_type = tab_in[i]->getRPattern().getColumnType(j);
            pattern.addColumn(column_type);
            tab_out->addColumn(columns_in[j]);	
        }
    }
    pattern.addColumn(new TypeCharN(1));
    for (int i = 0; i < Con_num; i++) {
        method[i] = Cond[i]->compare;
        Object *col = g_catalog.getObjByName(Cond[i]->column.name);
        col_rank[i] = tab_out->getColumnRank(col->getOid());
        compare_val_type[i] = tab_out->getRPattern().getColumnType(col_rank[i]);
        memcpy(compare_val[i], Cond[i]->value, 128);
       }
    g_memory.alloc(current_ptr, round2(tab_out->getRPattern().getRowSize()));
}

bool HashJoin::init() { 
    current_ptr = NULL;
    first_time = true;
    out_num_left = 0;
    for (int i = 0; i < 4; i++)
        is_end[i] = false;
    for (int i = 0; i < op_num; i++)
        op[i]->init();
    return true; 
}

bool HashJoin::getNext(char * ptr) {
    int size[4] = { 0 };
    int current_col = 0;
    for (int i = 1; i < op_num; i++) {
        current_col += getColumnNumber(tab_in[i - 1]);
        size[i] = tab_out->getRPattern().getColumnOffset(current_col);
        // one tabel has nothing, the result of join must be NULL, thus return false
        if (first_time && op[i]->isEnd()) {
            op[i]->init();
            if (op[i]->isEnd())
                return false;
        }
    }
    bool total_check = false;
    while (!isEnd()) {
        total_check = false;
        if (first_time || out_num_left == 0){
            if (!op[0]->getNext(current_ptr+size[0]))
                return false;
            first_time = false;
            memcpy(ptr, current_ptr, tab_in[0]->getRPattern().getRowSize()); 
            for (int i = 0; i < con_num; i++){
                Object *col = g_catalog.getObjByName(compare_val[i]);
                auto cout_rank  = tab_out->getColumnRank(col->getOid());
                int64_t col1_off, col2_off, col1_type_size;
                auto &rp = tab_out->getRPattern();
                if (rp.getColumnOffset(cout_rank) < rp.getColumnOffset(col_rank[i])) {
                    col1_off = rp.getColumnOffset(cout_rank);
                    col2_off = rp.getColumnOffset(col_rank[i]);
                    col1_type_size = tab_in[0]->getRPattern().getColumnType(cout_rank)->getTypeSize();
                }
                else{
                    col1_off = rp.getColumnOffset(col_rank[i]);
                    col2_off = rp.getColumnOffset(cout_rank);
                    col1_type_size = tab_in[0]->getRPattern().getColumnType(col_rank[i])->getTypeSize();
                }
                auto value_col_1 = ptr + col1_off;
                if (!is_use_self_ht) {
                    auto &indexs2=  tab_in[1]->getIndexs();
                    Index * this_index = (Index *)g_catalog.getObjById(indexs2[0]);
                    this_hash_index = (HashIndex *)this_index; 
                    void *result;
                    void *ff[4] = { (void *)value_col_1, NULL, NULL, NULL };
                    this_hash_index->set_ls(ff, NULL, (void * )(&this_hash_info));
                    if (this_hash_info.rnum > 0) {
                        out_num_left = (int)this_hash_index->lookup(ff, (void *)(&this_hash_info), result);
                        if (out_num_left) {
                            memcpy(ptr+size[1], result, tab_in[1]->getRPattern().getRowSize());
                            auto value_col_2 =  ptr + col2_off;
                            total_check = compare_val_type[i]->cmpEQ(value_col_1, value_col_2);
                        }
                    }
                }
                else {
                    int64_t key = multhash(value_col_1, col1_type_size); 
                    int num = ht->probe(key, this_match, 3000);
                    if (num > 0) {
                        out_num_left = num - 1;
                        memcpy(ptr+size[1], this_match[out_num_left], tab_in[1]->getRPattern().getRowSize());
                        auto value_col_2 =  ptr + col2_off;
                        total_check = compare_val_type[i]->cmpEQ(value_col_1, value_col_2);
                    }
                    else
                        out_num_left=0;
                 }
             if (!total_check)
                break;
            }
        }
        else {
            out_num_left--;
            void *result;
            memcpy(ptr, current_ptr, tab_in[0]->getRPattern().getRowSize());
            Object *col = g_catalog.getObjByName(compare_val[0]);
            auto cout_rank  = tab_out->getColumnRank(col->getOid());
            int64_t col1_off, col2_off;
            auto &rp = tab_out->getRPattern();
            if (rp.getColumnOffset(cout_rank) < rp.getColumnOffset(col_rank[0])) {
                col1_off = rp.getColumnOffset(cout_rank);
                col2_off = rp.getColumnOffset(col_rank[0]);
            }
            else{
                col1_off = rp.getColumnOffset(col_rank[0]);
                col2_off = rp.getColumnOffset(cout_rank);
            }
            auto value_col_1 = ptr + col1_off;
            if (!is_use_self_ht) {
                void *ff[4] = { (void *)value_col_1, NULL, NULL, NULL };
                out_num_left = (int)this_hash_index->lookup(ff, (void *)(&this_hash_info), result);
                if (out_num_left == 1) {
                    memcpy(ptr+size[1], result, tab_in[1]->getRPattern().getRowSize());
                    auto value_col_2 =  ptr + col2_off;
                    total_check = compare_val_type[0]->cmpEQ(value_col_1, value_col_2);
                }
            }
            else {
                memcpy(ptr+size[1], this_match[out_num_left], tab_in[1]->getRPattern().getRowSize());
                auto value_col_2 =  ptr + col2_off;
                total_check = compare_val_type[0]->cmpEQ(value_col_1, value_col_2);
             }
        }
        if (total_check) {
            hash_count++;
            return true;
        }
    }
    return false;
}

bool HashJoin::close() {
    bool result = true;
    for (int i = 0; i < op_num; i++) {
        result = result && op[i]->close();
        delete op[i];
    }
    g_memory.free(current_ptr, tab_out->getRPattern().getRowSize());
    if (is_use_self_ht) {
        delete ht;
        for (size_t i = 0; i < pos.size(); i++)
            g_memory.free(pos[i], op[1]->getRowTableOut()->getRPattern().getRowSize());
    }

    return result;
}

Join::Join(int Op_num, Operator **Op, int Con_num,  Condition **Cond){
    int col_num = 0;
    for (int i = 0; i <Op_num; i++)
        op[i] = Op[i];
    op_num = Op_num;
    con_num = Con_num;
    for (int i = 0; i <Op_num; i++) {
        tab_in[i] = op[i]->getRowTableOut();
        // BECAUSE the tabel have one more column when set up  
        col_num += getColumnNumber(tab_in[i]);
    }
    char *name = remove_const("join_temp_tabel");
    RowTable *join_tabel = (RowTable *)g_catalog.getObjByName(name);
    if (join_tabel != NULL)
        join_tabel->shut();
    tab_out = new RowTable(100, name);
    tab_out->init();
    RPattern &pattern = tab_out->getRPattern();
    pattern.init(col_num + 1);
    for (int i = 0; i <Op_num; i++) {
        auto &columns_in = tab_in[i]->getColumns();
        for (int j = 0; j < getColumnNumber(tab_in[i]); j++) {
            BasicType *column_type = tab_in[i]->getRPattern().getColumnType(j);
            pattern.addColumn(column_type);
            tab_out->addColumn(columns_in[j]);	
        }
    }
    pattern.addColumn(new TypeCharN(1));
    for (int i = 0; i <Con_num; i++) {
        method[i] = Cond[i]->compare;
        Object *col = g_catalog.getObjByName(Cond[i]->column.name);
        col_rank[i] = tab_out->getColumnRank(col->getOid());
        compare_val_type[i] = tab_out->getRPattern().getColumnType(col_rank[i]);
        memcpy(compare_val[i], Cond[i]->value, 128);   
    }
    g_memory.alloc(current_ptr, round2(tab_out->getRPattern().getRowSize()));
}

bool Join::getNext(char * ptr){
    int size[4] = { 0 };
    int current_col = 0;
    for (int i = 1; i <op_num; i++){
        current_col += getColumnNumber(tab_in[i - 1]);
        size[i] = tab_out->getRPattern().getColumnOffset(current_col);
        // one tabel has nothing, the result of join must be NULL, thus return false
        if (first_time && op[i]->isEnd())
            return false;
    }
    while (!isEnd()) {
        for (int i = 0; i < op_num; i++) {
            if (i == 0) {
                is_end[i] = op[i]->isEnd();
                if (is_end[i])
                    op[i]->init();
                op[i]->getNext(current_ptr+size[i]);
            }
            else if (is_end[i - 1] || first_time) {
                is_end[i - 1] = false;
                is_end[i] = op[i]->isEnd();
                if (is_end[i])
                    op[i]->init();
                op[i]->getNext(current_ptr + size[i]);	
            }
        }
        first_time = false;
        memcpy(ptr, current_ptr, tab_out->getRPattern().getRowSize());
        bool total_check = true;
        for (int i = 0; i < con_num; i++){
            Object *col = g_catalog.getObjByName(compare_val[i]);
            auto cout_rank  = tab_out->getColumnRank(col->getOid());
            auto value_col_1 = ptr + tab_out->getRPattern().getColumnOffset(col_rank[i]);
            auto value_col_2 =  ptr + tab_out->getRPattern().getColumnOffset(cout_rank);
            bool check = compare_val_type[i]->cmpEQ(value_col_1, value_col_2);
            total_check = total_check && check;
            if (!total_check)
                break;
        }
        if (total_check)
            return true;
    }
    return false;
}

Project::Project(Operator *m_op, int m_col_num, RequestColumn *m_req) {
    this->op = m_op;
    this->col_num = m_col_num; // ASSERT(method != LINK)
    tab_in[0] = m_op->getRowTableOut();
    // calculate column rank
    for (int i = 0; i < m_col_num; i++) {
        Object *col = g_catalog.getObjByName(m_req[i].name);
        col_rank[i] = tab_in[0]->getColumnRank(col->getOid());
    }
    // construct table out
    tab_out = new RowTable(777, getTableName().c_str());
    tab_out->init();
    RPattern &pattern = tab_out->getRPattern();
    pattern.init(m_col_num + 1);
    auto &columns_in = tab_in[0]->getColumns();
    // insert types, note that if method == COUNT, we change type into int64
    for (int i = 0; i < m_col_num; i++) {
        BasicType *p;
        if (m_req[i].aggrerate_method != COUNT)
             p = tab_in[0]->getRPattern().getColumnType(col_rank[i]);
        else
             p = new TypeInt64;
        assert(pattern.addColumn(p));
        tab_out->addColumn(columns_in[col_rank[i]]);
    }
    pattern.addColumn(new TypeCharN(1));
}

bool Project::getNext(char * ptr) {
    // check whether it's the end
    if (this->isEnd())
        return false;
    char *temp = (char *)malloc(tab_in[0]->getRPattern().getRowSize());
    if (!op->getNext(temp))
        return false;
    auto rp_out = tab_out->getRPattern();
    auto rp_in = tab_in[0]->getRPattern();
    auto col_num = getColumnNumber(tab_out);
    // copy data
    for (int i = 0; i < col_num; i++) {
        char *dest = ptr + rp_out.getColumnOffset(i);
        char *src = temp + rp_in.getColumnOffset(col_rank[i]);
        memcpy(dest, src, rp_out.getColumnType(i)->getTypeSize());
    }
    return true;
}

OrderBy::OrderBy(Operator *m_op, int num, RequestColumn orderby[4]) {
    // initialize basic information
    this->op = m_op;
    tab_out = tab_in[0] = m_op->getRowTableOut();
    this->row_size = m_op->getRowTableOut()->getRPattern().getRowSize();
    this->cmp_num = num;
    for (int i = 0; i <num; i++) {
        auto col = g_catalog.getObjByName(orderby[i].name);
        int64_t crank = m_op->getRowTableOut()->getColumnRank(col->getOid());
        this->offset[i] = m_op->getRowTableOut()->getRPattern().getColumnOffset(crank);
        this->col_type[i] = m_op->getRowTableOut()->getRPattern().getColumnType(crank);
    }
    // initialize buffer to init data
    this->buf_size = round2(this->step * this->row_size);
    g_memory.alloc(this->buf, this->buf_size);
    char *temp = (char *)malloc(this->row_size);
    int64_t count = 0;
    while (!m_op->isEnd() && m_op->getNext(temp)) {
        // if there's not enough space, resize it
        if ((count + 1) * this->row_size > this->buf_size) {
            char *tt;
            g_memory.alloc(tt, 2 * this->buf_size);
            memcpy(tt, this->buf, this->buf_size);
            g_memory.free(this->buf, this->buf_size);
            this->buf_size *= 2;
            this->buf = tt;
        }
        // add new record
        memcpy(this->buf + count * this->row_size, temp, this->row_size);
        count++;
    }
    free(temp);
    // initialize index vector
    for (int64_t i = 0; i <count; i++)
        this->pos.push_back(this->buf + i * this->row_size);
    // use lambda to convert the member function into a function pointer
    auto cmp = [this](char *l, char *r) { return compare(l, r); };
    // use std::sort and compare to sort the index vector
    std::sort(this->pos.begin(), this->pos.end(), cmp);
}

inline int OrderBy::compare(char *p, char *q) {
    for (int i = 0; i <cmp_num; i++) {
        auto left = p + this->offset[i];
        auto right = q + this->offset[i];
        if (!this->col_type[i]->cmpEQ(left, right))
            return this->col_type[i]->cmpLE(left, right);
    }
    return 0;
}

GroupBy::GroupBy (Operator *m_op, int num, RequestColumn groupby[4]) {
    // initialize basic information
    this->op = m_op;
    tab_out = tab_in[0] = m_op->getRowTableOut();
    this->row_size = m_op->getRowTableOut()->getRPattern().getRowSize();
    this->groupby_num = num;
    int m = 0, n = 0;
    for (int i = 0; i <num; i++) {
        if (groupby[i].aggrerate_method==NONE_AM) {
            this->noaggr_offset[m] = m_op->getRowTableOut()->getRPattern().getColumnOffset(i);
            this->noaggr_type[m] = m_op->getRowTableOut()->getRPattern().getColumnType(i);
            this->noaggr_num++;
            m++;
        }
        else {
            this->aggr_offset[n] = m_op->getRowTableOut()->getRPattern().getColumnOffset(i);
            this->aggr_type[n] = m_op->getRowTableOut()->getRPattern().getColumnType(i);
            this->aggr_num++;
            this->am[n] = groupby[i].aggrerate_method;
            n++;
        }
    }
    // initialize buffer to init data
    this->buf_size = round2(this->step * this->row_size);
    g_memory.alloc(this->buf, this->buf_size);
    char *temp;
    g_memory.alloc(temp, round2(this->row_size));
    int64_t count = 0;
    HashTable *ht = new HashTable(1000000, 2, 0);
    srand(100);
    int64_t key = 0;
    char *tdata;
    g_memory.alloc(tdata, round2(this->row_size));
    char **p = (char **)malloc(100* this->row_size);
    int64_t size_offset;
    // count for every tuple
    int64_t *case_count = (int64_t *)malloc(20000 *sizeof(int64_t));
    for (int i = 0; i < 20000; i++)
        case_count[i] = 0;
    int probe_num = 0;
    int k = 0;
    // expand buf
    while (!m_op->isEnd() && m_op->getNext(temp)) {
        // if there's not enough space, resize it
        if ((count + 1) * this->row_size > this->buf_size) {
            char *tt;
            g_memory.alloc(tt, 2 * this->buf_size);
            memcpy(tt, this->buf, this->buf_size);
            g_memory.free(this->buf, this->buf_size);
            this->buf_size *= 2;
            this->buf = tt;
        }
        // generate key
        key = 0;
        size_offset = 0;
        for (int i = 0; i < this->noaggr_num; i++) {
            char *src = temp + this->noaggr_offset[i];
            memcpy(tdata+size_offset, src, this->noaggr_type[i]->getTypeSize());
            size_offset += this->noaggr_type[i]->getTypeSize();
        }
        key = multhash((char *)tdata,size_offset);
        // lookup
        probe_num = ht->probe(key, p, 1000);
        if (probe_num > 0) {
            for (k = 0; k < probe_num; k++) {
                char * probe_row = this->buf + (uint64_t)p[k] * this->row_size;
                if (this->check(temp, probe_row)) {
                    case_count[(uint64_t)p[k]]++;
                    for (int i = 0; i < this->aggr_num; i++) {
                        auto datain = temp + this->aggr_offset[i];
                        auto datap = this->buf + (uint64_t)p[k] * this->row_size + this->aggr_offset[i];
                        switch (this->am[i]) {
                            case COUNT:
                                break;
                            case SUM:
                                this->plus(datain,datap,datap,this->aggr_type[i]);
                                break;
                            case AVG:
                                this->plus(datain,datap,datap,this->aggr_type[i]);
                                break;
                            case MAX:
                                if(this->aggr_type[i]->cmpLT(datap,datain))
                                    this->aggr_type[i]->copy(datap,temp + this->aggr_offset[i]);
                                break;
                            case MIN:
                                if(this->aggr_type[i]->cmpGT(datap,datain))
                                    this->aggr_type[i]->copy(datap,temp + this->aggr_offset[i]);
                                break;
                            default:
                                break;
                        }
                    }
                    break;
                }
            }
            if (k >= probe_num) {
                ht->add(key, (char *)count);
                memcpy(this->buf + count * this->row_size, temp, this->row_size);
                case_count[count]++;
                count++;
                this->row_num++;
            }
        }
        else {
            ht->add(key, (char *)count);
            memcpy(this->buf + count * this->row_size, temp, this->row_size);
            case_count[count]++;
            count++;	
            this->row_num++;
        }
    }
    for (int ii = 0; ii <count; ii++) {
        for (int j = 0; j < this->aggr_num; j++) {
            auto datain = this->buf +ii*this->row_size + this->aggr_offset[j];
            switch (this->am[j]) {
                case AVG:
                    this->avg(datain,case_count[ii],datain,this->aggr_type[j]);
                    break;
                case COUNT:
                    this->aggr_type[j]->copy(datain,(int64_t *)&case_count[ii]);
                    break;
                default:
                    break;
            }
        }
    }		
    g_memory.free(temp, round2(this->row_size));
    free(p);
    g_memory.free(tdata, round2(this->row_size));
}

int64_t GroupBy::multhash(char *key, int64_t len) {
    unsigned char *new_key = (unsigned char *)key;
    int64_t hash = 0;
    for (int i = 0; i < len; i++)
        hash = 31 * hash + new_key[i];
    return hash;
}

bool GroupBy::plus(void *a1, void *a2, void *dest, BasicType *type) {
    switch (type->getTypeCode()) {
        case INT8_TC: {
            int8_t result = *(int8_t *)a1 + *(int8_t *)a2;
            memcpy(dest, (void*)&result, 1);
            break;
        }
        case INT16_TC: {
            int16_t result = *(int16_t *)a1 + *(int16_t *)a2;
            memcpy(dest, (void*)&result, 2);
            break;
        }
        case INT32_TC: {
            int32_t result = *(int32_t *)a1 + *(int32_t *)a2;
            memcpy(dest, (void*)&result, 4);
            break;
        }
        case INT64_TC: {
            int64_t result = *(int64_t *)a1 + *(int64_t *)a2;
            memcpy(dest, (void*)&result, 8);
            break;
        }
        case FLOAT32_TC: {
            float result = *(float *)a1 + *(float *)a2;
            memcpy(dest, (void*)&result, 4);
            break;
        }
        case FLOAT64_TC: {
            double result = *(double *)a1 + *(double *)a2;
            memcpy(dest, (void*)&result, 8);
            break;
        }
        default:
            return false;
    }
    return true;
}

bool GroupBy::avg(void *a1, int64_t count, void *dest, BasicType *type) {
    switch (type->getTypeCode()) {
        case INT8_TC: {
            int8_t result = *(int8_t *)a1 / count;
            memcpy(dest, (void*)&result, 1);
            break;
        }
        case INT16_TC: {
            int16_t result = *(int16_t *)a1 / count;
            memcpy(dest, (void*)&result, 2);
            break;
        }
        case INT32_TC: {
            int32_t result = *(int32_t *)a1 / count;
            memcpy(dest, (void*)&result, 4);
            break;
        }
        case INT64_TC: {
            int64_t result = *(int64_t *)a1 / count;
            memcpy(dest, (void*)&result, 8);
            break;
        }
        case FLOAT32_TC: {
            float result = *(float *)a1 / count;
            memcpy(dest, (void*)&result, 4);
            break;
        }
        case FLOAT64_TC: {
            double result = *(double *)a1 / count;
            memcpy(dest, (void*)&result, 8);
            break;
        }
        default:
            return false;
    }
    return true;
}

bool GroupBy::check(char *a1, char *a2) {
    for (int i = 0; i < this->noaggr_num; i++) {
        char *p1 = a1 + this->noaggr_offset[i];
        char *p2 = a2 + this->noaggr_offset[i];
        if (!this->noaggr_type[i]->cmpEQ(p1, p2)) {
            return false;
        }
    }
    return true;
}
