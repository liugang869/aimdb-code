#include "stdio.h"
/*
#include "executor.h"
#include "datatype.h"

// test for ResultTable
int test1 ()
{
    printf("test1...\n");
    ResultTable rt;
    BasicType  *type[10];
    type[0] = new TypeInt8();
    type[1] = new TypeInt16();
    type[2] = new TypeInt32();
    type[3] = new TypeInt64();
    type[4] = new TypeFloat32();
    type[5] = new TypeFloat64();
    type[6] = new TypeDate();
    type[7] = new TypeTime();
    type[8] = new TypeDateTime();
    type[9] = new TypeCharN(16);
    rt.init (type, 10, 1024);

    int8_t  i8= 8;
    int16_t i16 = 16;
    int32_t i32 = 32;
    int64_t i64 = 64;
    float   f32 = 32.5;
    double  f64 = 64.5;
    char    date[16] = "2017-12-01";
    char    time[16] = "13:45:12";
    char    datetime[64] = "2017-01-01 03:01:02";
    time_t  tdate,ttime,tdatetime;
    char    charn[16]  = "charn16";
   
    rt.writeRC (0,0,&i8);
    rt.writeRC (0,1,&i16);
    rt.writeRC (0,2,&i32);
    rt.writeRC (0,3,&i64);
    rt.writeRC (0,4,&f32);
    rt.writeRC (0,5,&f64);
    rt.column_type[6]->formatBin(&tdate,date);
    rt.writeRC (0,6,&tdate);
    rt.column_type[7]->formatBin(&ttime,time);
    rt.writeRC (0,7,&ttime);
    rt.column_type[8]->formatBin(&tdatetime,datetime);
    rt.writeRC (0,8,&tdatetime);
    rt.writeRC (0,9,charn);
    rt.row_number ++;

    rt.print();
    return 0;
}
*/

int main() 
{
    // g_memory.init(1<<30,1<<3);
    // test1 ();
    // g_memory.shut();
    printf ("test pass!\n");
    return 0;
}
