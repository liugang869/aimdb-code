/**
 * @file    datatype.h
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *  
 * all datatype supported by this system
 *
 */

#ifndef _DATATYPE_H
#define _DATATYPE_H

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

/** data type code. */
enum TypeCode {
    INVID_TC = 0,
    INT8_TC,       /**< int8  */
    INT16_TC,      /**< int16 */
    INT32_TC,      /**< int32 */
    INT64_TC,      /**< int64 */
    FLOAT32_TC,    /**< float32 */
    FLOAT64_TC,    /**< float64 */
    CHARN_TC,      /**< charn */  
    DATE_TC,       /**< days from 1970-01-01 till current DATE */
    TIME_TC,       /**< seconds from 00:00:00 till current TIME */
    DATETIME_TC,   /**< seconds from 1970-01-01 00:00:00 till current DATETIME */
    MAXTYPE_TC
};

/** definition of class BasicType. */
class BasicType {
  protected:
    TypeCode b_type_code;  /**< data type code */
    int64_t  b_type_size;  /**< data type size */
  public:
    /**
     * constructor.
     */
    BasicType(TypeCode typecode, int64_t typesize) {
        b_type_code = typecode;
        b_type_size = typesize;
    }
    /**
     * destructor.
     */
    virtual ~BasicType () {} 
    /**
     * copy from data to dest.
     */ 
    virtual int copy(void *dest, void *data) {
        printf("[BasicType][ERROR][copy]: not support!\n");
        return -1;
    }
    /**
     * less than.
     */
    virtual bool cmpLT(void *data1, void *data2) {
        printf("[BasicType][ERROR][cmpLT]: not support!\n");
        return false;
    }
    /**
     * equal to.
     */
    virtual bool cmpEQ(void *data1, void *data2) {
        printf("[BasicType][ERROR][cmpEQ]: not support!\n");
        return false;
    }
    /**
     * less than or equal to.
     */
    virtual bool cmpLE(void *data1, void *data2) {
        printf("[BasicType][ERROR][cmpLT]: not support!\n");
        return false;
    }
    /**
     * greater than.
     */
    virtual bool cmpGT(void *data1, void *data2) {
        printf("[BasicType][ERROR][cmpGT: not support!\n");
        return false;
    }
    /**
     * greater than or equal to
     */
    virtual bool cmpGE(void *data1, void *data2) {
        printf("[BasicType][ERROR][cmpGE]: not support!\n");
        return false;
    }
    /**
     * extract txt format from data(bin) to dest.
     */
    virtual int formatTxt(void *dest, void *data) {
        printf("[BasicType][ERROR][formatTxt]: not support!\n");
        return -2;
    }
    /**
     * extract bin format from data(txt) to dest.
     */
    virtual int formatBin(void *dest, void *data) {
        printf("[BasicType][ERROR][formatBin]: not support!\n");
        return -3;
    }
    /**
     * get data size when stored in bin format.
     */
    virtual int64_t getTypeSize(void) {
        return b_type_size;
    }
    /**
     * get type code of this data type.
     */
    virtual TypeCode getTypeCode(void) {
        return b_type_code;
    }
};

/** definition of class TypeInt8,please refer to BasicType,it's same. */
class TypeInt8:public BasicType {
  public:
    /**
     * constructor.
     */
    TypeInt8(TypeCode typecode = INT8_TC, int64_t typesize = sizeof(int8_t)):BasicType(typecode,
              typesize)
    {
    }
    /**
     * copy from data to dest.
     */ 
    int copy(void *dest, void *data) {
        *(int8_t *) dest = *(int8_t *) data;
        return b_type_size;
    }
    int formatTxt(void *dest, void *data) {
        return sprintf((char *) dest, "%d", (int) (*(int8_t *) data));
    }
    int formatBin(void *dest, void *data) {
        int tmp;
        sscanf((char *) data, "%d", &tmp);
        if (tmp < 128 && tmp >= -128) {
            *(int8_t *) dest = tmp;
            return b_type_size;
        } else {
            printf("[TypeInt8][ERROR][formatBin]: data exceed range!\n");
            return -1;
        }
    }
    bool cmpLT(void *data1, void *data2) {
        return *(int8_t *) data1 < *(int8_t *) data2;
    }
    bool cmpLE(void *data1, void *data2) {
        return *(int8_t *) data1 <= *(int8_t *) data2;
    }
    bool cmpEQ(void *data1, void *data2) {
        return *(int8_t *) data1 == *(int8_t *) data2;
    }
    bool cmpGT(void *data1, void *data2) {
        return *(int8_t *) data1 > *(int8_t *) data2;
    }
    bool cmpGE(void *data1, void *data2) {
        return *(int8_t *) data1 >= *(int8_t *) data2;
    }
};

/** definition of class TypeInt16,please refer to BasicType,it's same. */
class TypeInt16:public BasicType {
  public:
    /**
     * constructor.
     */
  TypeInt16(TypeCode typecode = INT16_TC, int64_t typesize = sizeof(int16_t)):BasicType(typecode,
              typesize)
    {
    }
    /**
     * copy from data to dest.
     */ 
    int copy(void *dest, void *data) {
        *(int16_t *) dest = *(int16_t *) data;
        return b_type_size;
    }
    int formatTxt(void *dest, void *data) {
        return sprintf((char *) dest, "%d", (int) (*(int16_t *) data));
    }
    int formatBin(void *dest, void *data) {
        int tmp;
        sscanf((char *) data, "%d", &tmp);
        if (tmp < (1 << 16) && tmp >= -(1 << 16)) {
            *(int16_t *) dest = tmp;
            return b_type_size;
        } else {
            printf("[TypeInt16][ERROR][formatBin]: data exceed range!\n");
            return -1;
        }
    }
    bool cmpLT(void *data1, void *data2) {
        return *(int16_t *) data1 < *(int16_t *) data2;
    }
    bool cmpLE(void *data1, void *data2) {
        return *(int16_t *) data1 <= *(int16_t *) data2;
    }
    bool cmpEQ(void *data1, void *data2) {
        return *(int16_t *) data1 == *(int16_t *) data2;
    }
    bool cmpGT(void *data1, void *data2) {
        return *(int16_t *) data1 > *(int16_t *) data2;
    }
    bool cmpGE(void *data1, void *data2) {
        return *(int16_t *) data1 >= *(int16_t *) data2;
    }
};

/** definition of class TypeInt32,please refer to BasicType,it's same. */
class TypeInt32:public BasicType {
  public:
    /**
     * constructor.
     */
  TypeInt32(TypeCode typecode = INT32_TC, int64_t typesize = sizeof(int32_t)):BasicType(typecode,
              typesize)
    {
    }
    /**
     * copy from data to dest.
     */ 
    int copy(void *dest, void *data) {
        *(int32_t *) dest = *(int32_t *) data;
        return b_type_size;
    }
    int formatTxt(void *dest, void *data) {
        return sprintf((char *) dest, "%d", (int) (*(int32_t *) data));
    }
    int formatBin(void *dest, void *data) {
        int tmp;
        sscanf((char *) data, "%d", &tmp);
        *(int32_t *) dest = tmp;
        return b_type_size;
    }
    bool cmpLT(void *data1, void *data2) {
        return *(int32_t *) data1 < *(int32_t *) data2;
    }
    bool cmpLE(void *data1, void *data2) {
        return *(int32_t *) data1 <= *(int32_t *) data2;
    }
    bool cmpEQ(void *data1, void *data2) {
        return *(int32_t *) data1 == *(int32_t *) data2;
    }
    bool cmpGT(void *data1, void *data2) {
        return *(int32_t *) data1 > *(int32_t *) data2;
    }
    bool cmpGE(void *data1, void *data2) {
        return *(int32_t *) data1 >= *(int32_t *) data2;
    }
};

/** definition of class TypeInt64,please refer to BasicType,it's same. */
class TypeInt64:public BasicType {
  public:
    /**
     * constructor.
     */
  TypeInt64(TypeCode typecode = INT64_TC, int64_t typesize = sizeof(int64_t)):BasicType(typecode,
              typesize)
    {
    }
    /**
     * copy from data to dest.
     */ 
    int copy(void *dest, void *data) {
        *(int64_t *) dest = *(int64_t *) data;
        return b_type_size;
    }
    int formatTxt(void *dest, void *data) {
        return sprintf((char *) dest, "%ld",
                       (int64_t) (*(int64_t *) data));
    }
    int formatBin(void *dest, void *data) {
        int64_t tmp;
        sscanf((char *) data, "%ld", &tmp);
        *(int64_t *) dest = tmp;
        return b_type_size;
    }
    bool cmpLT(void *data1, void *data2) {
        return *(int64_t *) data1 < *(int64_t *) data2;
    }
    bool cmpLE(void *data1, void *data2) {
        return *(int64_t *) data1 <= *(int64_t *) data2;
    }
    bool cmpEQ(void *data1, void *data2) {
        return *(int64_t *) data1 == *(int64_t *) data2;
    }
    bool cmpGT(void *data1, void *data2) {
        return *(int64_t *) data1 > *(int64_t *) data2;
    }
    bool cmpGE(void *data1, void *data2) {
        return *(int64_t *) data1 >= *(int64_t *) data2;
    }
};

/** definition of class TypeFloat32,please refer to BasicType,it's same. */
class TypeFloat32:public BasicType {
  public:
    /**
     * constructor.
     */
  TypeFloat32(TypeCode typecode = FLOAT32_TC, int64_t typesize = sizeof(float)):BasicType(typecode,
              typesize)
    {
    } 
    /**
     * copy from data to dest.
     */ 
    int copy(void *dest, void *data) {
        *(float *) dest = *(float *) data;
        return b_type_size;
    }
    int formatTxt(void *dest, void *data) {
        return sprintf((char *) dest, "%f", (float) (*(float *) data));
    }
    int formatBin(void *dest, void *data) {
        float tmp;
        sscanf((char *) data, "%f", &tmp);
        *(float *) dest = tmp;
        return b_type_size;
    }
    bool cmpLT(void *data1, void *data2) {
        return *(float *) data1 < *(float *) data2;
    }
    bool cmpLE(void *data1, void *data2) {
        return *(float *) data1 <= *(float *) data2;
    }
    bool cmpEQ(void *data1, void *data2) {
        return *(float *) data1 == *(float *) data2;
    }
    bool cmpGT(void *data1, void *data2) {
        return *(float *) data1 > *(float *) data2;
    }
    bool cmpGE(void *data1, void *data2) {
        return *(float *) data1 >= *(float *) data2;
    }
};

/** definition of class TypeFloat64,please refer to BasicType,it's same. */
class TypeFloat64:public BasicType {
  public:
    /**
     * constructor.
     */
  TypeFloat64(TypeCode typecode = FLOAT64_TC, int64_t typesize = sizeof(double)):BasicType(typecode,
              typesize)
    {
    }
    /**
     * copy from data to dest.
     */ 
    int copy(void *dest, void *data) {
        *(double *) dest = *(double *) data;
        return b_type_size;
    }
    int formatTxt(void *dest, void *data) {
        return sprintf((char *) dest, "%lf", (double) (*(double *) data));
    }
    int formatBin(void *dest, void *data) {
        double tmp;
        sscanf((char *) data, "%lf", &tmp);
        *(double *) dest = tmp;
        return b_type_size;
    }
    bool cmpLT(void *data1, void *data2) {
        return *(double *) data1 < *(double *) data2;
    }
    bool cmpLE(void *data1, void *data2) {
        return *(double *) data1 <= *(double *) data2;
    }
    bool cmpEQ(void *data1, void *data2) {
        return *(double *) data1 == *(double *) data2;
    }
    bool cmpGT(void *data1, void *data2) {
        return *(double *) data1 > *(double *) data2;
    }
    bool cmpGE(void *data1, void *data2) {
        return *(double *) data1 >= *(double *) data2;
    }
};

/** definition of class TypeCharN,please refer to BasicType,it's same. */
class TypeCharN:public BasicType {
  public:
    /**
     * constructor.
     */
    TypeCharN(int64_t typesize):BasicType(CHARN_TC, typesize) {
    } 
    TypeCharN(TypeCode typecode = CHARN_TC, int64_t typesize = 32):BasicType(typecode,
              typesize)
    {
    }
    /**
     * copy from data to dest.
     */ 
    int copy(void *dest, void *data) {
        strncpy((char *) dest, (char *) data, b_type_size);
        return b_type_size;
    }
    int formatTxt(void *dest, void *data) {
        strncpy((char *) dest, (char *) data, b_type_size);
        return b_type_size;
    }
    int formatBin(void *dest, void *data) {
        strncpy((char *) dest, (char *) data, b_type_size);
        return b_type_size;
    }
    bool cmpLT(void *data1, void *data2) {
        return strncmp((char *) data1, (char *) data2,
                       b_type_size) < 0 ? true : false;
    }
    bool cmpLE(void *data1, void *data2) {
        return strncmp((char *) data1, (char *) data2,
                       b_type_size) <= 0 ? true : false;
    }
    bool cmpEQ(void *data1, void *data2) {
        return strncmp((char *) data1, (char *) data2,
                       b_type_size) == 0 ? true : false;
    }
    bool cmpGT(void *data1, void *data2) {
        return strncmp((char *) data1, (char *) data2,
                       b_type_size) > 0 ? true : false;
    }
    bool cmpGE(void *data1, void *data2) {
        return strncmp((char *) data1, (char *) data2,
                       b_type_size) >= 0 ? true : false;
    }
};

/** definition of class TypeDate,please refer to BasicType,it's same. */
class TypeDate:public BasicType {
  public:
    /**
     * constructor.
     */
  TypeDate(TypeCode typecode = DATE_TC, int64_t typesize = sizeof(time_t)):BasicType(typecode,
              typesize)
    {
    }
    /**
     * copy from data to dest.
     */ 
    int copy(void *dest, void *data) {
        *(time_t *) dest = *(time_t *) data;
        return b_type_size;
    }
    int formatTxt(void *dest, void *data) {
        struct tm *tt = localtime((time_t *) data);
        return strftime((char *) dest, 32, "%Y-%m-%d", tt);
    }
    int formatBin(void *dest, void *data) {
        struct tm tt = { 0 };
        strptime((char *) data, "%Y-%m-%d", &tt);
        time_t et = mktime(&tt);
        *(time_t *) dest = et;
        return b_type_size;
    }
    bool cmpLT(void *data1, void *data2) {
        return *(time_t *) data1 < *(time_t *) data2;
    }
    bool cmpLE(void *data1, void *data2) {
        return *(time_t *) data1 <= *(time_t *) data2;
    }
    bool cmpEQ(void *data1, void *data2) {
        return *(time_t *) data1 == *(time_t *) data2;
    }
    bool cmpGT(void *data1, void *data2) {
        return *(time_t *) data1 > *(time_t *) data2;
    }
    bool cmpGE(void *data1, void *data2) {
        return *(time_t *) data1 >= *(time_t *) data2;
    }
};

/** definition of class TypeTime,please refer to BasicType,it's same. */
class TypeTime:public BasicType {
  public:
    /**
     * constructor.
     */
    TypeTime(TypeCode typecode = TIME_TC, int64_t typesize = sizeof(time_t)):BasicType(typecode,
              typesize)
    {
    }
    /**
     * copy from data to dest.
     */ 
    int copy(void *dest, void *data) {
        *(time_t *) dest = *(time_t *) data;
        return b_type_size;
    }
    int formatTxt(void *dest, void *data) {
        struct tm *tt = localtime((time_t *) data);
        return strftime((char *) dest, 32, "%H:%M:%S", tt);
    }
    int formatBin(void *dest, void *data) {
        struct tm tt = { 0 };
        strptime((char *) data, "%H:%M:%S", &tt);
        time_t et = mktime(&tt);
        *(time_t *) dest = et;
        return b_type_size;
    }
    bool cmpLT(void *data1, void *data2) {
        return *(time_t *) data1 < *(time_t *) data2;
    }
    bool cmpLE(void *data1, void *data2) {
        return *(time_t *) data1 <= *(time_t *) data2;
    }
    bool cmpEQ(void *data1, void *data2) {
        return *(time_t *) data1 == *(time_t *) data2;
    }
    bool cmpGT(void *data1, void *data2) {
        return *(time_t *) data1 > *(time_t *) data2;
    }
    bool cmpGE(void *data1, void *data2) {
        return *(time_t *) data1 >= *(time_t *) data2;
    }
};

/** definition of class TypeDateTime,please refer to BasicType,it's same. */
class TypeDateTime:public BasicType {
  public:
    /**
     * constructor.
     */
    TypeDateTime(TypeCode typecode = DATETIME_TC, int64_t typesize = sizeof(time_t)):BasicType(typecode,
              typesize)
    {
    }
    /**
     * copy from data to dest.
     */ 
    int copy(void *dest, void *data) {
        *(time_t *) dest = *(time_t *) data;
        return b_type_size;
    }
    int formatTxt(void *dest, void *data) {
        struct tm *tt = localtime((time_t *) data);
        return strftime((char *) dest, 32, "%Y-%m-%d %H:%M:%S", tt);
    }
    int formatBin(void *dest, void *data) {
        struct tm tt = { 0 };
        strptime((char *) data, "%Y-%m-%d %H:%M:%S", &tt);
        time_t et = mktime(&tt);
        *(time_t *) dest = et;
        return b_type_size;
    }
    bool cmpLT(void *data1, void *data2) {
        return *(time_t *) data1 < *(time_t *) data2;
    }
    bool cmpLE(void *data1, void *data2) {
        return *(time_t *) data1 <= *(time_t *) data2;
    }
    bool cmpEQ(void *data1, void *data2) {
        return *(time_t *) data1 == *(time_t *) data2;
    }
    bool cmpGT(void *data1, void *data2) {
        return *(time_t *) data1 > *(time_t *) data2;
    }
    bool cmpGE(void *data1, void *data2) {
        return *(time_t *) data1 >= *(time_t *) data2;
    }
};
#endif
