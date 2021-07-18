/**
 * @file
 * @author  Shimin Chen <chensm@ict.ac.cn>
 * @version 0.1
 *
 * @section Description
 *
 * This file provides the error handling and logging utility.
 *
 */

#include <stdlib.h>
#include <string.h>
#include <malloc.h>
#include <stdarg.h>
#include <execinfo.h>
#include <cxxabi.h>

#include "errorlog.h"

const char *EL_src_file_name[] = {
    /* 000 */ "ErrorLog.h",
    /* 001 */ "ErrorLog.cc",

    // add the source files here
    /* 002 */ "schema.h",
    /* 003 */ "schema.cc",
    /* 004 */ "rowtable.h",
    /* 005 */ "rowtable.cc",
    /* 006 */ "cursor.h",
    /* 007 */ "cursor.cc",
    /* 008 */ "hashindex.h",
    /* 009 */ "hashindex.cc",
    /* 010 */ "storeprocedure.h",
    /* 011 */ "storeprocedure.cc",
    /* 012 */ "tpcserver.h",
    /* 013 */ "tpcserver.cc",
    "debug_Error.cc",

    /* last */ NULL
};

#define EL_TOTAL_FILES  (sizeof(EL_src_file_name)/sizeof(char*))

ErrorLog _Thread_local *thread_el = NULL;

/* ---------------------------------------------------------------------- */
/* class ErrorLog static methods                                          */
/* ---------------------------------------------------------------------- */
int
 ErrorLog::el_level;
pthread_mutex_t ErrorLog::el_lock;
char *ErrorLog::el_logfile;
FILE *ErrorLog::el_fp;
std::unordered_map < std::string, int >*ErrorLog::el_name_2_id;
const char *ErrorLog::el_level_name[EL_SERIOUS + 1];

void
 ErrorLog::init(int level, const char *logfile)
{
    // 1. lock
    pthread_mutex_init(&el_lock, NULL);

    // 2. level
    setLevel(level);

    // 3. file
    if (logfile != NULL) {
        el_logfile = strdup(logfile);
        el_fp = fopen(el_logfile, "w");
        if (el_fp == NULL) {
            perror(el_logfile);
            exit(1);
        }
    } else {
        el_logfile = NULL;
        el_fp = NULL;
    }

    // 4. el_name_2_id
    el_name_2_id =
        new std::unordered_map < std::string, int >(EL_TOTAL_FILES * 2);
    for (int i = 0; EL_src_file_name[i] != NULL; i++) {
        (*el_name_2_id)[EL_src_file_name[i]] = i;
    }

    // 5. el_level_name
    el_level_name[0] = "nvalid";
    el_level_name[EL_DEBUG] = "DEBUG";
    el_level_name[EL_INFO] = "INFO";
    el_level_name[EL_WARN] = "WARN";
    el_level_name[EL_ERROR] = "ERROR";
    el_level_name[EL_SERIOUS] = "SERIOUS";

}                               // end of ErrorLog::init

void ErrorLog::setLevel(int level)
{
    // 1. level must be between EL_LEVEL_COMPILE and EL_ERROR
    //    all error and serious messages must be logged.
    if (level < EL_LEVEL_COMPILE)
        level = EL_LEVEL_COMPILE;
    if (level > EL_ERROR)
        level = EL_ERROR;

    // 2. set the level, make this thread safe
    pthread_mutex_lock(&el_lock);
    el_level = level;
    pthread_mutex_unlock(&el_lock);

}                               // end of ErrorLog::setLevel

int ErrorLog::name2Id(const char *src_name)
{
    std::unordered_map < std::string, int >::const_iterator got
        = el_name_2_id->find(src_name);
    if (got == el_name_2_id->end())
        return -1;
    else
        return got->second;

}                               // end of ErrorLog::name2Id

const char *ErrorLog::id2Name(int src_id)
{
    return ((src_id < 0 || src_id >= (int) EL_TOTAL_FILES)
            ? NULL : EL_src_file_name[src_id]);
}


/* ---------------------------------------------------------------------- */
/* class ErrorLog instance methods                                        */
/* ---------------------------------------------------------------------- */
ErrorLog::ErrorLog(const char *thread_name, int msg_cap)
{
    el_thread_name = strdup(thread_name);
    el_err_code = EL_OK;
    el_msg_buf = (char *) malloc(msg_cap);
    if ((el_thread_name == NULL) || (el_msg_buf == NULL)) {
        perror("malloc");
        exit(1);
    }
    el_msg_cap = msg_cap;
    el_msg_cur = el_msg_buf;
    *el_msg_cur = 0;

    el_demangle_len = 1024;
    el_demangle_buf = (char *) malloc(el_demangle_len);
    if (el_demangle_buf == NULL) {
        perror("malloc");
        exit(1);
    }

    time(&el_tloc);
    localtime_r(&el_tloc, &el_tm);

}                               // end of ErrorLog constructor

ErrorLog::~ErrorLog()
{
    free(el_demangle_buf);
    free(el_msg_buf);
    free(el_thread_name);
}

void
 ErrorLog::reset()
{
    el_err_code = EL_OK;
    el_msg_cur = el_msg_buf;
    *el_msg_cur = 0;

}                               // end of ErrorLog::reset

int ErrorLog::getFuncNameGCC(char *bt_symbol)
{
    char fn[1024];

    // 1. look for the left (
    char *start = strchr(bt_symbol, '(');
    if (start == NULL)
        return -1;
    start++;

    // 2. look for +
    char *end = strchr(start, '+');
    if (end == NULL)
        return -1;

    // 3. check the length 
    int ll = end - start;
    if ((ll <= 0) || (ll >= (int) sizeof(fn)))
        return -1;              // this is basically not possible

    // 4. demangle
    if ((ll > 2) && (start[0] == '_') && (start[1] == 'Z')) {   // start with _Z
        memcpy(fn, start, ll);
        fn[ll] = 0;
        int status = -100;
        char *ret =
            abi::__cxa_demangle(fn, el_demangle_buf, &el_demangle_len,
                                &status);
        if (ret && (ret != el_demangle_buf))
            free(ret);
        if (status == 0)
            return 0;
    }
    // 5. copy the original
    memcpy(el_demangle_buf, start, ll);
    el_demangle_buf[ll] = 0;
    return 0;

}                               // end of ErrorLog::getFuncNameGCC

void ErrorLog::log(int level, const char *src_name, const int lineno, ...)
{
    va_list ap;
    char *cur, *format;
    int len, ll, num_funcs, i, check, org_cap;
    char **fninfo;

    time(&el_tloc);
    localtime_r(&el_tloc, &el_tm);
    org_cap = el_msg_cap;

  again:

    cur = el_msg_cur;
    len = el_msg_cap - (cur - el_msg_buf);      // remaining size

    // 0. time
    ll = strftime(cur, len, "%F %T ", &el_tm);
    if (ll == 0)
        goto double_the_buffer; // not large enough
    cur += ll;
    len -= ll;

    // 1. [thread][level][file:line]
    if (snprintf(cur, len, "[%s][%s][%s:%d] ",
                 el_thread_name, el_level_name[level], src_name,
                 lineno) >= len) {
        goto double_the_buffer;
    }
    ll = strlen(cur);
    cur += ll;
    len -= ll;

    // 2. the rest of the message
    va_start(ap, lineno);
    format = va_arg(ap, char *);
    if (vsnprintf(cur, len, format, ap) >= len) {
        va_end(ap);
        goto double_the_buffer;
    }
    va_end(ap);
    ll = strlen(cur);
    cur += ll;
    len -= ll;

    // 3. generate stack trace if necessary
    if (level >= EL_ERROR) {
        num_funcs = backtrace(el_bt_buffer, 256);
        fninfo = backtrace_symbols(el_bt_buffer, num_funcs);

        el_demangle_buf[0] = 0;
        check = 0;
        for (i = 1; i < num_funcs; i++) {
            if (getFuncNameGCC(fninfo[i]) == 0) {
                check = (snprintf(cur, len, "  %s, %s\n",
                                  fninfo[i], el_demangle_buf) >= len);
            } else {
                check = (snprintf(cur, len, "  %s\n", fninfo[i]) >= len);
            }
            if (check)
                break;
            ll = strlen(cur);
            cur += ll;
            len -= ll;
            if (strcmp(el_demangle_buf, "main") == 0)
                break;
        }
        free(fninfo);

        if (check)
            goto double_the_buffer;
    }
    // 4. write to log
    if (el_fp)
        fputs(el_msg_cur, el_fp);

    // 5. update el_msg_cur
    if (level >= EL_WARN) {
        el_msg_cur = cur;

        // 6. set the error code
        i = name2Id(src_name);
        el_err_code =
            ((i >= 0) ? EL_ERROR_CODE(i, lineno) : EL_BAD_FILEID);
    } else {
        *el_msg_cur = 0;
    }

    // 7. if cap has changed, record this info
    if (el_msg_cap > org_cap) {
        fprintf(el_fp,
                "[%s][INFO][%s:%d] Message Buffer Capacity is %d bytes\n",
                el_thread_name, __FILE__, __LINE__, el_msg_cap);
    }

    return;


  double_the_buffer:

    el_msg_cap = el_msg_cap * 2;
    len = el_msg_cur - el_msg_buf;
    el_msg_buf = (char *) realloc(el_msg_buf, el_msg_cap);
    if (el_msg_buf == NULL) {
        perror("ErrorLog::log");
        exit(1);
    }
    el_msg_cur = el_msg_buf + len;

    goto again;
}
