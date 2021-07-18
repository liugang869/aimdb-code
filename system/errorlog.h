/**
 * @file
 * @author  Shimin Chen <chensm@ict.ac.cn>
 * @version 0.1
 *
 * @section Description
 *
 * This file provides the error handling and logging utility.
 *
 * 1. error code 
 * The error code is a decimal number with 8 digits:
 *
 *     FFFLLLLL
 *
 * The higher 3 digits show the source file ID, while the lower 5 digits
 * indicate the line number in the source file, where the error occurs.
 *
 * The following macros are useful:
 *
 *      EL_GET_FILEID(err_code)
 *      EL_GET_LINENO(err_code)
 *      EL_GET_FILENAME(err_code)      
 *
 * 2. initialize
 *
 * (1) main() must call ErrorLog::init(int level, const char *logfile);
 *
 * (2) then every thread must new an ErrorLog instance:
 *
 *     thread_el= new ErrorLog("thread_name");
 *
 * (3) put source file names into EL_src_file_name[] in ErrorLog.cc
 *
 * 3. normal use
 *
 *   EL_LOG_DEBUG(format, args, ...);
 *   EL_LOG_INFO(format, args, ...);
 *   EL_LOG_WARN(format, args, ...);
 *   EL_LOG_ERROR(format, args, ...);
 *   EL_LOG_SERIOUS(format, args, ...);
 *
 *   The message will be written into the specified log file as follows
 *
 *   [thread][level][file:lineno] message
 *   a call stack trace will be shown for error and serious messages.
 *
 *   Moreover, an assertion can be written as:
 *
 *   EL_ASSERT(expression);
 *
 *   The expression is a test that evaluates to a True or False value.
 *   If the value is False, then a debug assertion message will be
 *   generated and written to the log.
 *
 * 4. In a worker thread:
 *
 *   (1) reset and clear the error messages
 *
 *    EL_RESET();
 *
 *   (2) then follow the above to log messages
 *
 *   (3) finally, obtain error code and message as follows
 *
 *   int err_code= EL_ERRCODE();
 *   const char *err_msg= EL_ERRMSG();
 *
 *   (4) optionally, flush the log file
 *
 *   ErrorLog::flushLog();
 *
 * 5. Before exiting, call the following globally once
 *
 *   ErrorLog::closeLog();
 *   
 */

#ifndef _ERRORLOG_H
#define _ERRORLOG_H

#include <pthread.h>
#include <stdio.h>
#include <string>
#include <unordered_map>
#include <time.h>

/* ---------------------------------------------------------------------- */
/* Error Logging Macros                                                   */
/* ---------------------------------------------------------------------- */

#define EL_DEBUG          1     /// debug message
#define EL_INFO           2     /// provide extra information
#define EL_WARN           3     /// warning but user operations can still run
#define EL_ERROR          4     /// errors but the system can still run
#define EL_SERIOUS        5     /// serious errors, the system needs to exit

// errors < this level will not be compiled into the executable
// this can be from EL_DEBUG to EL_ERROR.
// Note that we will always report any EL_ERROR and EL_SERIOUS messages.

#ifdef DEBUG
#define EL_LEVEL_COMPILE  EL_DEBUG
#else
#define EL_LEVEL_COMPILE  EL_INFO
#endif

#if EL_DEBUG >= EL_LEVEL_COMPILE
#define EL_LOG_DEBUG(...)  \
do{ if (EL_DEBUG>=ErrorLog::el_level) \
        thread_el->log(EL_DEBUG,__FILE__,__LINE__,__VA_ARGS__);}while(0)
#else
#define EL_LOG_DEBUG(...)
#endif

#if EL_INFO >= EL_LEVEL_COMPILE
#define EL_LOG_INFO(...)  \
do{ if (EL_INFO>=ErrorLog::el_level) \
        thread_el->log(EL_INFO,__FILE__,__LINE__,__VA_ARGS__);}while(0)
#else
#define EL_LOG_INFO(...)
#endif

#if EL_WARN >= EL_LEVEL_COMPILE
#define EL_LOG_WARN(...)  \
do{ if (EL_WARN>=ErrorLog::el_level) \
        thread_el->log(EL_WARN,__FILE__,__LINE__,__VA_ARGS__);}while(0)
#else
#define EL_LOG_WARN(...)
#endif

#define EL_LOG_ERROR(...)  \
        thread_el->log(EL_ERROR,__FILE__,__LINE__,__VA_ARGS__)

#define EL_LOG_SERIOUS(...)  \
        thread_el->log(EL_SERIOUS,__FILE__,__LINE__,__VA_ARGS__)

#define EL_RESET()     (thread_el->reset())

#define EL_ERRCODE()   (thread_el->getErrorCode())

#define EL_ERRMSG()    (thread_el->getErrorMsg())

#if EL_DEBUG >= EL_LEVEL_COMPILE
#define EL_ASSERT(exp)   \
do{ if (!(exp)) EL_LOG_DEBUG("Assertion failed: %s\n", #exp);} while(0)
#else
#define EL_ASSERT(t)
#endif

/* ---------------------------------------------------------------------- */
/* Error Code                                                             */
/* ---------------------------------------------------------------------- */

// Error code consists of 3 digits of file id and 5 digits of line number
// line number cannot 
#define EL_ERROR_CODE(fileid, lineno)    ((fileid)*100000 + (lineno))
#define EL_GET_FILEID(errcode)           ((errcode)/100000)
#define EL_GET_LINENO(errcode)           ((errcode)%100000)

#define EL_OK                            (0)
#define EL_BAD_FILEID                    (99999999)

#define EL_GET_FILENAME(errcode) \
        (ErrorLog::id2Name(EL_GET_FILEID(errcode)))

// source file names
extern const char *EL_src_file_name[];  /// an array of source file names

/* ---------------------------------------------------------------------- */
/* class ErrorLog                                                         */
/* ---------------------------------------------------------------------- */
class ErrorLog {

    // ---
    // class static
    // ---

  public:
    static int el_level;        ///< logging level
    static const char *el_level_name[EL_SERIOUS + 1];
    ///< level => name

  private:
    static pthread_mutex_t el_lock;     ///< protect the global states
    static char *el_logfile;    ///< file path to write log to

    static FILE *el_fp;         ///< file handle of el_logfile
    static std::unordered_map < std::string, int >*el_name_2_id;
    ///< file name => id
  public:
   /**
    * global initiator
    *
    * @param level     the dynamic logging level 
    * @param logfile   if not NULL then specify the log file path
    */
    static void init(int level, const char *logfile);

   /**
    * set the dynamice logging level (this must be at least EL_LEVEL_COMPILE)
    * This method is thread safe.
    *
    * @param level     the dynamic logging level 
    */
    static void setLevel(int level);

   /**
    * flush the log file
    */
    static void flushLog(void) {
        if (el_fp)
            fflush(el_fp);
    }
   /**
    * close the log file
    */ static void closeLog(void) {
        if (el_fp)
            fclose(el_fp);
        el_fp = NULL;
    }

   /**
    * get fileid for a given file name
    *
    * @param src_name   the file name
    *
    * @retval >=0 the file id
    * @retval <0  the file name does not exist
    */
    static int name2Id(const char *src_name);

   /**
    * get the file name for a given id
    *
    * @param src_id  the file id
    *
    * @retval !=NULL the file name
    * @retval ==NULL the file id is out of range
    */
    static const char *id2Name(int src_id);

    // ---
    // instance
    // ---

  private:
    char *el_thread_name;       ///< the thread name

    int el_err_code;            ///< current error code

    char *el_msg_buf;           ///< a buffer to hold the error message
    int el_msg_cap;             ///< the message buffer size
    char *el_msg_cur;           ///< point to the end of the message

    char *el_demangle_buf;      ///< buffer needed for demangle
    size_t el_demangle_len;     ///< demangle buffer length
    void *el_bt_buffer[256];    ///< allow up to 256 levels of calls


    time_t el_tloc;             ///< local time in seconds at last message
    struct tm el_tm;            ///< broken down time at last message

   /**
    * Parse the symbol returned from the backtrace_symbols() call.
    * Then demangle the function name if necessary.  Note this
    * implementation is GCC specific.
    *
    * A symbol has the following format:
    *
    *      binary(function+offset) [return address] 
    * 
    * @param bt_symbol   a symbol returned from backtrace_symbols()
    * 
    * @retval 0  success, output is in el_demangle_buf
    * @retval -1 function name is not available
    */
    int getFuncNameGCC(char *bt_symbol);

  public:
   /**
    * constructor
    *
    * @param threadid  the current thread id to generate the log for
    * @param level     the dynamic logging level 
    * @param logfile   if not NULL then specify the log file path
    */
    ErrorLog(const char *thread_name, int msg_cap = 256 * 1024);

   /**
    * destructor
    */
    ~ErrorLog();

   /**
    * Clear current error messages.  Call this before executing an operation.
    */
    void reset();

   /**
    * Log the message. Stack trace will be generated for error and serious 
    * messages.
    *
    * @param level     the level of the message
    * @param src_name  the source file name
    * @param lineno    the line number in the source file
    * @param ...       printf-like format string and arguments
    */
    void log(int level, const char *src_name, const int lineno, ...);

   /**
    * return the last error code
    */
    int getErrorCode(void) {
        return el_err_code;
    }

   /**
    * return the error message accumulated since last reset()
    */
    const char *getErrorMsg(void) {
        return el_msg_buf;
    }

};                              // ErrorLog

#ifndef _Thread_local
#define _Thread_local __thread
#endif

extern _Thread_local ErrorLog *thread_el;
#endif
