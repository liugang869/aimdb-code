/**
 * @file    debug_errorlog.cc
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *
 * debug errorlog
 *
 */

#include "errorlog.h"
#include <pthread.h>

class testClass {

  public:
    void printErr();
    void checkLog();
    void checkMultiMsg();

};                              // end of testClass

void
 testClass::printErr()
{
    printf("----------\n");
    printf("Errcode:%d\n", EL_ERRCODE());
    printf("Errmsg:%s\n", EL_ERRMSG());
}

void testClass::checkLog()
{
    printf
        ("============================================================\n");
    printf("log level: %s\n", ErrorLog::el_level_name[ErrorLog::el_level]);

    EL_RESET();
    EL_LOG_DEBUG("This is debug: %d\n", 1);
    printErr();

    EL_RESET();
    EL_LOG_INFO("This is info: %d\n", 2);
    printErr();

    EL_RESET();
    EL_LOG_WARN("This is warn: %d\n", 3);
    printErr();

    EL_RESET();
    EL_LOG_ERROR("This is error: %d\n", 4);
    printErr();

    EL_RESET();
    EL_LOG_SERIOUS("This is serious: %d\n", 5);
    printErr();

    printf("\n\n");
}

void testClass::checkMultiMsg()
{
    printf
        ("============================================================\n");
    printf("log level: %s\n", ErrorLog::el_level_name[ErrorLog::el_level]);

    EL_RESET();
    EL_LOG_DEBUG("This is debug 2: %d\n", 10);
    EL_LOG_INFO("This is info 2: %d\n", 20);
    EL_LOG_WARN("This is warn 2: %d\n", 30);
    EL_LOG_ERROR("This is error 2: %d\n", 40);
    EL_LOG_SERIOUS("This is serious 2: %d\n", 50);

    printErr();
}

int test()
{
    // 1. initialize the global log state
    ErrorLog::init(EL_DEBUG, "tmp.log");

    // 2. initialize thread local log instance
    thread_el = new ErrorLog("Main", 1);

    // 3. try error code macro
    printf("try error code macro\n");
    int fileid = 123;
    int lineno = 4567;
    int err_code = EL_ERROR_CODE(fileid, lineno);
    printf("fileid= %d, lineno= %d, errorcode= %d\n",
           fileid, lineno, err_code);
    printf("EL_GET_FILEID= %d, EL_GET_LINENO= %d\n",
           EL_GET_FILEID(err_code), EL_GET_LINENO(err_code));
    printf("EL_GET_FILENAME(100000) = %s\n", EL_GET_FILENAME(100000));
    printf("\n");

    // 4. change log level then call checkLog
    testClass mytest;
    for (int level = EL_DEBUG; level <= EL_SERIOUS; level++) {

        ErrorLog::setLevel(level);

        EL_RESET();
        EL_LOG_SERIOUS("global log level is %s\n",
                       ErrorLog::el_level_name[ErrorLog::el_level]);

        mytest.checkLog();
    }

    // 5. accumulate multiple messages
    ErrorLog::setLevel(EL_DEBUG);
    mytest.checkMultiMsg();

    // 6. check assertion
    EL_ASSERT(thread_el == NULL);

    // n. close log globally
    ErrorLog::closeLog();

    return 0;
}

void *Handle(void *d)
{
    thread_el = new ErrorLog("Main", 1);
    EL_RESET();
    EL_LOG_DEBUG("some error here... %d\n", *(int *) d);
    thread_el->flushLog();
    return NULL;
}

int main()
{

    printf("------errorlog test(you can check output of tmp.log)------\n");
    // test();
    ErrorLog::init(EL_DEBUG, "tmp.log");
    test();
    printf("test pass!(checked by author)\n");
    // pthread_t p1, p2;
    // pthread_create(&p1,NULL,Handle,(void*)&p1);
    // pthread_create(&p2,NULL,Handle,(void*)&p2);
    return 0;
}
